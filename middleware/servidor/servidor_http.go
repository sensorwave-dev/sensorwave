package servidor

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/sensorwave-dev/sensorwave/middleware/internal/qos"
)

type Cliente struct {
	ID      string
	Canal   chan Mensaje
	cerrado bool
	mu      sync.Mutex
}

// InflightTracker rastrea mensajes QoS1 pendientes de ACK por suscriptor HTTP
// (egreso servidor -> suscriptor), brindando deduplicación y visibilidad del
// inflight. Clave: (MensajeID, SuscriptorID).
//
// El redelivery lo orquesta enviarHTTPQoS1 con backoff RFC 7252 (constantes en
// middleware/internal/qos). La liberación de entradas ocurre por ACK explícito
// del suscriptor (POST /sensorwave/ack) o al agotar los reintentos; y al
// desconectarse/desuscribirse un cliente vía EliminarSuscriptor.
type InflightTracker struct {
	mu   sync.Mutex
	pend map[string]map[string]struct{}
}

func NewInflightTracker() *InflightTracker {
	return &InflightTracker{pend: make(map[string]map[string]struct{})}
}

// Registrar marca (MensajeID, SuscriptorID) como pendiente. Retorna true si
// era nuevo (hay que iniciar el redelivery) o false si ya estaba pendiente
// (dedup: no retransmitir de nuevo desde cero).
func (t *InflightTracker) Registrar(mensajeID, suscriptorID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.pend[mensajeID]; !ok {
		t.pend[mensajeID] = make(map[string]struct{})
	}
	if _, exists := t.pend[mensajeID][suscriptorID]; exists {
		return false
	}
	t.pend[mensajeID][suscriptorID] = struct{}{}
	return true
}

// Existe indica si (MensajeID, SuscriptorID) sigue pendiente de ACK.
func (t *InflightTracker) Existe(mensajeID, suscriptorID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if subs, ok := t.pend[mensajeID]; ok {
		_, ok := subs[suscriptorID]
		return ok
	}
	return false
}

// Ack confirma recepción y elimina el inflight para (MensajeID, SuscriptorID).
func (t *InflightTracker) Ack(mensajeID, suscriptorID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if subs, ok := t.pend[mensajeID]; ok {
		delete(subs, suscriptorID)
		if len(subs) == 0 {
			delete(t.pend, mensajeID)
		}
	}
}

// EliminarSuscriptor borra todos los inflight asociados a un suscriptor
// (p.ej. al desconectarse un cliente HTTP). Retorna la cantidad eliminadas.
func (t *InflightTracker) EliminarSuscriptor(suscriptorID string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for mensajeID, subs := range t.pend {
		if _, ok := subs[suscriptorID]; ok {
			delete(subs, suscriptorID)
			n++
			if len(subs) == 0 {
				delete(t.pend, mensajeID)
			}
		}
	}
	return n
}

var (
	clientesPorTopico = make(map[string]map[string]*Cliente)
	clientesPorID     = make(map[string]*Cliente)
	mutexHTTP         sync.Mutex

	// inflightHTTP rastrea los mensajes QoS1 pendientes de ACK por suscriptor
	// HTTP, con backoff RFC 7252 (constantes compartidas en middleware/internal/qos).
	inflightHTTP = NewInflightTracker()
)

const LOG_HTTP string = "HTTP"

// IniciarHTTP inicia un servidor HTTP en el puerto especificado.
// La función retorna cuando el servidor está listo para aceptar conexiones.
func IniciarHTTP(puerto string) {
	listo := make(chan struct{})

	go func() {
		// Crear un ServeMux individual para esta instancia (evita conflictos de registro)
		mux := http.NewServeMux()

		// Endpoint para manejar conexiones
		mux.HandleFunc("/sensorwave", manejadorHTTP)
		mux.HandleFunc("/sensorwave/ack", manejarAckHTTP)

		// Crear listener primero para saber cuándo está listo
		listener, err := net.Listen("tcp", ":"+puerto)
		if err != nil {
			loggerFatal(LOG_HTTP, "Error al iniciar listener: %v", err)
		}

		// Configurar servidor HTTP con timeouts apropiados para SSE
		// ReadTimeout: 0 (sin límite) para permitir conexiones largas
		// WriteTimeout: 0 (sin límite) para permitir streaming SSE indefinido
		server := &http.Server{
			Handler:      mux,
			ReadTimeout:  0, // Sin timeout de lectura
			WriteTimeout: 0, // Sin timeout de escritura (crítico para SSE)
			IdleTimeout:  0, // Sin timeout de idle
		}

		loggerPrint(LOG_HTTP, "Servidor iniciado - Puerto: %s", puerto)
		close(listo) // Señalizar que está listo para aceptar conexiones

		server.Serve(listener)
	}()

	<-listo // Esperar a que la goroutine avise que está listo
}

// manejador es el punto de entrada para todas las solicitudes HTTP
func manejadorHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		manejarSuscripcionHTTP(w, r)
	}
	if r.Method == http.MethodPost {
		manejarPublicacionHTTP(w, r)
	}
	if r.Method == http.MethodDelete {
		manejarDesuscripcionHTTP(w, r)
	}
}

func manejarSuscripcionHTTP(w http.ResponseWriter, r *http.Request) {
	topico := r.URL.Query().Get("topico")
	if topico == "" {
		http.Error(w, "Falta el parámetro 'topico'", http.StatusBadRequest)
		return
	}

	normalizado, err := normalizarYValidarTopico(topico, true)
	if err != nil {
		http.Error(w, "Topico invalido", http.StatusBadRequest)
		return
	}
	if EsTopicoControl(normalizado) {
		http.Error(w, "Tópico de control no permitido por HTTP", http.StatusForbidden)
		return
	}

	// Configurar cabeceras SSE antes de escribir el status
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Deshabilitar buffering de proxies como nginx

	// Escribir status 200 explícitamente para iniciar la respuesta
	w.WriteHeader(http.StatusOK)

	clienteID := fmt.Sprintf("%d", time.Now().UnixNano())
	cliente := &Cliente{
		ID:      clienteID,
		Canal:   make(chan Mensaje, 10000),
		cerrado: false,
	}

	mutexHTTP.Lock()
	if clientesPorTopico[normalizado] == nil {
		clientesPorTopico[normalizado] = make(map[string]*Cliente)
	}
	clientesPorTopico[normalizado][clienteID] = cliente
	clientesPorID[clienteID] = cliente
	mutexHTTP.Unlock()

	defer func() {
		mutexHTTP.Lock()
		if clientes, exists := clientesPorTopico[normalizado]; exists {
			delete(clientes, clienteID)
			if len(clientes) == 0 {
				delete(clientesPorTopico, normalizado)
			}
		}
		delete(clientesPorID, clienteID)
		mutexHTTP.Unlock()

		cliente.mu.Lock()
		if !cliente.cerrado {
			close(cliente.Canal)
			cliente.cerrado = true
		}
		cliente.mu.Unlock()

		inflightHTTP.EliminarSuscriptor(clienteID)

		loggerPrint(LOG_HTTP, "Cliente desconectado - ID: %s, Tópico: %s", clienteID, normalizado)
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "El servidor no soporta streaming", http.StatusInternalServerError)
		return
	}

	// Enviar mensaje inicial con clienteID y hacer flush inmediatamente
	_, err = fmt.Fprintf(w, "data: {\"clienteID\":\"%s\"}\n\n", clienteID)
	if err != nil {
		loggerPrint(LOG_HTTP, "Error al enviar clienteID - ID: %s, Error: %v", clienteID, err)
		return
	}
	flusher.Flush()

	// Pequeña pausa para asegurar que el paquete TCP se envíe antes de que el cliente lea
	time.Sleep(100 * time.Millisecond)

	loggerPrint(LOG_HTTP, "ClienteID enviado y flush realizado - ID: %s", clienteID)

	loggerPrint(LOG_HTTP, "Cliente conectado - ID: %s, Tópico: %s", clienteID, normalizado)

	// Crear ticker de keepalive para mantener la conexión viva (cada 5 segundos)
	// Intervalo muy corto para evitar que routers/firewalls cierren la conexión TCP por inactividad
	// Especialmente importante para clientes WiFi (ESP32) que atraviesan routers
	keepaliveTicker := time.NewTicker(5 * time.Second)
	defer keepaliveTicker.Stop()

	// Loop principal: manejar mensajes del channel y keepalives
	for {
		select {
		case msg, ok := <-cliente.Canal:
			if !ok {
				// Canal cerrado, salir del loop
				loggerPrint(LOG_HTTP, "Canal cerrado para cliente - ID: %s", clienteID)
				return
			}
			jsonBytes, err := json.Marshal(msg)
			if err != nil {
				loggerPrint(LOG_HTTP, "Error - No se pudo serializar mensaje: %v", err)
				continue
			}
			_, err = fmt.Fprintf(w, "data: %s\n\n", string(jsonBytes))
			if err != nil {
				loggerPrint(LOG_HTTP, "Error al escribir mensaje al cliente - ID: %s, Error: %v", clienteID, err)
				return
			}
			flusher.Flush()

		case <-keepaliveTicker.C:
			// Enviar comentario SSE (válido y no dispara callbacks) para mantener viva la conexión
			_, err := fmt.Fprintf(w, ":keepalive\n\n")
			if err != nil {
				loggerPrint(LOG_HTTP, "Error al enviar keepalive - ID: %s, Error: %v", clienteID, err)
				return
			}
			flusher.Flush()
		}
	}
}

// Manejar publicaciones de mensajes
func manejarPublicacionHTTP(w http.ResponseWriter, r *http.Request) {
	topicoQuery := r.URL.Query().Get("topico")
	if topicoQuery == "" {
		http.Error(w, "Falta el parámetro 'topico'", http.StatusBadRequest)
		return
	}

	topicoQuery, err := normalizarYValidarTopico(topicoQuery, false)
	if err != nil {
		http.Error(w, "Topico invalido", http.StatusBadRequest)
		return
	}
	if EsTopicoControl(topicoQuery) {
		http.Error(w, "Tópico de control no permitido por HTTP", http.StatusForbidden)
		return
	}

	// Leer el cuerpo de la solicitud
	var mensaje Mensaje
	err = json.NewDecoder(r.Body).Decode(&mensaje)
	if err != nil {
		http.Error(w, "Error al procesar el cuerpo de la solicitud: "+err.Error(), http.StatusBadRequest)
		return
	}

	if mensaje.Topico == "" {
		http.Error(w, "Falta el parámetro 'topico'", http.StatusBadRequest)
		return
	}

	mensajeTopico, err := normalizarYValidarTopico(mensaje.Topico, false)
	if err != nil {
		http.Error(w, "Topico invalido", http.StatusBadRequest)
		return
	}
	if err := validarQoS(mensaje); err != nil {
		http.Error(w, "QoS invalido", http.StatusBadRequest)
		return
	}
	if err := validarTamanoPayload(mensaje); err != nil {
		http.Error(w, "Payload demasiado grande", http.StatusRequestEntityTooLarge)
		return
	}
	if mensajeTopico != topicoQuery {
		http.Error(w, "El tópico del query y del cuerpo no coinciden", http.StatusBadRequest)
		return
	}
	mensaje.Topico = mensajeTopico
	asignarOrigenSiVacio(&mensaje)

	loggerPrint(LOG_HTTP, "Mensaje recibido - Tópico: %s, QoS: %d, MensajeID: %s", mensaje.Topico, mensaje.QoS, mensaje.MensajeID)

	// Si el mensaje fue originado por esta instancia y regresó del upstream, no distribuir localmente
	if esMensajeRebotado(mensaje) {
		loggerPrint(LOG_HTTP, "Mensaje ignorado - Regresó del upstream, ya fue distribuido localmente - Tópico: %s", mensaje.Topico)
		return
	}

	// enviar a los protocolos
	if mensaje.Original {
		mensaje.Original = false
		go enviarHTTP(LOG_HTTP, mensaje)
		go enviarCoAP(LOG_HTTP, mensaje)
		go enviarMQTT(LOG_HTTP, mensaje)
		go reenviarUpstream(mensaje)
	}
	// Responder al cliente que envió el POST
	if mensaje.QoS == 1 {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"ack":       "ok",
			"mensajeId": mensaje.MensajeID,
		})
		return
	}
	w.WriteHeader(http.StatusOK)
}

func manejarDesuscripcionHTTP(w http.ResponseWriter, r *http.Request) {
	topico := r.URL.Query().Get("topico")
	clienteID := r.URL.Query().Get("clienteID")

	if topico == "" || clienteID == "" {
		http.Error(w, "Faltan parámetros 'topico' o 'clienteID'", http.StatusBadRequest)
		return
	}

	normalizado, err := normalizarYValidarTopico(topico, true)
	if err != nil {
		http.Error(w, "Topico invalido", http.StatusBadRequest)
		return
	}

	mutexHTTP.Lock()
	var clienteEncontrado *Cliente
	if clientes, exists := clientesPorTopico[normalizado]; exists {
		if cliente, existe := clientes[clienteID]; existe {
			clienteEncontrado = cliente
			delete(clientes, clienteID)
			if len(clientes) == 0 {
				delete(clientesPorTopico, normalizado)
			}
		}
	}
	mutexHTTP.Unlock()

	if clienteEncontrado != nil {
		clienteEncontrado.mu.Lock()
		if !clienteEncontrado.cerrado {
			close(clienteEncontrado.Canal)
			clienteEncontrado.cerrado = true
		}
		clienteEncontrado.mu.Unlock()

		inflightHTTP.EliminarSuscriptor(clienteID)

		loggerPrint(LOG_HTTP, "Cliente desuscrito - ID: %s, Tópico: %s", clienteID, normalizado)
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, "Cliente no encontrado", http.StatusNotFound)
	}
}

type solicitudAck struct {
	ClienteID string `json:"clienteId"`
	MensajeID string `json:"mensajeId"`
}

func manejarAckHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var ack solicitudAck
	if err := json.NewDecoder(r.Body).Decode(&ack); err != nil {
		http.Error(w, "Error al procesar el cuerpo de la solicitud", http.StatusBadRequest)
		return
	}
	if ack.ClienteID == "" || ack.MensajeID == "" {
		http.Error(w, "Faltan parámetros 'clienteId' o 'mensajeId'", http.StatusBadRequest)
		return
	}

	mutexHTTP.Lock()
	cliente := clientesPorID[ack.ClienteID]
	mutexHTTP.Unlock()
	if cliente == nil {
		http.Error(w, "Cliente no encontrado", http.StatusNotFound)
		return
	}

	inflightHTTP.Ack(ack.MensajeID, ack.ClienteID)
	loggerPrint(LOG_HTTP, "ACK recibido - ClienteID: %s, MensajeID: %s", ack.ClienteID, ack.MensajeID)
	w.WriteHeader(http.StatusOK)
}

func enviarHTTPQoS1(LOG string, c *Cliente, msg Mensaje) {
	if msg.MensajeID == "" {
		loggerPrint(LOG, "Error - QoS 1 sin MensajeID, no se envía")
		return
	}
	// Registrar en el tracker compartido. Si ya estaba pendiente (dedup), no
	// iniciar un nuevo ciclo de redelivery.
	if !inflightHTTP.Registrar(msg.MensajeID, c.ID) {
		return
	}

	go func() {
		// Backoff RFC 7252: ACK_TIMEOUT aleatorizado + ×2 por reintento.
		delay := qos.JitterAckTimeout()
		for intento := 0; intento <= qos.MaxRetransmisiones; intento++ {
			if !inflightHTTP.Existe(msg.MensajeID, c.ID) {
				return
			}
			select {
			case c.Canal <- msg:
				// Éxito silencioso - el ACK confirmará la recepción
			default:
				loggerPrint(LOG, "Error - No se pudo enviar mensaje QoS 1 - MensajeID: %s, Intento: %d, Razón: canal bloqueado", msg.MensajeID, intento)
			}
			if intento == qos.MaxRetransmisiones {
				inflightHTTP.Ack(msg.MensajeID, c.ID) // agotado: liberar
				return
			}
			time.Sleep(delay)
			delay *= qos.FactorBackoff
		}
	}()
}
