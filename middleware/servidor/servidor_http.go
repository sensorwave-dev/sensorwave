package servidor

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

type Cliente struct {
	ID      string
	Canal   chan Mensaje
	cerrado bool
	mu      sync.Mutex

	pendientes   map[string]struct{}
	pendientesMu sync.Mutex
}

var (
	clientesPorTopico = make(map[string]map[string]*Cliente)
	clientesPorID     = make(map[string]*Cliente)
	mutexHTTP         sync.Mutex
)

const LOG_HTTP string = "HTTP"

const (
	ackTimeout         = 2 * time.Second
	factorAleatorioAck = 1.5
	maxRetransmisiones = 4
)

// IniciarHTTP inicia un servidor HTTP en el puerto especificado.
// Retorna un canal que se cierra cuando el servidor está listo para aceptar conexiones.
func IniciarHTTP(puerto string) <-chan struct{} {
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

	return listo
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

	// Configurar cabeceras SSE antes de escribir el status
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Deshabilitar buffering de proxies como nginx

	// Escribir status 200 explícitamente para iniciar la respuesta
	w.WriteHeader(http.StatusOK)

	clienteID := fmt.Sprintf("%d", time.Now().UnixNano())
	cliente := &Cliente{
		ID:         clienteID,
		Canal:      make(chan Mensaje, 10000),
		cerrado:    false,
		pendientes: make(map[string]struct{}),
	}

	mutexHTTP.Lock()
	if clientesPorTopico[normalizado] == nil {
		clientesPorTopico[normalizado] = make(map[string]*Cliente)
	}
	clientesPorTopico[normalizado][clienteID] = cliente
	clientesPorID[clienteID] = cliente
	mutexHTTP.Unlock()

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

	cliente.pendientesMu.Lock()
	cliente.pendientes = make(map[string]struct{})
	cliente.pendientesMu.Unlock()

	loggerPrint(LOG_HTTP, "Cliente desconectado - ID: %s, Tópico: %s", clienteID, normalizado)
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
	if mensajeTopico != topicoQuery {
		http.Error(w, "El tópico del query y del cuerpo no coinciden", http.StatusBadRequest)
		return
	}
	mensaje.Topico = mensajeTopico

	loggerPrint(LOG_HTTP, "Mensaje recibido - Tópico: %s, QoS: %d, MensajeID: %s", mensaje.Topico, mensaje.QoS, mensaje.MensajeID)

	// enviar a los protocolos
	if mensaje.Original {
		mensaje.Original = false
		go enviarHTTP(LOG_HTTP, mensaje)
		go enviarCoAP(LOG_HTTP, mensaje)
		go enviarMQTT(LOG_HTTP, mensaje)
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

		clienteEncontrado.pendientesMu.Lock()
		clienteEncontrado.pendientes = make(map[string]struct{})
		clienteEncontrado.pendientesMu.Unlock()

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

	cliente.marcarAck(ack.MensajeID)
	loggerPrint(LOG_HTTP, "ACK recibido - ClienteID: %s, MensajeID: %s", ack.ClienteID, ack.MensajeID)
	w.WriteHeader(http.StatusOK)
}

func enviarHTTPQoS1(LOG string, c *Cliente, msg Mensaje) {
	if msg.MensajeID == "" {
		loggerPrint(LOG, "Error - QoS 1 sin MensajeID, no se envía")
		return
	}
	if !c.registrarPendiente(msg.MensajeID) {
		return
	}

	go func() {
		delay := ackTimeout
		for intento := 0; intento <= maxRetransmisiones; intento++ {
			if !c.pendienteExiste(msg.MensajeID) {
				return
			}
			select {
			case c.Canal <- msg:
				// Éxito silencioso - el ACK confirmará la recepción
			default:
				loggerPrint(LOG, "Error - No se pudo enviar mensaje QoS 1 - MensajeID: %s, Intento: %d, Razón: canal bloqueado", msg.MensajeID, intento)
			}
			if intento == maxRetransmisiones {
				c.marcarAck(msg.MensajeID)
				return
			}
			time.Sleep(delay)
			delay = time.Duration(float64(delay) * factorAleatorioAck)
		}
	}()
}

func (c *Cliente) registrarPendiente(mensajeID string) bool {
	c.pendientesMu.Lock()
	defer c.pendientesMu.Unlock()
	if _, ok := c.pendientes[mensajeID]; ok {
		return false
	}
	c.pendientes[mensajeID] = struct{}{}
	return true
}

func (c *Cliente) pendienteExiste(mensajeID string) bool {
	c.pendientesMu.Lock()
	defer c.pendientesMu.Unlock()
	_, ok := c.pendientes[mensajeID]
	return ok
}

func (c *Cliente) marcarAck(mensajeID string) {
	c.pendientesMu.Lock()
	delete(c.pendientes, mensajeID)
	c.pendientesMu.Unlock()
}
