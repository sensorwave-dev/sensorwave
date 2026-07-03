package borde

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

// ============================================================================
// FEDERACION MQTT (BORDE)
// ============================================================================

// federacionMQTT gestiona la conexión MQTT del borde para recibir consultas
// y comandos de la nube vía el plano de control swctl/#.
type federacionMQTT struct {
	cliente    mqtt.Client
	gestor     *GestorBorde
	finalizado chan struct{}
	wg         sync.WaitGroup

	// Idempotencia de comandos en memoria (MVP con TTL)
	comandosMu      sync.RWMutex
	comandosProcesados map[string]time.Time // claveIdempotencia -> timestamp
}

// iniciarFederacionMQTT crea e inicia el worker de federación MQTT del borde.
func (me *GestorBorde) iniciarFederacionMQTT(broker string) (*federacionMQTT, error) {
	opciones := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID("sw-borde-" + me.nodoID + "-" + uuid.New().String()).
		SetAutoReconnect(true).
		SetResumeSubs(true).
		SetConnectRetry(true).
		SetCleanSession(false)

	cliente := mqtt.NewClient(opciones)
	if token := cliente.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("error conectando al broker MQTT: %w", token.Error())
	}

	f := &federacionMQTT{
		cliente:            cliente,
		gestor:             me,
		finalizado:         make(chan struct{}),
		comandosProcesados: make(map[string]time.Time),
	}

	// Suscribirse a todas las consultas y comandos dirigidos a este nodo
	topicoSuscribir := fmt.Sprintf("swctl/nodos/%s/#", me.nodoID)
	token := cliente.Subscribe(topicoSuscribir, 1, f.manejarMensaje)
	if token.Wait() && token.Error() != nil {
		cliente.Disconnect(250)
		return nil, fmt.Errorf("error suscribiéndose a %s: %w", topicoSuscribir, token.Error())
	}

	// Iniciar goroutine de limpieza de idempotencia
	f.wg.Add(1)
	go f.limpiarIdempotencia()

	log.Printf("Federación MQTT activa para nodo %s en %s", me.nodoID, broker)
	return f, nil
}

// cerrar detiene la federación MQTT de forma ordenada.
func (f *federacionMQTT) cerrar() {
	close(f.finalizado)
	f.wg.Wait()
	if f.cliente != nil && f.cliente.IsConnected() {
		f.cliente.Disconnect(250)
	}
	log.Println("Federación MQTT cerrada")
}

// manejarMensaje despacha mensajes del plano de control según el tópico.
func (f *federacionMQTT) manejarMensaje(cliente mqtt.Client, msg mqtt.Message) {
	partes := splitTopic(msg.Topic())
	if len(partes) < 4 {
		return
	}

	// Formato esperado: swctl/nodos/{nodoID}/{tipo}/...
	if partes[0] != "swctl" || partes[1] != "nodos" || partes[2] != f.gestor.nodoID {
		return
	}

	tipo := partes[3]
	switch tipo {
	case "consulta":
		if len(partes) == 6 && partes[4] == "solicitud" {
			f.manejarConsulta(msg.Topic(), msg.Payload(), partes[5])
		}
	case "comando":
		if len(partes) == 6 && partes[4] == "solicitud" {
			f.manejarComando(msg.Topic(), msg.Payload(), partes[5])
		}
	default:
		// Ignorar otros tipos (latido, capacidades, etc.)
	}
}

// ============================================================================
// CONSULTAS
// ============================================================================

func (f *federacionMQTT) manejarConsulta(topico string, payload []byte, idConsulta string) {
	var solicitud tipos.SolicitudControlConsulta
	if err := json.Unmarshal(payload, &solicitud); err != nil {
		f.publicarErrorConsulta(idConsulta, "parse_error", err.Error())
		return
	}

	// Ejecutar la consulta local y publicar respuesta
	resultado, err := f.ejecutarConsulta(solicitud)
	if err != nil {
		f.publicarErrorConsulta(idConsulta, "consulta_error", err.Error())
		return
	}

	// Publicar resultado como una única parte
	parteJSON, err := json.Marshal(resultado)
	if err != nil {
		f.publicarErrorConsulta(idConsulta, "serializacion_error", err.Error())
		return
	}

	parte := tipos.RespuestaControlConsultaParte{
		Version:       1,
		IDConsulta:    idConsulta,
		IDNodo:        f.gestor.nodoID,
		IndiceParte:   0,
		EsUltimaParte: true,
		Resultado:     parteJSON,
	}
	parteBytes, _ := json.Marshal(parte)

	topicoParte := tipos.ConstruirTopicoConsultaParte(idConsulta, 0)
	f.cliente.Publish(topicoParte, 1, false, parteBytes)

	// Publicar fin
	fin := tipos.RespuestaControlConsultaFin{
		Version:    1,
		IDConsulta: idConsulta,
		IDNodo:     f.gestor.nodoID,
		Estado:     "finalizado",
		Partes:     1,
		Parcial:    false,
	}
	finBytes, _ := json.Marshal(fin)
	topicoFin := tipos.ConstruirTopicoConsultaFin(idConsulta)
	f.cliente.Publish(topicoFin, 1, false, finBytes)
}

func (f *federacionMQTT) ejecutarConsulta(solicitud tipos.SolicitudControlConsulta) (interface{}, error) {
	args := solicitud.Argumentos
	switch solicitud.TipoConsulta {
	case tipos.ConsultaRango:
		tiempoInicio := time.Unix(0, args.TiempoInicio)
		tiempoFin := time.Unix(0, args.TiempoFin)
		return f.gestor.ConsultarRango(args.Serie, tiempoInicio, tiempoFin)
	case tipos.ConsultaUltimo:
		var tInicio, tFin *time.Time
		if args.TiempoInicioPtr != nil {
			t := time.Unix(0, *args.TiempoInicioPtr)
			tInicio = &t
		}
		if args.TiempoFinPtr != nil {
			t := time.Unix(0, *args.TiempoFinPtr)
			tFin = &t
		}
		return f.gestor.ConsultarUltimoPunto(args.Serie, tInicio, tFin)
	case tipos.ConsultaAgregacion:
		tiempoInicio := time.Unix(0, args.TiempoInicio)
		tiempoFin := time.Unix(0, args.TiempoFin)
		return f.gestor.ConsultarAgregacion(args.Serie, tiempoInicio, tiempoFin, args.Agregaciones)
	case tipos.ConsultaAgregacionTemporal:
		tiempoInicio := time.Unix(0, args.TiempoInicio)
		tiempoFin := time.Unix(0, args.TiempoFin)
		intervalo := time.Duration(args.Intervalo)
		return f.gestor.ConsultarAgregacionTemporal(args.Serie, tiempoInicio, tiempoFin, args.Agregaciones, intervalo)
	default:
		return nil, fmt.Errorf("tipo de consulta no soportado: %s", solicitud.TipoConsulta)
	}
}

func (f *federacionMQTT) publicarErrorConsulta(idConsulta, codigo, mensaje string) {
	errResp := tipos.RespuestaControlConsultaError{
		Version:    1,
		IDConsulta: idConsulta,
		IDNodo:     f.gestor.nodoID,
		Codigo:     codigo,
		Mensaje:    mensaje,
	}
	payload, _ := json.Marshal(errResp)
	topico := tipos.ConstruirTopicoConsultaError(idConsulta)
	f.cliente.Publish(topico, 1, false, payload)
}

// ============================================================================
// COMANDOS
// ============================================================================

func (f *federacionMQTT) manejarComando(topico string, payload []byte, idComando string) {
	var solicitud tipos.SolicitudControlComando
	if err := json.Unmarshal(payload, &solicitud); err != nil {
		f.publicarErrorComando(idComando, "parse_error", err.Error())
		return
	}

	// Idempotencia: si ya procesamos esta clave, retornar éxito directamente
	if solicitud.ClaveIdempotencia != "" {
		if f.yaProcesado(solicitud.ClaveIdempotencia) {
			f.publicarFinComando(idComando, "finalizado_idempotente", nil)
			return
		}
		f.marcarProcesado(solicitud.ClaveIdempotencia)
	}

	resultado, err := f.ejecutarComando(solicitud)
	if err != nil {
		f.publicarErrorComando(idComando, "comando_error", err.Error())
		return
	}
	f.publicarFinComando(idComando, "finalizado", resultado)
}

func (f *federacionMQTT) ejecutarComando(solicitud tipos.SolicitudControlComando) (interface{}, error) {
	args := solicitud.Argumentos
	switch solicitud.Operacion {
	case tipos.OpSerieCrear:
		if args.Serie == nil {
			return nil, fmt.Errorf("argumento serie requerido")
		}
		return nil, f.gestor.CrearSerie(*args.Serie)
	case tipos.OpReglaCrear:
		if args.Regla == nil {
			return nil, fmt.Errorf("argumento regla requerido")
		}
		regla, err := parsearReglaDesdeMapa(args.Regla)
		if err != nil {
			return nil, err
		}
		return nil, f.gestor.AgregarRegla(regla)
	case tipos.OpReglaActualizar:
		if args.Regla == nil {
			return nil, fmt.Errorf("argumento regla requerido")
		}
		regla, err := parsearReglaDesdeMapa(args.Regla)
		if err != nil {
			return nil, err
		}
		return nil, f.gestor.ActualizarRegla(regla)
	case tipos.OpReglaEliminar:
		return nil, f.gestor.EliminarRegla(args.ReglaID)
	case tipos.OpDatoInsertar:
		return nil, f.gestor.Insertar(args.Path, args.Timestamp, args.Valor)
	default:
		return nil, fmt.Errorf("operación no soportada: %s", solicitud.Operacion)
	}
}

func (f *federacionMQTT) publicarFinComando(idComando, estado string, resultado interface{}) {
	resp := tipos.RespuestaControlComandoFin{
		Version:   1,
		IDComando: idComando,
		IDNodo:    f.gestor.nodoID,
		Estado:    estado,
		Resultado: resultado,
	}
	payload, _ := json.Marshal(resp)
	topico := fmt.Sprintf(tipos.TopicoComandoFin, idComando)
	f.cliente.Publish(topico, 1, false, payload)
}

func (f *federacionMQTT) publicarErrorComando(idComando, codigo, mensaje string) {
	resp := tipos.RespuestaControlComandoError{
		Version:   1,
		IDComando: idComando,
		IDNodo:    f.gestor.nodoID,
		Codigo:    codigo,
		Mensaje:   mensaje,
	}
	payload, _ := json.Marshal(resp)
	topico := fmt.Sprintf(tipos.TopicoComandoError, idComando)
	f.cliente.Publish(topico, 1, false, payload)
}

// ============================================================================
// IDEMPOTENCIA EN MEMORIA (MVP, TTL 24h)
// ============================================================================

func (f *federacionMQTT) yaProcesado(clave string) bool {
	f.comandosMu.RLock()
	defer f.comandosMu.RUnlock()
	_, ok := f.comandosProcesados[clave]
	return ok
}

func (f *federacionMQTT) marcarProcesado(clave string) {
	f.comandosMu.Lock()
	defer f.comandosMu.Unlock()
	f.comandosProcesados[clave] = time.Now()
}

// limpiarIdempotencia elimina entradas de idempotencia con más de 24 horas.
func (f *federacionMQTT) limpiarIdempotencia() {
	defer f.wg.Done()
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-f.finalizado:
			return
		case <-ticker.C:
			f.comandosMu.Lock()
			limite := time.Now().Add(-24 * time.Hour)
			for clave, ts := range f.comandosProcesados {
				if ts.Before(limite) {
					delete(f.comandosProcesados, clave)
				}
			}
			f.comandosMu.Unlock()
		}
	}
}

// ============================================================================
// HELPERS
// ============================================================================

func splitTopic(t string) []string {
	for len(t) > 0 && t[0] == '/' {
		t = t[1:]
	}
	for len(t) > 0 && t[len(t)-1] == '/' {
		t = t[:len(t)-1]
	}
	return strings.Split(t, "/")
}

// parsearReglaDesdeMapa convierte un mapa genérico a una *Regla del borde.
// Es un parser minimal para comandos federados.
func parsearReglaDesdeMapa(m map[string]interface{}) (*Regla, error) {
	// Como el formato exacto de reglas en JSON no está completamente definido
	// para comandos federados, esta función es un stub que retorna error informativo.
	// En una implementación real, se mapearía desde tipos.Regla (ya definido en tipos).
	_ = m
	return nil, fmt.Errorf("parseo de regla desde comando federado no implementado: usar API local del borde")
}
