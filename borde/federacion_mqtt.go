package borde

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

const ttlConsultasCanceladas = 2 * time.Minute

// ============================================================================
// FEDERACION MQTT (BORDE)
// ============================================================================

// federacionMQTT gestiona la conexión MQTT del borde para recibir consultas
// de la nube vía el plano de control swctl/#.
type federacionMQTT struct {
	cliente    mqtt.Client
	gestor     *GestorBorde
	finalizado chan struct{}
	wg         sync.WaitGroup

	// Consultas canceladas por el despachador (MVP con TTL)
	canceladasMu sync.Mutex
	canceladas   map[string]time.Time // idConsulta -> timestamp
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
		cliente:    cliente,
		gestor:     me,
		finalizado: make(chan struct{}),
		canceladas: make(map[string]time.Time),
	}

	topicoSuscribir := fmt.Sprintf("swctl/nodos/%s/consulta/#", me.nodoID)
	token := cliente.Subscribe(topicoSuscribir, 1, f.manejarMensaje)
	if token.Wait() && token.Error() != nil {
		cliente.Disconnect(250)
		return nil, fmt.Errorf("error suscribiéndose a %s: %w", topicoSuscribir, token.Error())
	}

	f.wg.Add(1)
	go f.limpiarCancelaciones()

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
// Acusa el mensaje al entrar: con el orden por defecto de Paho el PUBACK de
// una solicitud QoS 1 no sale hasta que este callback termina, y publicar la
// respuesta esperando el acuse desde aquí deja la consulta bloqueada.
func (f *federacionMQTT) manejarMensaje(_ mqtt.Client, msg mqtt.Message) {
	msg.Ack()

	partes := splitTopic(msg.Topic())
	if len(partes) != 6 {
		return
	}

	// Formato: swctl/nodos/{nodoID}/consulta/{solicitud|cancelar}/{id}
	if partes[0] != "swctl" || partes[1] != "nodos" || partes[2] != f.gestor.nodoID || partes[3] != "consulta" {
		return
	}

	switch partes[4] {
	case "solicitud":
		payload := bytes.Clone(msg.Payload())
		go f.manejarConsulta(payload, partes[5])
	case "cancelar":
		f.manejarCancelacion(partes[5])
	}
}

// ============================================================================
// CONSULTAS
// ============================================================================

func (f *federacionMQTT) manejarCancelacion(idConsulta string) {
	if idConsulta == "" {
		return
	}
	f.canceladasMu.Lock()
	f.canceladas[idConsulta] = time.Now()
	f.canceladasMu.Unlock()
}

func (f *federacionMQTT) consultaCancelada(idConsulta string) bool {
	f.canceladasMu.Lock()
	defer f.canceladasMu.Unlock()
	_, ok := f.canceladas[idConsulta]
	return ok
}

func (f *federacionMQTT) manejarConsulta(payload []byte, idConsulta string) {
	if f.consultaCancelada(idConsulta) {
		return
	}

	var solicitud tipos.SolicitudControlConsulta
	if err := json.Unmarshal(payload, &solicitud); err != nil {
		f.publicarErrorConsulta("", idConsulta, "parse_error", err.Error())
		return
	}
	idDespachador := solicitud.IDDespachador
	if idDespachador == "" {
		f.publicarErrorConsulta("", idConsulta, "reply_to_faltante", "id_despachador es requerido")
		return
	}

	resultado, err := f.ejecutarConsulta(solicitud)
	if err != nil {
		if f.consultaCancelada(idConsulta) {
			return
		}
		f.publicarErrorConsulta(idDespachador, idConsulta, "consulta_error", err.Error())
		return
	}

	if f.consultaCancelada(idConsulta) {
		return
	}

	parteJSON, err := serializarResultadoControl(resultado)
	if err != nil {
		f.publicarErrorConsulta(idDespachador, idConsulta, "serializacion_error", err.Error())
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

	topicoParte := tipos.ConstruirTopicoConsultaParte(idDespachador, idConsulta, 0)
	token := f.cliente.Publish(topicoParte, 1, false, parteBytes)
	if token.Wait() && token.Error() != nil {
		f.publicarErrorConsulta(idDespachador, idConsulta, "publicacion_error", token.Error().Error())
	}
}

func (f *federacionMQTT) ejecutarConsulta(solicitud tipos.SolicitudControlConsulta) (any, error) {
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

func (f *federacionMQTT) publicarErrorConsulta(idDespachador, idConsulta, codigo, mensaje string) {
	errResp := tipos.RespuestaControlConsultaError{
		Version:    1,
		IDConsulta: idConsulta,
		IDNodo:     f.gestor.nodoID,
		Codigo:     codigo,
		Mensaje:    mensaje,
	}
	payload, _ := json.Marshal(errResp)
	if idDespachador == "" {
		log.Printf("Error de consulta %s sin id_despachador (%s: %s); no se publica respuesta", idConsulta, codigo, mensaje)
		return
	}
	topico := tipos.ConstruirTopicoConsultaError(idDespachador, idConsulta)
	f.cliente.Publish(topico, 1, false, payload)
}

// serializarResultadoControl codifica el resultado a JSON, convirtiendo NaN a null
// en agregaciones (JSON estándar no admite NaN).
func serializarResultadoControl(resultado any) ([]byte, error) {
	switch r := resultado.(type) {
	case tipos.ResultadoAgregacion:
		type out struct {
			Series             []string               `json:"Series"`
			Agregaciones       []tipos.TipoAgregacion `json:"Agregaciones"`
			Valores            [][]tipos.FloatNulo    `json:"Valores"`
			NodosNoDisponibles []string               `json:"NodosNoDisponibles,omitempty"`
		}
		o := out{Series: r.Series, Agregaciones: r.Agregaciones, NodosNoDisponibles: r.NodosNoDisponibles}
		o.Valores = make([][]tipos.FloatNulo, len(r.Valores))
		for i := range r.Valores {
			o.Valores[i] = make([]tipos.FloatNulo, len(r.Valores[i]))
			for j := range r.Valores[i] {
				o.Valores[i][j] = tipos.FloatNulo(r.Valores[i][j])
			}
		}
		return json.Marshal(o)
	case tipos.ResultadoAgregacionTemporal:
		type out struct {
			Series             []string               `json:"Series"`
			Tiempos            []int64                `json:"Tiempos"`
			Agregaciones       []tipos.TipoAgregacion `json:"Agregaciones"`
			Valores            [][][]tipos.FloatNulo  `json:"Valores"`
			NodosNoDisponibles []string               `json:"NodosNoDisponibles,omitempty"`
		}
		o := out{Series: r.Series, Tiempos: r.Tiempos, Agregaciones: r.Agregaciones, NodosNoDisponibles: r.NodosNoDisponibles}
		o.Valores = make([][][]tipos.FloatNulo, len(r.Valores))
		for i := range r.Valores {
			o.Valores[i] = make([][]tipos.FloatNulo, len(r.Valores[i]))
			for j := range r.Valores[i] {
				o.Valores[i][j] = make([]tipos.FloatNulo, len(r.Valores[i][j]))
				for k := range r.Valores[i][j] {
					o.Valores[i][j][k] = tipos.FloatNulo(r.Valores[i][j][k])
				}
			}
		}
		return json.Marshal(o)
	default:
		return json.Marshal(resultado)
	}
}

// limpiarCancelaciones elimina entradas de cancelación vencidas.
func (f *federacionMQTT) limpiarCancelaciones() {
	defer f.wg.Done()
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-f.finalizado:
			return
		case <-ticker.C:
			limite := time.Now().Add(-ttlConsultasCanceladas)
			f.canceladasMu.Lock()
			maps.DeleteFunc(f.canceladas, func(_ string, ts time.Time) bool {
				return ts.Before(limite)
			})
			f.canceladasMu.Unlock()
		}
	}
}

func splitTopic(t string) []string {
	for len(t) > 0 && t[0] == '/' {
		t = t[1:]
	}
	for len(t) > 0 && t[len(t)-1] == '/' {
		t = t[:len(t)-1]
	}
	return strings.Split(t, "/")
}
