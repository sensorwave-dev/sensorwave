package despachador

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

const timeoutConsultaDefault = 30 * time.Second

// clienteBordeMQTT implementa clienteBorde usando MQTT federado (swctl/#)
type clienteBordeMQTT struct {
	cliente       mqtt.Client
	idDespachador string
	consultas     map[string]*consultaPendiente
	mu            sync.Mutex
}

type consultaPendiente struct {
	idConsulta string
	partes     []json.RawMessage
	fin        chan struct{}
	finOnce    sync.Once
	error      chan error
	timeout    time.Duration
}

func generarIDDespachador() string {
	if v := os.Getenv("SENSORWAVE_DESPACHADOR_ID"); v != "" {
		return v
	}
	if v := os.Getenv("POD_NAME"); v != "" {
		return v
	}
	return "desp-" + uuid.New().String()
}

// nuevoClienteBordeMQTT crea un cliente MQTT para comunicación federada con bordes
func nuevoClienteBordeMQTT(broker string) (*clienteBordeMQTT, error) {
	idDespachador := generarIDDespachador()
	opciones := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID("sw-despachador-" + idDespachador).
		SetAutoReconnect(true).
		SetResumeSubs(true).
		SetConnectRetry(false).
		SetConnectTimeout(5 * time.Second)

	cliente := mqtt.NewClient(opciones)
	if token := cliente.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("error conectando al broker MQTT: %w", token.Error())
	}

	cb := &clienteBordeMQTT{
		cliente:       cliente,
		idDespachador: idDespachador,
		consultas:     make(map[string]*consultaPendiente),
	}

	filtro := tipos.ConstruirTopicoRespuestasDespachador(idDespachador)
	token := cliente.Subscribe(filtro, 1, cb.manejarRespuesta)
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("error suscribiéndose a respuestas: %w", token.Error())
	}

	log.Printf("Cliente borde MQTT federado conectado a %s (id_despachador=%s)", broker, idDespachador)
	return cb, nil
}

func (cb *clienteBordeMQTT) cerrar() {
	if cb.cliente != nil && cb.cliente.IsConnected() {
		cb.cliente.Disconnect(250)
	}
}

func (cb *clienteBordeMQTT) manejarRespuesta(_ mqtt.Client, msg mqtt.Message) {
	partesTopico := splitTopic(msg.Topic())
	// swctl/despachadores/{idDesp}/consultas/{idConsulta}/parte|error/...
	if len(partesTopico) < 6 ||
		partesTopico[0] != "swctl" ||
		partesTopico[1] != "despachadores" ||
		partesTopico[3] != "consultas" {
		return
	}
	if partesTopico[2] != cb.idDespachador {
		return
	}
	idConsulta := partesTopico[4]

	cb.mu.Lock()
	defer cb.mu.Unlock()

	consulta, ok := cb.consultas[idConsulta]
	if !ok {
		return
	}

	switch {
	case len(partesTopico) == 7 && partesTopico[5] == "parte":
		payload := make([]byte, len(msg.Payload()))
		copy(payload, msg.Payload())
		consulta.partes = append(consulta.partes, json.RawMessage(payload))
		consulta.finOnce.Do(func() { close(consulta.fin) })
	case len(partesTopico) == 6 && partesTopico[5] == "error":
		var errResp tipos.RespuestaControlConsultaError
		if err := json.Unmarshal(msg.Payload(), &errResp); err == nil {
			select {
			case consulta.error <- fmt.Errorf("%s: %s", errResp.Codigo, errResp.Mensaje):
			default:
			}
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

func timeoutDesdeContexto(ctx context.Context, defecto time.Duration) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		restante := time.Until(deadline)
		if restante <= 0 {
			return time.Millisecond
		}
		return min(restante, defecto)
	}
	return defecto
}

// ejecutarConsulta es el motor común para todas las consultas
func (cb *clienteBordeMQTT) ejecutarConsulta(ctx context.Context, nodoID string, tipoConsulta tipos.TipoConsulta, args tipos.ConsultaArgs) ([]json.RawMessage, error) {
	idConsulta := uuid.New().String()
	topicoSolicitud := tipos.ConstruirTopicoConsultaSolicitud(nodoID, idConsulta)
	timeout := timeoutDesdeContexto(ctx, timeoutConsultaDefault)

	solicitud := tipos.SolicitudControlConsulta{
		Version:        1,
		IDConsulta:     idConsulta,
		IDNodo:         nodoID,
		IDDespachador:  cb.idDespachador,
		TipoConsulta:   tipoConsulta,
		TiempoEsperaMs: int(timeout / time.Millisecond),
		Argumentos:     args,
	}

	consulta := &consultaPendiente{
		idConsulta: idConsulta,
		partes:     make([]json.RawMessage, 0),
		fin:        make(chan struct{}),
		error:      make(chan error, 1),
		timeout:    timeout,
	}

	cb.mu.Lock()
	cb.consultas[idConsulta] = consulta
	cb.mu.Unlock()

	defer func() {
		cb.mu.Lock()
		delete(cb.consultas, idConsulta)
		cb.mu.Unlock()
	}()

	payload, err := json.Marshal(solicitud)
	if err != nil {
		return nil, fmt.Errorf("error serializando solicitud: %w", err)
	}

	if token := cb.cliente.Publish(topicoSolicitud, 1, false, payload); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("error publicando solicitud: %w", token.Error())
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, consulta.timeout)
	defer cancel()

	select {
	case <-consulta.fin:
		cb.mu.Lock()
		partes := append([]json.RawMessage(nil), consulta.partes...)
		cb.mu.Unlock()
		if len(partes) == 0 {
			return nil, fmt.Errorf("respuesta incompleta del borde %s: fin sin partes", nodoID)
		}
		return partes, nil
	case err := <-consulta.error:
		return nil, err
	case <-timeoutCtx.Done():
		cb.publicarCancelacion(nodoID, idConsulta)
		if errors.Is(timeoutCtx.Err(), context.Canceled) {
			return nil, fmt.Errorf("consulta cancelada hacia borde %s: %w", nodoID, context.Canceled)
		}
		return nil, fmt.Errorf("timeout esperando respuesta del borde %s", nodoID)
	}
}

// publicarCancelacion avisa al borde (best-effort) que abandone la consulta.
func (cb *clienteBordeMQTT) publicarCancelacion(nodoID, idConsulta string) {
	topico := tipos.ConstruirTopicoConsultaCancelar(nodoID, idConsulta)
	token := cb.cliente.Publish(topico, 1, false, []byte{})
	if token.Wait() && token.Error() != nil {
		log.Printf("aviso: no se pudo publicar cancelación de consulta %s hacia %s: %v", idConsulta, nodoID, token.Error())
	}
}

func deserializarResultadoParte[T any](raw json.RawMessage) (T, error) {
	var zero T
	var parte tipos.RespuestaControlConsultaParte
	if err := json.Unmarshal(raw, &parte); err != nil {
		return zero, fmt.Errorf("error deserializando envelope de parte: %w", err)
	}
	if len(parte.Resultado) == 0 {
		return zero, fmt.Errorf("parte de consulta sin campo resultado")
	}
	var resultado T
	if err := json.Unmarshal(parte.Resultado, &resultado); err != nil {
		return zero, fmt.Errorf("error deserializando resultado: %w", err)
	}
	return resultado, nil
}

// ConsultarRango implementa clienteBorde
func (cb *clienteBordeMQTT) ConsultarRango(ctx context.Context, nodoID string, direccion string, req tipos.SolicitudConsultaRango) (*tipos.RespuestaConsultaRango, error) {
	partes, err := cb.ejecutarConsulta(ctx, nodoID, tipos.ConsultaRango, tipos.ConsultaArgs{
		Serie:        req.Serie,
		TiempoInicio: req.TiempoInicio,
		TiempoFin:    req.TiempoFin,
	})
	if err != nil {
		return nil, err
	}

	resultado, err := deserializarResultadoParte[tipos.ResultadoConsultaRango](partes[0])
	if err != nil {
		return nil, err
	}
	return &tipos.RespuestaConsultaRango{Resultado: resultado}, nil
}

// ConsultarUltimoPunto implementa clienteBorde
func (cb *clienteBordeMQTT) ConsultarUltimoPunto(ctx context.Context, nodoID string, direccion string, req tipos.SolicitudConsultaPunto) (*tipos.RespuestaConsultaPunto, error) {
	partes, err := cb.ejecutarConsulta(ctx, nodoID, tipos.ConsultaUltimo, tipos.ConsultaArgs{
		Serie:           req.Serie,
		TiempoInicioPtr: req.TiempoInicio,
		TiempoFinPtr:    req.TiempoFin,
	})
	if err != nil {
		return nil, err
	}

	resultado, err := deserializarResultadoParte[tipos.ResultadoConsultaPunto](partes[0])
	if err != nil {
		return nil, err
	}
	return &tipos.RespuestaConsultaPunto{Resultado: resultado}, nil
}

// ConsultarAgregacion implementa clienteBorde
func (cb *clienteBordeMQTT) ConsultarAgregacion(ctx context.Context, nodoID string, direccion string, req tipos.SolicitudConsultaAgregacion) (*tipos.RespuestaConsultaAgregacion, error) {
	partes, err := cb.ejecutarConsulta(ctx, nodoID, tipos.ConsultaAgregacion, tipos.ConsultaArgs{
		Serie:        req.Serie,
		TiempoInicio: req.TiempoInicio,
		TiempoFin:    req.TiempoFin,
		Agregaciones: req.Agregaciones,
	})
	if err != nil {
		return nil, err
	}

	resultado, err := deserializarAgregacion(partes[0])
	if err != nil {
		return nil, err
	}
	return &tipos.RespuestaConsultaAgregacion{Resultado: resultado}, nil
}

// ConsultarAgregacionTemporal implementa clienteBorde
func (cb *clienteBordeMQTT) ConsultarAgregacionTemporal(ctx context.Context, nodoID string, direccion string, req tipos.SolicitudConsultaAgregacionTemporal) (*tipos.RespuestaConsultaAgregacionTemporal, error) {
	partes, err := cb.ejecutarConsulta(ctx, nodoID, tipos.ConsultaAgregacionTemporal, tipos.ConsultaArgs{
		Serie:        req.Serie,
		TiempoInicio: req.TiempoInicio,
		TiempoFin:    req.TiempoFin,
		Agregaciones: req.Agregaciones,
		Intervalo:    req.Intervalo,
	})
	if err != nil {
		return nil, err
	}

	resultado, err := deserializarAgregacionTemporal(partes[0])
	if err != nil {
		return nil, err
	}
	return &tipos.RespuestaConsultaAgregacionTemporal{Resultado: resultado}, nil
}

func deserializarAgregacion(raw json.RawMessage) (tipos.ResultadoAgregacion, error) {
	var parte tipos.RespuestaControlConsultaParte
	if err := json.Unmarshal(raw, &parte); err != nil {
		return tipos.ResultadoAgregacion{}, fmt.Errorf("error deserializando envelope de parte: %w", err)
	}
	var aux struct {
		Series             []string               `json:"Series"`
		Agregaciones       []tipos.TipoAgregacion `json:"Agregaciones"`
		Valores            [][]tipos.FloatNulo    `json:"Valores"`
		NodosNoDisponibles []string               `json:"NodosNoDisponibles"`
	}
	if err := json.Unmarshal(parte.Resultado, &aux); err != nil {
		return tipos.ResultadoAgregacion{}, fmt.Errorf("error deserializando resultado: %w", err)
	}
	out := tipos.ResultadoAgregacion{
		Series:             aux.Series,
		Agregaciones:       aux.Agregaciones,
		NodosNoDisponibles: aux.NodosNoDisponibles,
		Valores:            make([][]float64, len(aux.Valores)),
	}
	for i := range aux.Valores {
		out.Valores[i] = make([]float64, len(aux.Valores[i]))
		for j := range aux.Valores[i] {
			out.Valores[i][j] = float64(aux.Valores[i][j])
			if aux.Valores[i][j].EsNulo() {
				out.Valores[i][j] = math.NaN()
			}
		}
	}
	return out, nil
}

func deserializarAgregacionTemporal(raw json.RawMessage) (tipos.ResultadoAgregacionTemporal, error) {
	var parte tipos.RespuestaControlConsultaParte
	if err := json.Unmarshal(raw, &parte); err != nil {
		return tipos.ResultadoAgregacionTemporal{}, fmt.Errorf("error deserializando envelope de parte: %w", err)
	}
	var aux struct {
		Series             []string               `json:"Series"`
		Tiempos            []int64                `json:"Tiempos"`
		Agregaciones       []tipos.TipoAgregacion `json:"Agregaciones"`
		Valores            [][][]tipos.FloatNulo  `json:"Valores"`
		NodosNoDisponibles []string               `json:"NodosNoDisponibles"`
	}
	if err := json.Unmarshal(parte.Resultado, &aux); err != nil {
		return tipos.ResultadoAgregacionTemporal{}, fmt.Errorf("error deserializando resultado: %w", err)
	}
	out := tipos.ResultadoAgregacionTemporal{
		Series:             aux.Series,
		Tiempos:            aux.Tiempos,
		Agregaciones:       aux.Agregaciones,
		NodosNoDisponibles: aux.NodosNoDisponibles,
		Valores:            make([][][]float64, len(aux.Valores)),
	}
	for i := range aux.Valores {
		out.Valores[i] = make([][]float64, len(aux.Valores[i]))
		for j := range aux.Valores[i] {
			out.Valores[i][j] = make([]float64, len(aux.Valores[i][j]))
			for k := range aux.Valores[i][j] {
				if aux.Valores[i][j][k].EsNulo() {
					out.Valores[i][j][k] = math.NaN()
				} else {
					out.Valores[i][j][k] = float64(aux.Valores[i][j][k])
				}
			}
		}
	}
	return out, nil
}
