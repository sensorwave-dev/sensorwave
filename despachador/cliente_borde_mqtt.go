package despachador

import (
	"context"
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

// clienteBordeMQTT implementa clienteBorde usando MQTT federado (swctl/#)
type clienteBordeMQTT struct {
	cliente       mqtt.Client
	consultas     map[string]*consultaPendiente
	mu            sync.RWMutex
	prefijoControl string
}

type consultaPendiente struct {
	idConsulta string
	partes     []json.RawMessage
	fin        chan struct{}
	error      chan error
	timeout    time.Duration
}

// nuevoClienteBordeMQTT crea un cliente MQTT para comunicación federada con bordes
func nuevoClienteBordeMQTT(broker string) (*clienteBordeMQTT, error) {
	opciones := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID("sw-despachador-" + uuid.New().String()).
		SetAutoReconnect(true).
		SetResumeSubs(true).
		SetConnectRetry(true)

	cliente := mqtt.NewClient(opciones)
	if token := cliente.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("error conectando al broker MQTT: %w", token.Error())
	}

	cb := &clienteBordeMQTT{
		cliente:       cliente,
		consultas:     make(map[string]*consultaPendiente),
		prefijoControl: tipos.PrefijoControl,
	}

	// Suscribirse a respuestas de todas las consultas
	token := cliente.Subscribe("swctl/consultas/+/+", 1, cb.manejarRespuesta)
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("error suscribiéndose a respuestas: %w", token.Error())
	}

	log.Printf("Cliente borde MQTT federado conectado a %s", broker)
	return cb, nil
}

func (cb *clienteBordeMQTT) manejarRespuesta(cliente mqtt.Client, msg mqtt.Message) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	// Parsear tópico
	partes := splitTopic(msg.Topic())
	if len(partes) < 3 {
		return
	}
	idConsulta := partes[2]

	consulta, ok := cb.consultas[idConsulta]
	if !ok {
		return
	}

	switch {
	case len(partes) == 5 && partes[3] == "parte":
		// swctl/consultas/{id}/parte/{indice}
		consulta.partes = append(consulta.partes, msg.Payload())
	case len(partes) == 4 && partes[3] == "fin":
		// swctl/consultas/{id}/fin
		close(consulta.fin)
	case len(partes) == 4 && partes[3] == "error":
		// swctl/consultas/{id}/error
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
	// Normalizar
	for len(t) > 0 && t[0] == '/' {
		t = t[1:]
	}
	for len(t) > 0 && t[len(t)-1] == '/' {
		t = t[:len(t)-1]
	}
	return strings.Split(t, "/")
}

// ejecutarConsulta es el motor común para todas las consultas
func (cb *clienteBordeMQTT) ejecutarConsulta(ctx context.Context, nodoID string, tipoConsulta tipos.TipoConsulta, args tipos.ConsultaArgs) ([]json.RawMessage, error) {
	idConsulta := uuid.New().String()
	topicoSolicitud := tipos.ConstruirTopicoConsultaSolicitud(nodoID, idConsulta)

	solicitud := tipos.SolicitudControlConsulta{
		Version:        1,
		IDConsulta:     idConsulta,
		IDNodo:         nodoID,
		TipoConsulta:   tipoConsulta,
		TiempoEsperaMs: 30000,
		Argumentos:     args,
	}

	consulta := &consultaPendiente{
		idConsulta: idConsulta,
		partes:     make([]json.RawMessage, 0),
		fin:        make(chan struct{}),
		error:      make(chan error, 1),
		timeout:    30 * time.Second,
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

	// Esperar respuesta o timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, consulta.timeout)
	defer cancel()

	select {
	case <-consulta.fin:
		return consulta.partes, nil
	case err := <-consulta.error:
		return nil, err
	case <-timeoutCtx.Done():
		return nil, fmt.Errorf("timeout esperando respuesta del borde %s", nodoID)
	}
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
	if len(partes) == 0 {
		return &tipos.RespuestaConsultaRango{Resultado: tipos.ResultadoConsultaRango{}}, nil
	}

	var resultado tipos.ResultadoConsultaRango
	if err := json.Unmarshal(partes[0], &resultado); err != nil {
		return nil, fmt.Errorf("error deserializando resultado: %w", err)
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
	if len(partes) == 0 {
		return &tipos.RespuestaConsultaPunto{Resultado: tipos.ResultadoConsultaPunto{}}, nil
	}

	var resultado tipos.ResultadoConsultaPunto
	if err := json.Unmarshal(partes[0], &resultado); err != nil {
		return nil, fmt.Errorf("error deserializando resultado: %w", err)
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
	if len(partes) == 0 {
		return &tipos.RespuestaConsultaAgregacion{Resultado: tipos.ResultadoAgregacion{}}, nil
	}

	var resultado tipos.ResultadoAgregacion
	if err := json.Unmarshal(partes[0], &resultado); err != nil {
		return nil, fmt.Errorf("error deserializando resultado: %w", err)
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
	if len(partes) == 0 {
		return &tipos.RespuestaConsultaAgregacionTemporal{Resultado: tipos.ResultadoAgregacionTemporal{}}, nil
	}

	var resultado tipos.ResultadoAgregacionTemporal
	if err := json.Unmarshal(partes[0], &resultado); err != nil {
		return nil, fmt.Errorf("error deserializando resultado: %w", err)
	}

	return &tipos.RespuestaConsultaAgregacionTemporal{Resultado: resultado}, nil
}
