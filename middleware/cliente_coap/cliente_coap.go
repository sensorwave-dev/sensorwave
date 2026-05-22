package cliente_coap

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	obs "github.com/plgd-dev/go-coap/v3/net/client"
	"github.com/plgd-dev/go-coap/v3/udp"
	"github.com/plgd-dev/go-coap/v3/udp/client"
	"github.com/sensorwave-dev/sensorwave/middleware"
	"github.com/sensorwave-dev/sensorwave/middleware/internal/mensaje"
)

var ruta string = "/sensorwave"

// Constantes de reconexión
const (
	maxIntentosReconexion = 5
	backoffInicial        = 1 * time.Second
	backoffFactor         = 2
)

// tipo del cliente
type ClienteCoAP struct {
	cliente       *client.Conn
	direccion     string
	mu            sync.Mutex
	observaciones map[string]obs.Observation
	// Guardar callbacks para re-observación tras reconexión
	callbacks map[string]middleware.CallbackFunc
}

// conectar cliente con backoff exponencial
func Conectar(host string, puerto string) *ClienteCoAP {
	servidor := host + ":" + puerto

	c := &ClienteCoAP{
		direccion:     servidor,
		observaciones: make(map[string]obs.Observation),
		callbacks:     make(map[string]middleware.CallbackFunc),
	}

	delay := backoffInicial
	for intento := 1; intento <= maxIntentosReconexion; intento++ {
		conn, err := udp.Dial(servidor)
		if err == nil {
			c.cliente = conn
			return c
		}
		log.Printf("Error al conectarse (intento %d/%d): %v",
			intento, maxIntentosReconexion, err)
		if intento < maxIntentosReconexion {
			log.Printf("Reintentando en %v...", delay)
			time.Sleep(delay)
			delay *= backoffFactor
		}
	}

	log.Fatalf("No se pudo conectar al servidor CoAP después de %d intentos", maxIntentosReconexion)
	return nil // unreachable
}

// cerrar cliente
func (c *ClienteCoAP) Desconectar() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cliente.Close()
	c.observaciones = make(map[string]obs.Observation)
	c.callbacks = make(map[string]middleware.CallbackFunc)
}

// publicar
func (c *ClienteCoAP) Publicar(topico string, payload interface{}, opciones ...middleware.PublicarOpcion) {
	mensaje, err := mensaje.Construir(topico, payload, opciones...)
	if err != nil {
		log.Printf("Error al construir mensaje: %v", err)
		return
	}

	// Serializar el mensaje a JSON
	mensajeBytes, err := json.Marshal(mensaje)
	if err != nil {
		log.Printf("Error al serializar el mensaje: %v", err)
		return
	}

	// publicar en el recurso
	ctx := context.Background()
	query := message.Option{ID: message.URIQuery, Value: []byte("topico=" + url.QueryEscape(topico))}
	req, err := c.cliente.NewPostRequest(ctx, ruta, message.TextPlain, bytes.NewReader(mensajeBytes), query)
	if err != nil {
		log.Printf("Error al crear la solicitud: %v", err)
		return
	}
	if mensaje.QoS == 1 {
		req.SetType(message.Confirmable)
	} else {
		req.SetType(message.NonConfirmable)
	}
	_, err = c.cliente.Do(req)
	if err != nil {
		log.Printf("Error al publicar en %s: %v", topico, err)
	}
}

// suscribir a tópico
func (c *ClienteCoAP) Suscribir(topico string, callback middleware.CallbackFunc) {
	// subscribe al recurso
	ctx := context.Background()
	query := message.Option{ID: message.URIQuery, Value: []byte("topico=" + url.QueryEscape(topico))}
	callbackInterno := func(msg *pool.Message) {
		var mensaje middleware.Mensaje
		if p, err := msg.ReadBody(); err == nil && len(p) > 0 {
			err := json.Unmarshal(p, &mensaje)
			if err != nil {
				log.Printf("Error al procesar el cuerpo de la solicitud: %v", err)
				return
			}
		}
		// si es un mensaje interno, no lo procesamos
		if mensaje.Interno {
			return
		}
		callback(mensaje.Topico, mensaje.Payload)
	}
	observation, err := c.cliente.Observe(ctx, ruta, callbackInterno, query)
	if err != nil {
		log.Printf("Error al observar %s: %v", topico, err)
		return
	}

	c.mu.Lock()
	c.observaciones[topico] = observation
	c.callbacks[topico] = callback
	c.mu.Unlock()
}

// se desuscribe a un topico
func (c *ClienteCoAP) Desuscribir(topico string) {
	c.mu.Lock()
	observation, ok := c.observaciones[topico]
	if !ok {
		c.mu.Unlock()
		return
	}
	delete(c.observaciones, topico)
	delete(c.callbacks, topico)
	c.mu.Unlock()

	if err := observation.Cancel(context.Background()); err != nil {
		log.Printf("Error al cancelar observación de %s: %v", topico, err)
	}
}
