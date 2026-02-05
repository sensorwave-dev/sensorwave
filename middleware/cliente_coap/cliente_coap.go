package cliente_coap

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/url"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	obs "github.com/plgd-dev/go-coap/v3/net/client"
	"github.com/plgd-dev/go-coap/v3/udp"
	"github.com/plgd-dev/go-coap/v3/udp/client"
	"github.com/sensorwave-dev/sensorwave/middleware"
	"github.com/sensorwave-dev/sensorwave/middleware/internal/mensaje"
)

var ruta string = "/sensorwave"

// tipo del cliente
type ClienteCoAP struct {
	cliente *client.Conn
}

// almacena suscripciones del cliente
var observaciones = make(map[string]obs.Observation)

// conectar cliente
func Conectar(direccion string, puerto string) *ClienteCoAP {
	servidor := direccion + ":" + puerto
	log.Println("Conectandose a: ", servidor)
	cliente, err := udp.Dial(servidor)
	if err != nil {
		log.Fatalf("Error al conectarse: %v", err)
	}
	return &ClienteCoAP{cliente: cliente}
}

// cerrar cliente
func (c *ClienteCoAP) Desconectar() {
	c.cliente.Close()
}

// publicar
func (c *ClienteCoAP) Publicar(topico string, payload interface{}, opciones ...middleware.PublicarOpcion) {
	mensaje, err := mensaje.Construir(topico, payload, opciones...)
	if err != nil {
		log.Fatalf("Error al construir mensaje: %v", err)
	}

	// Serializar el mensaje a JSON
	mensajeBytes, err := json.Marshal(mensaje)
	if err != nil {
		log.Fatalf("Error al serializar el mensaje: %v", err)
	}

	// publicar en el recurso
	ctx := context.Background()
	query := message.Option{ID: message.URIQuery, Value: []byte("topico=" + url.QueryEscape(topico))}
	req, err := c.cliente.NewPostRequest(ctx, ruta, message.TextPlain, bytes.NewReader(mensajeBytes), query)
	if err != nil {
		log.Fatalf("Error al crear la solicitud: %v", err)
	}
	if mensaje.QoS == 1 {
		req.SetType(message.Confirmable)
	} else {
		req.SetType(message.NonConfirmable)
	}
	_, err = c.cliente.Do(req)
	if err != nil {
		log.Fatalf("Error : %v", err)
	}
}

// suscribir a tópico
// A futuro si ya estoy suscripto, primero desuscribir y luego suscribir
func (c *ClienteCoAP) Suscribir(topico string, callback middleware.CallbackFunc) {
	// subscribe al recurso
	ctx := context.Background()
	query := message.Option{ID: message.URIQuery, Value: []byte("topico=" + url.QueryEscape(topico))}
	internalCallback := func(msg *pool.Message) {
		var mensaje middleware.Mensaje
		if p, err := msg.ReadBody(); err == nil && len(p) > 0 {
			err := json.Unmarshal(p, &mensaje)
			if err != nil {
				log.Fatalf("Error al procesar el cuerpo de la solicitud: %v", err)
				return
			}
		}
		// si es un mensaje interno, no lo procesamos
		if mensaje.Interno {
			log.Printf("Mensaje interno, ignorando")
			return
		}
		callback(topico, string(mensaje.Payload))
	}
	obs, err := c.cliente.Observe(ctx, ruta, internalCallback, query)
	if err != nil {
		log.Fatalf("Error : %v", err)
	}
	observaciones[topico] = obs
}

// se desuscribe a un topico
func (c *ClienteCoAP) Desuscribir(topico string) {
	obs, ok := observaciones[topico]
	if !ok {
		log.Printf("No hay observación activa en %s", topico)
		return
	}
	if err := obs.Cancel(context.Background()); err != nil {
		log.Printf("Error al cancelar %s: %v", topico, err)
		return
	}
	delete(observaciones, topico)
}
