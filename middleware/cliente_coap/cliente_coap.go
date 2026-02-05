package cliente_coap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	obs "github.com/plgd-dev/go-coap/v3/net/client"
	"github.com/plgd-dev/go-coap/v3/udp"
	"github.com/plgd-dev/go-coap/v3/udp/client"
	"github.com/sensorwave-dev/sensorwave/middleware"
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
func (c *ClienteCoAP) Publicar(topico string, payload interface{}) {
	var data []byte
	switch v := payload.(type) {
	case string:
		data = []byte(v) // Si es un string, convertir directamente a []byte
	case []byte:
		data = v // Si ya es []byte, usarlo directamente
	case int, int32, int64, float32, float64:
		data = []byte(fmt.Sprintf("%v", v)) // Convertir números a string y luego a []byte
	default:
		// Para otros tipos, usar JSON como formato de serialización
		var err error
		data, err = json.Marshal(v)
		if err != nil {
			log.Fatalf("Error al serializar el payload: %v", err)
		}
	}

	mensaje := middleware.Mensaje{Original: true, Topico: topico, Payload: data, Interno: false}

	// Serializar el mensaje a JSON
	mensajeBytes, err := json.Marshal(mensaje)
	if err != nil {
		log.Fatalf("Error al serializar el mensaje: %v", err)
	}

	// publicar en el recurso
	ctx := context.Background()
	path := fmt.Sprintf("%s?topico=%s", ruta, url.QueryEscape(topico))
	_, err = c.cliente.Post(ctx, path, message.TextPlain, bytes.NewReader(mensajeBytes))
	if err != nil {
		log.Fatalf("Error : %v", err)
	}
}

// suscribir a tópico
// A futuro si ya estoy suscripto, primero desuscribir y luego suscribir
func (c *ClienteCoAP) Suscribir(topico string, callback middleware.CallbackFunc) {
	// subscribe al recurso
	ctx := context.Background()
	path := fmt.Sprintf("%s?topico=%s", ruta, url.QueryEscape(topico))
	internalCallback := func(msg *pool.Message) {
		var mensaje middleware.Mensaje
		if p, err := msg.ReadBody(); err == nil && len(p) > 0 {
			err := json.Unmarshal(p, &mensaje)
			if err != nil {
				log.Fatalf("Error al procesar el cuerpo de la solicitud: " + err.Error())
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
	obs, err := c.cliente.Observe(ctx, path, internalCallback)
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
