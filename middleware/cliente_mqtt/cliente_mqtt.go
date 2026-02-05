package cliente_mqtt

import (
	"encoding/json"
	"fmt"
	"log"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/sensorwave-dev/sensorwave/middleware"
	"github.com/sensorwave-dev/sensorwave/middleware/internal/mensaje"
)

type ClienteMQTT struct {
	cliente mqtt.Client
}

// conectar cliente
func Conectar(direccion string, puerto string) *ClienteMQTT {
	// Configuración del cliente MQTT
	servidor := "tcp://" + direccion + ":" + puerto
	opts := mqtt.NewClientOptions()
	opts.AddBroker(servidor)
	opts.SetClientID("sensorwave_" + uuid.New().String())

	// Crear el cliente MQTT
	cliente := mqtt.NewClient(opts)
	if token := cliente.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error al conectar al broker MQTT: %v", token.Error())
	}
	fmt.Println("Conectado al broker MQTT")
	return &ClienteMQTT{cliente: cliente}
}

// cerrar cliente
func (c *ClienteMQTT) Desconectar() {
	// Desconectar el cliente
	c.cliente.Disconnect(250)
	fmt.Println("Cliente desconectado")
}

// publicar
func (c *ClienteMQTT) Publicar(topico string, payload interface{}, opciones ...middleware.PublicarOpcion) {
	mensaje, err := mensaje.Construir(topico, payload, opciones...)
	if err != nil {
		log.Fatalf("Error al construir mensaje: %v", err)
	}

	// Serializar el mensaje a JSON
	mensajeBytes, err := json.Marshal(mensaje)
	if err != nil {
		log.Fatalf("Error al serializar el mensaje: %v", err)
	}

	// Publicar un mensaje en el tópico
	if token := c.cliente.Publish(topico, byte(mensaje.QoS), false, mensajeBytes); token.Wait() && token.Error() != nil {
		log.Fatalf("Error: %v", token.Error())
	}
}

// suscribir a tópico
func (c *ClienteMQTT) Suscribir(topico string, callback middleware.CallbackFunc) {
	// Suscribirse a un tópico
	internalCallback := func(client mqtt.Client, msg mqtt.Message) {

		var mensaje middleware.Mensaje
		err := json.Unmarshal(msg.Payload(), &mensaje)
		if err != nil {
			log.Fatalf("Error al procesar el cuerpo de la solicitud: %v", err)
			return
		}
		callback(msg.Topic(), string(mensaje.Payload))
	}

	if token := c.cliente.Subscribe(topico, 0, internalCallback); token.Wait() && token.Error() != nil {
		log.Fatalf("Error: %v", token.Error())
	}
}

// desuscribir a tópico
func (c *ClienteMQTT) Desuscribir(topico string) {
	// Desuscribirse de un tópico
	if token := c.cliente.Unsubscribe(topico); token.Wait() && token.Error() != nil {
		log.Fatalf("Error: %v", token.Error())
	}
}
