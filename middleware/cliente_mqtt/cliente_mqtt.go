package cliente_mqtt

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/sensorwave-dev/sensorwave/middleware"
	"github.com/sensorwave-dev/sensorwave/middleware/internal/mensaje"
)

// Constantes de reconexión
const (
	maxIntentosReconexion = 5
	backoffInicial        = 1 * time.Second
	backoffFactor         = 2
)

type ClienteMQTT struct {
	cliente       mqtt.Client
	mu            sync.Mutex
	suscripciones map[string]mqtt.MessageHandler
}

// conectar cliente con backoff exponencial
func Conectar(host string, puerto string) *ClienteMQTT {
	c := &ClienteMQTT{
		suscripciones: make(map[string]mqtt.MessageHandler),
	}

	// Configuración del cliente MQTT
	servidor := "tcp://" + host + ":" + puerto
	opts := mqtt.NewClientOptions()
	opts.AddBroker(servidor)
	opts.SetClientID("sensorwave_" + uuid.New().String())

	// Reconexión automática con re-suscripción
	opts.SetAutoReconnect(true)
	opts.SetResumeSubs(true)
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Printf("Cliente MQTT conectado al broker %s", servidor)
	})
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		log.Printf("Conexión MQTT perdida: %v (reconexión automática activada)", err)
	})

	// Crear el cliente MQTT
	c.cliente = mqtt.NewClient(opts)

	// Intentar conexión con backoff exponencial
	delay := backoffInicial
	for intento := 1; intento <= maxIntentosReconexion; intento++ {
		token := c.cliente.Connect()
		token.Wait()
		if token.Error() == nil {
			return c
		}
		log.Printf("Error al conectar al broker MQTT (intento %d/%d): %v",
			intento, maxIntentosReconexion, token.Error())
		if intento < maxIntentosReconexion {
			log.Printf("Reintentando en %v...", delay)
			time.Sleep(delay)
			delay *= backoffFactor
		}
	}

	log.Fatalf("No se pudo conectar al broker MQTT después de %d intentos", maxIntentosReconexion)
	return nil // unreachable
}

// cerrar cliente
func (c *ClienteMQTT) Desconectar() {
	// Desconectar el cliente
	c.cliente.Disconnect(250)
}

// publicar
func (c *ClienteMQTT) Publicar(topico string, payload interface{}, opciones ...middleware.PublicarOpcion) {
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

	// Publicar un mensaje en el tópico
	if token := c.cliente.Publish(topico, byte(mensaje.QoS), false, mensajeBytes); token.Wait() && token.Error() != nil {
		log.Printf("Error al publicar en %s: %v", topico, token.Error())
	}
}

// suscribir a tópico
func (c *ClienteMQTT) Suscribir(topico string, callback middleware.CallbackFunc) {
	// Suscribirse a un tópico
	callbackInterno := func(client mqtt.Client, msg mqtt.Message) {

		var mensaje middleware.Mensaje
		err := json.Unmarshal(msg.Payload(), &mensaje)
		if err != nil {
			log.Printf("Error al procesar el cuerpo de la solicitud: %v", err)
			return
		}
		callback(msg.Topic(), mensaje.Payload)
	}

	// Guardar callback para re-suscripción
	c.mu.Lock()
	c.suscripciones[topico] = callbackInterno
	c.mu.Unlock()

	if token := c.cliente.Subscribe(topico, 2, callbackInterno); token.Wait() && token.Error() != nil {
		log.Printf("Error al suscribirse a %s: %v", topico, token.Error())
	}
}

// desuscribir a tópico
func (c *ClienteMQTT) Desuscribir(topico string) {
	// Eliminar de suscripciones
	c.mu.Lock()
	delete(c.suscripciones, topico)
	c.mu.Unlock()

	// Desuscribirse de un tópico
	if token := c.cliente.Unsubscribe(topico); token.Wait() && token.Error() != nil {
		log.Printf("Error al desuscribirse de %s: %v", topico, token.Error())
	}
}
