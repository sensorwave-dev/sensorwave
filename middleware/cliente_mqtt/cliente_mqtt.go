package cliente_mqtt

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/sensorwave-dev/sensorwave/middleware"
	"github.com/sensorwave-dev/sensorwave/middleware/internal/errores"
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
func Conectar(host string, puerto string) (*ClienteMQTT, error) {
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
	var ultimoErr error
	for intento := 1; intento <= maxIntentosReconexion; intento++ {
		token := c.cliente.Connect()
		token.Wait()
		if token.Error() == nil {
			return c, nil
		}
		ultimoErr = token.Error()
		log.Printf("Error al conectar al broker MQTT (intento %d/%d): %v",
			intento, maxIntentosReconexion, ultimoErr)
		if intento < maxIntentosReconexion {
			log.Printf("Reintentando en %v...", delay)
			time.Sleep(delay)
			delay *= backoffFactor
		}
	}

	return nil, fmt.Errorf("%w: no se pudo conectar al broker MQTT después de %d intentos: %v",
		errores.ErrConexion, maxIntentosReconexion, ultimoErr)
}

// cerrar cliente
func (c *ClienteMQTT) Desconectar() {
	// Desconectar el cliente
	c.cliente.Disconnect(250)
}

// publicar
func (c *ClienteMQTT) Publicar(topico string, payload interface{}, opciones ...middleware.PublicarOpcion) error {
	mensaje, err := mensaje.Construir(topico, payload, opciones...)
	if err != nil {
		return fmt.Errorf("%w: %v", errores.ErrPublicacion, err)
	}

	// Serializar el mensaje a JSON
	mensajeBytes, err := json.Marshal(mensaje)
	if err != nil {
		return fmt.Errorf("%w: %v", errores.ErrPublicacion, err)
	}

	// Publicar un mensaje en el tópico
	if token := c.cliente.Publish(topico, byte(mensaje.QoS), false, mensajeBytes); token.Wait() && token.Error() != nil {
		return fmt.Errorf("%w: %s: %v", errores.ErrPublicacion, topico, token.Error())
	}
	return nil
}

// suscribir a tópico
func (c *ClienteMQTT) Suscribir(topico string, callback middleware.CallbackFunc) error {
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

	if token := c.cliente.Subscribe(topico, 1, callbackInterno); token.Wait() && token.Error() != nil {
		// Revertir el registro si la suscripción falló
		c.mu.Lock()
		delete(c.suscripciones, topico)
		c.mu.Unlock()
		return fmt.Errorf("%w: %s: %v", errores.ErrSuscripcion, topico, token.Error())
	}
	return nil
}

// desuscribir a tópico. Idempotente: retorna nil si el tópico no estaba suscrito.
// Devuelve error sólo si falla la cancelación de red (Unsubscribe).
func (c *ClienteMQTT) Desuscribir(topico string) error {
	// Eliminar de suscripciones (idempotente: nil si no existe)
	c.mu.Lock()
	_, ok := c.suscripciones[topico]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	delete(c.suscripciones, topico)
	c.mu.Unlock()

	// Desuscribirse de un tópico
	if token := c.cliente.Unsubscribe(topico); token.Wait() && token.Error() != nil {
		return fmt.Errorf("%w: %s: %v", errores.ErrDesuscripcion, topico, token.Error())
	}
	return nil
}
