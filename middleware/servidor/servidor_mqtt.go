package servidor

import (
	"encoding/json"
	"strings"
	"sync"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

const LOG_MQTT = "MQTT"

var clienteMQTT MQTT.Client

var (
	brokerMQTTMu   sync.RWMutex
	brokerMQTTHost = "localhost"
)

// ConfigurarBrokerMQTT permite definir el host (o URL base) del broker.
// Si host está vacío, se vuelve al comportamiento por defecto (localhost).
func ConfigurarBrokerMQTT(host string) {
	host = strings.TrimSpace(host)
	if host == "" {
		host = "localhost"
	}

	brokerMQTTMu.Lock()
	brokerMQTTHost = host
	brokerMQTTMu.Unlock()
}

func obtenerBrokerMQTTHost() string {
	brokerMQTTMu.RLock()
	host := brokerMQTTHost
	brokerMQTTMu.RUnlock()
	return host
}

func construirBrokerMQTT(host, puerto string) string {
	if strings.Contains(host, "://") {
		if puerto == "" {
			return host
		}
		base := strings.SplitN(host, "://", 2)
		if len(base) == 2 && strings.Contains(base[1], ":") {
			return host
		}
		return host + ":" + puerto
	}

	if puerto == "" {
		return "tcp://" + host
	}

	return "tcp://" + host + ":" + puerto
}

// Iniciar el servidor MQTT
func IniciarMQTT(puerto string) {
	broker := construirBrokerMQTT(obtenerBrokerMQTTHost(), puerto)
	loggerPrint(LOG_MQTT, "Servidor iniciado - Broker: %s", broker)

	// Crear un nuevo cliente MQTT
	opciones := MQTT.NewClientOptions().AddBroker(broker)
	opciones.SetClientID("SensorWaveMQTT")

	// Conectar al servidor MQTT
	// crea un cliente MQTT
	clienteMQTT = MQTT.NewClient(opciones)
	if token := clienteMQTT.Connect(); token.Wait() && token.Error() != nil {
		loggerFatal(LOG_MQTT, "Error - No se pudo conectar al servidor: %v", token.Error())
	}
	// Suscribirse al topico "#" (VER SI SE SUSCRIBE A OTRO TOPICO)
	token := clienteMQTT.Subscribe("#", 1, manejadorMQTT)
	if token.Wait() && token.Error() != nil {
		loggerFatal(LOG_MQTT, "Error - No se pudo suscribir al tópico: %v", token.Error())
	}

	loggerPrint(LOG_MQTT, "Servidor conectado y suscrito al tópico #")
}

// manejadorMQTT maneja los mensajes MQTT recibidos
func manejadorMQTT(cliente MQTT.Client, mensajeMQTT MQTT.Message) {
	if strings.HasPrefix(mensajeMQTT.Topic(), "$SYS/") {
		return // Ignora mensajes del sistema
	}

	topicoMQTT, err := normalizarYValidarTopico(mensajeMQTT.Topic(), false)
	if err != nil {
		loggerPrint(LOG_MQTT, "Error - Tópico inválido: %v", mensajeMQTT.Topic())
		return
	}

	var mensaje Mensaje
	err = json.Unmarshal(mensajeMQTT.Payload(), &mensaje)
	if err != nil {
		loggerPrint(LOG_MQTT, "Error - No se pudo procesar el cuerpo: %v", err)
		return
	}
	if mensaje.Topico == "" {
		loggerPrint(LOG_MQTT, "Error - Mensaje sin tópico en body")
		return
	}
	mensajeTopico, err := normalizarYValidarTopico(mensaje.Topico, false)
	if err != nil {
		loggerPrint(LOG_MQTT, "Error - Tópico en body inválido: %v", mensaje.Topico)
		return
	}
	if err := validarQoS(mensaje); err != nil {
		loggerPrint(LOG_MQTT, "Error - QoS inválido: %v", err)
		return
	}
	if err := validarTamanoPayload(mensaje); err != nil {
		loggerPrint(LOG_MQTT, "Error - Payload demasiado grande: %v", err)
		return
	}
	if mensajeTopico != topicoMQTT {
		loggerPrint(LOG_MQTT, "Error - Tópico MQTT y body no coinciden: %s != %s", topicoMQTT, mensajeTopico)
		return
	}
	mensaje.Topico = mensajeTopico
	asignarOrigenSiVacio(&mensaje)
	loggerPrint(LOG_MQTT, "Mensaje recibido - Tópico: %s, QoS: %d, MensajeID: %s", mensaje.Topico, mensaje.QoS, mensaje.MensajeID)

	// enviar publicaciones a los otros protocolos (si el mensaje es original)
	if mensaje.Original {
		mensaje.Original = false
		go enviarCoAP(LOG_MQTT, mensaje)
		go enviarHTTP(LOG_MQTT, mensaje)
		go reenviarUpstream(mensaje)
	}
}
