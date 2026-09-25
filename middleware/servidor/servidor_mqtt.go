package servidor

import (
	"encoding/json"
	"strings"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/sensorwave-dev/sensorwave/middleware"
)

const LOG_MQTT = "MQTT"

// brokerMQTT es el broker MQTT embebido (mochi-mqtt). Reemplaza la dependencia
// de un broker externo (mosquitto, etc.). El middleware actúa como broker y a la
// vez popula el fanout a los otros protocolos desde un hook OnPublish en proceso,
// eliminando el roundtrip TCP y el hack de mensaje rebotado.
var brokerMQTT *mochi.Server

// hookMQTTOptions contiene la configuración del hook de SensorWave.
type hookMQTTOptions struct{}

// hookMQTT implementa el hook OnPublish del broker embebido.
// Se invoca en proceso por cada PUBLISH de un cliente MQTT externo. Si el
// publicante es el cliente inline del propio middleware (p.ej. cuando el
// middleware reenvía a suscriptores MQTT un mensaje que ingresó por HTTP/CoAP),
// el hook se omite: el broker entregará a los suscriptores MQTT sin re-fanout.
type hookMQTT struct {
	mochi.HookBase
}

func (h *hookMQTT) ID() string { return "sensorwave-mqtt" }

func (h *hookMQTT) Provides(b byte) bool {
	return b == mochi.OnPublish || b == mochi.OnSubscribe || b == mochi.OnUnsubscribe
}

// OnPublish maneja los PUBLISHes entrantes de clientes externos.
// Retorna ErrRejectPacket si el mensaje es inválido (no se envía PUBACK de éxito),
// o nil tras validar y encolar el fanout a los otros protocolos. De este modo el
// PUBACK al publicante significa "el middleware tomó responsabilidad del mensaje",
// semántica QoS1 coherente con HTTP (POST ack) y CoAP (ACK POST).
func (h *hookMQTT) OnPublish(cl *mochi.Client, pk packets.Packet) (packets.Packet, error) {
	// El cliente inline es el propio middleware publicando a suscriptores MQTT;
	// el broker entrega a subscribers automáticamente, no hay fanout que hacer.
	if cl.Net.Inline {
		return pk, nil
	}

	if strings.HasPrefix(pk.TopicName, "$SYS/") {
		return pk, nil
	}

	topicoMQTT, err := middleware.NormalizarYValidarTopico(pk.TopicName, false)
	if err != nil {
		loggerPrint(LOG_MQTT, "Error - Tópico inválido: %v", pk.TopicName)
		return pk, packets.ErrRejectPacket
	}

	var mensaje Mensaje
	if err := json.Unmarshal(pk.Payload, &mensaje); err != nil {
		loggerPrint(LOG_MQTT, "Error - No se pudo procesar el cuerpo: %v", err)
		return pk, packets.ErrRejectPacket
	}
	if mensaje.Topico == "" {
		loggerPrint(LOG_MQTT, "Error - Mensaje sin tópico en body")
		return pk, packets.ErrRejectPacket
	}
	
	mensajeTopico, err := middleware.NormalizarYValidarTopico(mensaje.Topico, false)
	if err != nil {
		loggerPrint(LOG_MQTT, "Error - Tópico en body inválido: %v", mensaje.Topico)
		return pk, packets.ErrRejectPacket
	}
	if err := validarQoS(mensaje); err != nil {
		loggerPrint(LOG_MQTT, "Error - QoS inválido: %v", err)
		return pk, packets.ErrRejectPacket
	}
	if err := validarTamanoPayload(mensaje); err != nil {
		loggerPrint(LOG_MQTT, "Error - Payload demasiado grande: %v", err)
		return pk, packets.ErrRejectPacket
	}
	if mensajeTopico != topicoMQTT {
		loggerPrint(LOG_MQTT, "Error - Tópico MQTT y body no coincieren: %s != %s", topicoMQTT, mensajeTopico)
		return pk, packets.ErrRejectPacket
	}
	// Canonizamos el topic del paquete para que el broker rutee a suscriptores
	// usando la misma forma que usamos al normalizar filtros en OnSubscribe.
	// Sin esto, un publicador que use /test rutea por "/test" y un suscriptor
	// a "test" nunca recibe (mochi compara filtro vs TopicName crudos).
	pk.TopicName = topicoMQTT
	mensaje.Topico = mensajeTopico

	// En proceso: el broker entrega a suscriptores MQTT automáticamente al
	// retornar nil. Acá sólo disparamos el fanout hacia los otros protocolos.
	if mensaje.Original {
		mensaje.Original = false
		if middleware.EsTopicoControl(mensaje.Topico) {
			// Plano de control: no fanout a HTTP/CoAP.
			return pk, nil
		}
		go enviarCoAP(LOG_MQTT, mensaje)
		go enviarHTTP(LOG_MQTT, mensaje)
	}

	return pk, nil
}

// OnSubscribe canoniza cada filtro de suscripción antes de que el broker lo
// registre. Esto garantiza simetría: un cliente suscrito a /test y un publicador
// a test ruteen al mismo filtro interno "test". Los filtros inválidos o del
// plano de control (swctl/...) se reemplazan por "" para que mochi los rechace
// en el SUBACK con ErrTopicFilterInvalid. El cliente inline del middleware queda
// exento (sus tópicos ya vienen canonizados).
func (h *hookMQTT) OnSubscribe(cl *mochi.Client, pk packets.Packet) packets.Packet {
	if cl.Net.Inline {
		return pk
	}
	for i := range pk.Filters {
		filtro := pk.Filters[i].Filter
		if strings.HasPrefix(filtro, "$SYS/") {
			continue
		}
		canon, err := middleware.NormalizarYValidarTopico(filtro, true)
		if err != nil {
			loggerPrint(LOG_MQTT, "Error - Filtro de suscripción inválido: %s", filtro)
			pk.Filters[i].Filter = ""
			continue
		}
		if middleware.EsTopicoControl(canon) {
			loggerPrint(LOG_MQTT, "Error - Filtro de control no permitido en suscripción MQTT externa: %s", filtro)
			pk.Filters[i].Filter = ""
			continue
		}
		pk.Filters[i].Filter = canon
	}
	return pk
}

// OnUnsubscribe canoniza los filtros de UNSUBSCRIBE con el mismo criterio que
// OnSubscribe, de modo que un cliente que se suscribió vía /test pueda
// desuscribirse con test (o viceversa) y mochi encuentre la suscripción.
func (h *hookMQTT) OnUnsubscribe(cl *mochi.Client, pk packets.Packet) packets.Packet {
	if cl.Net.Inline {
		return pk
	}
	for i := range pk.Filters {
		filtro := pk.Filters[i].Filter
		if strings.HasPrefix(filtro, "$SYS/") {
			continue
		}
		canon, err := middleware.NormalizarYValidarTopico(filtro, true)
		if err != nil {
			continue
		}
		pk.Filters[i].Filter = canon
	}
	return pk
}

// IniciarMQTT arranca el broker MQTT embebido en el puerto indicado.
// A diferencia de la implementación anterior (cliente paho contra un broker
// externo), ahora el middleware es el broker: los dispositivos MQTT se conectan
// directamente a este proceso.
func IniciarMQTT(puerto string) {
	brokerMQTT = mochi.New(&mochi.Options{
		InlineClient: true, // habilita server.Publish/Subscribe para egress in-process.
	})

	// Por defecto mochi rechaza todas las conexiones; permitir todas.
	if err := brokerMQTT.AddHook(new(auth.AllowHook), nil); err != nil {
		loggerFatal(LOG_MQTT, "Error - No se pudo agregar hook de auth: %v", err)
	}

	// Hook de SensorWave: fanout a HTTP/CoAP + control de PUBACK.
	if err := brokerMQTT.AddHook(new(hookMQTT), hookMQTTOptions{}); err != nil {
		loggerFatal(LOG_MQTT, "Error - No se pudo agregar hook de publish: %v", err)
	}

	tcp := newTCPNoDelay("sensorwave-tcp", ":"+puerto)
	if err := brokerMQTT.AddListener(tcp); err != nil {
		loggerFatal(LOG_MQTT, "Error - No se pudo agregar listener TCP: %v", err)
	}

	loggerPrint(LOG_MQTT, "Servidor iniciado - Broker embebido en puerto: %s", puerto)

	go func() {
		if err := brokerMQTT.Serve(); err != nil {
			loggerFatal(LOG_MQTT, "Error - No se pudo iniciar el broker: %v", err)
		}
	}()
}