package servidor

import (
	"encoding/json"

	"github.com/sensorwave-dev/sensorwave/middleware"
)

func enviarCoAP(LOG string, payload Mensaje) {
	if middleware.EsTopicoControl(payload.Topico) {
		return
	}

	if err := validarQoS(payload); err != nil {
		loggerPrint(LOG, "Error - QoS inválido para CoAP: %v", err)
		return
	}

	publicacion, err := middleware.NormalizarYValidarTopico(payload.Topico, false)
	if err != nil {
		loggerPrint(LOG, "Error - Tópico inválido para CoAP: %v", payload.Topico)
		return
	}

	// notifico a todos los observadores
	mutexCoAP.Lock()
	for patron, conexiones := range observadores {
		if !coincidePatron(publicacion, patron) {
			continue
		}
		for _, o := range conexiones {
			if err := enviarRespuestaConTipo(o.conexion, o.token, payload, valor.Add(1), tipoCoAPPorQoS(payload.QoS)); err != nil {
				loggerPrint(LOG, "Error - No se pudo enviar a observador CoAP - Tópico: %s, Error: %v", payload.Topico, err)
			}
		}
	}
	mutexCoAP.Unlock()
}

func enviarHTTP(LOG string, payload Mensaje) {
	if middleware.EsTopicoControl(payload.Topico) {
		return
	}

	if err := validarQoS(payload); err != nil {
		loggerPrint(LOG, "Error - QoS inválido para HTTP: %v", err)
		return
	}

	publicacion, err := middleware.NormalizarYValidarTopico(payload.Topico, false)
	if err != nil {
		loggerPrint(LOG, "Error - Tópico inválido para HTTP: %v", payload.Topico)
		return
	}

	// Enviar el mensaje a todos los clientes suscritos al tópico
	mutexHTTP.Lock()
	for patron, clientes := range clientesPorTopico {
		if !coincidePatron(publicacion, patron) {
			continue
		}
		for _, cliente := range clientes {
			if payload.QoS == 1 {
				enviarHTTPQoS1(LOG, cliente, payload)
				continue
			}
			go func(c *Cliente) {
				select {
				case c.Canal <- payload:
					// Éxito silencioso para evitar spam de logs
				default:
					loggerPrint(LOG, "Error - No se pudo enviar mensaje - ClienteID: %s, Tópico: %s, Razón: canal bloqueado", c.ID, payload.Topico)
				}
			}(cliente)
		}
	}
	mutexHTTP.Unlock()
}

func enviarMQTT(LOG string, payload Mensaje) {
	mensajeBytes, err := json.Marshal(payload)
	if err != nil {
		loggerPrint(LOG, "Error - No se pudo serializar mensaje: %v", err)
		return
	}
	// Publicación in-process al broker embebido. Llega directo a los
	// suscriptores MQTT sin roundtrip TCP. El hook OnPublish detecta el
	// cliente inline y omite el fanout (sin bucle/rebotado).
	if brokerMQTT == nil {
		loggerPrint(LOG, "Error - Broker MQTT no inicializado")
		return
	}
	if err := brokerMQTT.Publish(payload.Topico, mensajeBytes, false, byte(payload.QoS)); err != nil {
		loggerPrint(LOG, "Error - No se pudo publicar mensaje: %v", err)
		return
	}
}
