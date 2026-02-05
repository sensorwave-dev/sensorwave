package servidor

import "encoding/json"

func enviarCoAP(LOG string, payload Mensaje) {
	// publica el mensaje en el tópico
	loggerPrint(LOG, ">> Publicando mensaje en el tópico CoAP "+payload.Topico)

	// notifico a todos los observadores
	mutexCoAP.Lock()
	for patron, conexiones := range observadores {
		if !coincidePatron(payload.Topico, patron) {
			continue
		}
		for _, o := range conexiones {
			loggerPrint(LOG, "Enviando mensaje a observador CoAP: %v", o)
			enviarRespuesta(o.conexion, o.token, payload, valor.Add(1))
		}
	}
	mutexCoAP.Unlock()
}

func enviarHTTP(LOG string, payload Mensaje) {
	// publica el mensaje en el tópico
	loggerPrint(LOG, ">> Publicando mensaje en el tópico HTTP "+payload.Topico)

	// no se serializa el mensaje a JSON
	// Publicar un mensaje en el tópico
	// Enviar el mensaje a todos los clientes suscritos al tópico
	mutexHTTP.Lock()
	clienteEnviado := false
	for patron, clientes := range clientesPorTopico {
		if !coincidePatron(payload.Topico, patron) {
			continue
		}
		for _, cliente := range clientes {
			clienteEnviado = true
			go func(c *Cliente) {
				select {
				case c.Channel <- string(payload.Payload):
					loggerPrint(LOG, "Mensaje enviado al tópico HTTP"+payload.Topico)
				default:
					loggerPrint(LOG, "No se pudo enviar el mensaje al cliente en el tópico HTTP"+payload.Topico+" (canal bloqueado)")
				}
			}(cliente)
		}
	}
	if !clienteEnviado {
		loggerPrint(LOG, "No hay clientes suscritos al tópico HTTP "+payload.Topico)
	}
	mutexHTTP.Unlock()
}

func enviarMQTT(LOG string, payload Mensaje) {
	// publica el mensaje en el tópico
	loggerPrint(LOG, ">> Publicando mensaje en el tópico MQTT "+payload.Topico)

	// Serializar el mensaje a JSON
	mensajeBytes, err := json.Marshal(payload)
	if err != nil {
		loggerPrint(LOG, "Error al serializar el mensaje: %v", err)
	}
	// Publicar un mensaje en el tópico
	if token := clienteMQTT.Publish(payload.Topico, 0, false, mensajeBytes); token.Wait() && token.Error() != nil {
		loggerPrint(LOG, "Error: %v", token.Error())
	}
}
