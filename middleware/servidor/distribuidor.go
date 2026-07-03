package servidor

import "encoding/json"

func enviarCoAP(LOG string, payload Mensaje) {
	if EsTopicoControl(payload.Topico) {
		return
	}

	loggerPrint(LOG, "Distribuyendo mensaje - Destino: CoAP, Tópico: %s, QoS: %d, MensajeID: %s", payload.Topico, payload.QoS, payload.MensajeID)

	if err := validarQoS(payload); err != nil {
		loggerPrint(LOG, "Error - QoS inválido para CoAP: %v", err)
		return
	}

	publicacion, err := normalizarYValidarTopico(payload.Topico, false)
	if err != nil {
		loggerPrint(LOG, "Error - Tópico inválido para CoAP: %v", payload.Topico)
		return
	}

	// notifico a todos los observadores
	mutexCoAP.Lock()
	totalEnviados := 0
	totalErrores := 0
	for patron, conexiones := range observadores {
		if !coincidePatron(publicacion, patron) {
			continue
		}
		for _, o := range conexiones {
			if err := enviarRespuestaConTipo(o.conexion, o.token, payload, valor.Add(1), tipoCoAPPorQoS(payload.QoS)); err != nil {
				loggerPrint(LOG, "Error - No se pudo enviar a observador CoAP - Tópico: %s, Error: %v", payload.Topico, err)
				totalErrores++
			} else {
				totalEnviados++
			}
		}
	}
	mutexCoAP.Unlock()
	if totalEnviados > 0 || totalErrores > 0 {
		loggerPrint(LOG, "Mensaje distribuido en CoAP - Tópico: %s, Enviados: %d, Errores: %d", payload.Topico, totalEnviados, totalErrores)
	} else {
		loggerPrint(LOG, "Mensaje no distribuido en CoAP - Tópico: %s (sin observadores)", payload.Topico)
	}
}

func enviarHTTP(LOG string, payload Mensaje) {
	if EsTopicoControl(payload.Topico) {
		return
	}

	loggerPrint(LOG, "Distribuyendo mensaje - Destino: HTTP, Tópico: %s, QoS: %d, MensajeID: %s", payload.Topico, payload.QoS, payload.MensajeID)

	if err := validarQoS(payload); err != nil {
		loggerPrint(LOG, "Error - QoS inválido para HTTP: %v", err)
		return
	}

	publicacion, err := normalizarYValidarTopico(payload.Topico, false)
	if err != nil {
		loggerPrint(LOG, "Error - Tópico inválido para HTTP: %v", payload.Topico)
		return
	}

	// Enviar el mensaje a todos los clientes suscritos al tópico
	mutexHTTP.Lock()
	totalEnviados := 0
	totalClientes := 0
	for patron, clientes := range clientesPorTopico {
		totalClientes += len(clientes)
		if !coincidePatron(publicacion, patron) {
			continue
		}
		for _, cliente := range clientes {
			totalEnviados++
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
	if totalEnviados > 0 {
		loggerPrint(LOG, "Mensaje distribuido en HTTP - Tópico: %s, Clientes: %d/%d", payload.Topico, totalEnviados, totalClientes)
	} else {
		loggerPrint(LOG, "Mensaje no distribuido en HTTP - Tópico: %s (sin clientes suscritos)", payload.Topico)
	}
}

func enviarMQTT(LOG string, payload Mensaje) {
	loggerPrint(LOG, "Distribuyendo mensaje - Destino: MQTT, Tópico: %s, QoS: %d, MensajeID: %s", payload.Topico, payload.QoS, payload.MensajeID)

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
	loggerPrint(LOG, "Mensaje distribuido en MQTT - Tópico: %s, QoS: %d", payload.Topico, payload.QoS)
}
