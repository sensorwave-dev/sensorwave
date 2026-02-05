package servidor

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"

	coap "github.com/plgd-dev/go-coap/v3"
	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/mux"
)

const LOG_COAP = "COAP"

func obtenerTopicoCoAP(r *mux.Message) (string, error) {
	queries, err := r.Options().Queries()
	if err != nil {
		return "", err
	}
	for _, q := range queries {
		partes := strings.SplitN(q, "=", 2)
		if len(partes) == 0 {
			continue
		}
		if partes[0] != "topico" {
			continue
		}
		if len(partes) == 2 {
			return partes[1], nil
		}
		return "", nil
	}
	return "", nil
}

// datos de las conexiones de los observadores
type Conexion struct {
	conexion mux.Conn
	context  context.Context
	token    []byte
}

// Almacena observadores por ruta
var (
	valor        atomic.Int64                  // valor de observación
	observadores = make(map[string][]Conexion) // conexiones CoAP
	mutexCoAP    sync.Mutex                    // Mutex para proteger el acceso a `clientesPorTopico`
)

// Iniciar el servidor CoAP
func IniciarCoAP(puerto string) {
	r := mux.NewRouter()
	// Manejador para /sensorwave
	r.Handle("/sensorwave", mux.HandlerFunc(manejadorCoAP))
	r.DefaultHandle(mux.HandlerFunc(func(w mux.ResponseWriter, r *mux.Message) {
		_ = w.SetResponse(codes.NotFound, message.TextPlain, bytes.NewReader([]byte("Ruta no encontrada")))
	}))

	// Iniciar el servidor en el puerto especificado
	loggerPrint(LOG_COAP, "Iniciando servidor CoAP en :"+puerto)
	err := coap.ListenAndServe("udp", ":"+puerto, r)
	if err != nil {
		loggerFatal(LOG_COAP, "Error al iniciar el servidor: %v", err)
	}
}

// handleAll maneja todas las solicitudes CoAP, independientemente de la ruta
func manejadorCoAP(w mux.ResponseWriter, r *mux.Message) {
	metodo := r.Code()
	loggerPrint(LOG_COAP, "Solicitud recibida: Método %v", metodo)

	topico, err := obtenerTopicoCoAP(r)
	if err != nil {
		loggerPrint(LOG_COAP, "Error al obtener el query: %v", err)
		_ = w.SetResponse(codes.BadRequest, message.TextPlain, bytes.NewReader([]byte("Error en query")))
		return
	}
	if topico == "" {
		_ = w.SetResponse(codes.BadRequest, message.TextPlain, bytes.NewReader([]byte("Falta el parámetro 'topico'")))
		return
	}

	// obtengo si tiene observe
	obs, err := r.Options().Observe()

	// Responder según el método
	switch {
	// suscribirse
	case metodo == codes.GET && err == nil && obs == 0:
		manejarSuscripcionCoAP(w, r, topico)
	// desuscribirse
	case metodo == codes.GET && err == nil && obs != 0:
		eliminarSuscripcionCoAP(w, r, topico)
	// publicar
	case metodo == codes.POST:
		// Obtener la carga útil de la solicitud, si hay alguna
		var mensaje Mensaje
		cuerpo, err := r.Message.ReadBody()
		if err != nil {
			loggerPrint(LOG_COAP, "Error al procesar el cuerpo de la solicitud: "+err.Error())
			return
		}

		err = json.Unmarshal(cuerpo, &mensaje)
		if err != nil {
			loggerPrint(LOG_COAP, "Error al convertir el cuerpo de la solicitud: "+err.Error())
			_ = w.SetResponse(codes.BadRequest, message.TextPlain, bytes.NewReader([]byte("Cuerpo invalido")))
			return
		}
		if mensaje.Topico == "" {
			_ = w.SetResponse(codes.BadRequest, message.TextPlain, bytes.NewReader([]byte("Falta el parámetro 'topico'")))
			return
		}
		if mensaje.Topico != topico {
			_ = w.SetResponse(codes.BadRequest, message.TextPlain, bytes.NewReader([]byte("El tópico del query y del cuerpo no coinciden")))
			return
		}
		loggerPrint(LOG_COAP, "Cuerpo convertido a Mensaje: %+v", mensaje)
		manejarPublicacionCoAP(w, r, topico, mensaje)
	default:
		loggerPrint(LOG_COAP, "Método no soportado: %v", metodo)
		err := w.SetResponse(codes.MethodNotAllowed, message.TextPlain, bytes.NewReader([]byte("Método no soportado")))
		if err != nil {
			loggerPrint(LOG_COAP, "Error al enviar respuesta: %v", err)
		}
	}
}

// manejarSuscripcionCoAP maneja las solicitudes GET con observe
func manejarSuscripcionCoAP(w mux.ResponseWriter, r *mux.Message, topico string) {

	// agrego observadores
	loggerPrint(LOG_COAP, "Agregando observador")
	mutexCoAP.Lock()
	datosConexion := Conexion{w.Conn(), r.Context(), r.Token()}
	observadores[topico] = append(observadores[topico], datosConexion)
	loggerPrint(LOG_COAP, "Agregando Observador en topico %v", topico)
	mutexCoAP.Unlock()

	// enviar respuesta
	err := enviarRespuesta(w.Conn(), r.Token(), Mensaje{Interno: true}, valor.Add(1))
	if err != nil {
		loggerPrint(LOG_COAP, "Error en transmitir: %v", err)
	}
}

// manejarPublicacionCoAP envía una publicación a los observadores de una ruta
func manejarPublicacionCoAP(w mux.ResponseWriter, r *mux.Message, topico string, payload Mensaje) {

	err := w.SetResponse(codes.Created, message.TextPlain, nil)
	if err != nil {
		loggerPrint(LOG_COAP, "Error al enviar respuesta: %v", err)
	}

	// enviar publicaciones a los protocolos
	if payload.Original {
		payload.Original = false
		go enviarCoAP(LOG_COAP, payload)
		go enviarHTTP(LOG_COAP, payload)
		go enviarMQTT(LOG_COAP, payload)
	}
}

func eliminarSuscripcionCoAP(w mux.ResponseWriter, r *mux.Message, ruta string) {
	err := enviarRespuesta(w.Conn(), r.Token(), Mensaje{Interno: true}, -1)
	if err != nil {
		loggerPrint(LOG_COAP, "Error al enviar respuesta: %v", err)
	}
	// quito el observador
	mutexCoAP.Lock()
	for i, o := range observadores[ruta] {
		if bytes.Equal(o.token, r.Token()) {
			observadores[ruta] = append(observadores[ruta][:i], observadores[ruta][i+1:]...)
			break
		}
	}
	// Si no hay más observadores en la ruta, eliminar la ruta
	if len(observadores[ruta]) == 0 {
		delete(observadores, ruta)
	}
	mutexCoAP.Unlock()
}

func enviarRespuesta(cc mux.Conn, token []byte, mensaje Mensaje, obs int64) error {
	m := cc.AcquireMessage(cc.Context())
	defer cc.ReleaseMessage(m)
	m.SetCode(codes.Content)
	m.SetToken(token)
	mensajeBytes, err := json.Marshal(mensaje)
	if err != nil {
		loggerPrint(LOG_COAP, "Error al convertir el mensaje a []byte: %v", err)
		return err
	}
	m.SetBody(bytes.NewReader(mensajeBytes))
	m.SetContentFormat(message.TextPlain)
	if obs >= 0 {
		m.SetObserve(uint32(obs))
	}
	return cc.WriteMessage(m)
}
