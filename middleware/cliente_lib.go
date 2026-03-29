package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Constante compartida: tamaño máximo de payload (64KB)
const TamanoMaximoPayload = 65536

var errQoSInvalido = errors.New("qos invalido")
var errPayloadMuyGrande = errors.New("payload demasiado grande")

type CallbackFunc func(topico string, payload []byte)

type Cliente interface {
	Desconectar()
	Publicar(topico string, mensaje interface{}, opciones ...PublicarOpcion)
	Suscribir(topico string, manejador CallbackFunc)
	Desuscribir(topico string)
}

type PublicarOpcion func(*Mensaje) error

func ConQoS(qos int) PublicarOpcion {
	return func(m *Mensaje) error {
		m.QoS = qos
		return nil
	}
}

// almacena si el mensaje es original o replica
type Mensaje struct {
	Original  bool   `json:"original"`
	Topico    string `json:"topico"`
	Payload   []byte `json:"payload"`
	Interno   bool   `json:"interno"`
	QoS       int    `json:"qos,omitempty"`
	MensajeID string `json:"mensajeId,omitempty"`
}

// Construir crea un mensaje a partir de un payload y opciones
func Construir(topico string, payload interface{}, opciones ...PublicarOpcion) (Mensaje, error) {
	mensaje := Mensaje{Original: true, Topico: topico, Interno: false}

	switch v := payload.(type) {
	case Mensaje:
		mensaje = v
	case *Mensaje:
		if v != nil {
			mensaje = *v
		}
	default:
		var data []byte
		switch val := payload.(type) {
		case string:
			data = []byte(val)
		case []byte:
			data = val
		case int, int32, int64, float32, float64:
			data = []byte(fmt.Sprintf("%v", val))
		default:
			var err error
			data, err = json.Marshal(val)
			if err != nil {
				return Mensaje{}, err
			}
		}

		// Validar tamaño del payload (máximo 64KB para evitar OOM)
		if len(data) > TamanoMaximoPayload {
			return Mensaje{}, fmt.Errorf("%w: %d bytes (máximo %d)", errPayloadMuyGrande, len(data), TamanoMaximoPayload)
		}

		mensaje.Payload = data
	}

	if mensaje.Topico == "" {
		mensaje.Topico = topico
	}
	if mensaje.Topico != topico {
		return Mensaje{}, errors.New("el tópico del mensaje no coincide con el parámetro topico")
	}

	for _, op := range opciones {
		if err := op(&mensaje); err != nil {
			return Mensaje{}, err
		}
	}

	if mensaje.QoS != 0 && mensaje.QoS != 1 {
		return Mensaje{}, errQoSInvalido
	}
	if mensaje.QoS == 1 && mensaje.MensajeID == "" {
		mensaje.MensajeID = generarMensajeID()
	}

	return mensaje, nil
}

func generarMensajeID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err == nil {
		return hex.EncodeToString(buf)
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
