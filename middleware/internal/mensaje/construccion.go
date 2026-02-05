package mensaje

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sensorwave-dev/sensorwave/middleware"
)

var errQoSInvalido = errors.New("qos invalido")

func Construir(topico string, payload interface{}, opciones ...middleware.PublicarOpcion) (middleware.Mensaje, error) {
	mensaje := middleware.Mensaje{Original: true, Topico: topico, Interno: false}

	switch v := payload.(type) {
	case middleware.Mensaje:
		mensaje = v
	case *middleware.Mensaje:
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
				return middleware.Mensaje{}, err
			}
		}
		mensaje.Payload = data
	}

	if mensaje.Topico == "" {
		mensaje.Topico = topico
	}
	if mensaje.Topico != topico {
		return middleware.Mensaje{}, errors.New("el tópico del mensaje no coincide con el parámetro topico")
	}

	for _, op := range opciones {
		if err := op(&mensaje); err != nil {
			return middleware.Mensaje{}, err
		}
	}

	if mensaje.QoS != 0 && mensaje.QoS != 1 {
		return middleware.Mensaje{}, errQoSInvalido
	}
	if mensaje.QoS == 1 && mensaje.MessageID == "" {
		mensaje.MessageID = generarMessageID()
	}

	return mensaje, nil
}

func generarMessageID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err == nil {
		return hex.EncodeToString(buf)
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
