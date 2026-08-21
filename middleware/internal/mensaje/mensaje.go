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

const tamanoMaximoPayload = 65536

var errQoSInvalido = errors.New("qos invalido")
var errPayloadMuyGrande = errors.New("payload demasiado grande")

// Construir crea un mensaje a partir de un payload y opciones. Canoniza el
// tópico mediante middleware.NormalizarYValidarTopico (sin wildcards: un
// mensaje de publicación no puede contener + ni #). Si payload ya es un
// middleware.Mensaje con su propio Topico, también se canoniza y debe
// coincidir con el canon del parámetro topico.
func Construir(topico string, payload interface{}, opciones ...middleware.PublicarOpcion) (middleware.Mensaje, error) {
	canon, err := middleware.NormalizarYValidarTopico(topico, false)
	if err != nil {
		return middleware.Mensaje{}, err
	}

	mensaje := middleware.Mensaje{Original: true, Topico: canon, Interno: false}

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

		// Validar tamaño del payload (máximo 64KB para evitar OOM)
		if len(data) > tamanoMaximoPayload {
			return middleware.Mensaje{}, fmt.Errorf("%w: %d bytes (máximo %d)", errPayloadMuyGrande, len(data), tamanoMaximoPayload)
		}

		mensaje.Payload = data
	}

	// Canonizar el Topico que vino dentro del mensaje (si el payload era un
	// middleware.Mensaje ya poblado). Debe coincidir con el canon del
	// parámetro topico tras canonización.
	if mensaje.Topico != "" {
		canonMsg, err := middleware.NormalizarYValidarTopico(mensaje.Topico, false)
		if err != nil {
			return middleware.Mensaje{}, err
		}
		mensaje.Topico = canonMsg
	}
	if mensaje.Topico == "" {
		mensaje.Topico = canon
	}
	if mensaje.Topico != canon {
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
