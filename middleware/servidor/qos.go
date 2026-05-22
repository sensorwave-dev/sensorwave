package servidor

import (
	"errors"
	"fmt"
)

var (
	errQoSInvalido        = errors.New("qos invalido")
	errMensajeIDRequerido = errors.New("mensajeId requerido")
	errPayloadMuyGrande   = errors.New("payload demasiado grande")
)

const tamanoMaximoPayload = 65536

func validarQoS(m Mensaje) error {
	switch m.QoS {
	case 0:
		return nil
	case 1:
		if m.MensajeID == "" {
			return errMensajeIDRequerido
		}
		return nil
	default:
		return errQoSInvalido
	}
}

func validarTamanoPayload(m Mensaje) error {
	if len(m.Payload) > tamanoMaximoPayload {
		return fmt.Errorf("%w: %d bytes (máximo %d)", errPayloadMuyGrande, len(m.Payload), tamanoMaximoPayload)
	}
	return nil
}
