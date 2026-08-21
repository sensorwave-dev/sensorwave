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

// Tamaño máximo de un payload en bytes
const tamanoMaximoPayload = 65536

// validarQoS Valida el QoS del mensaje y el MensajeID para QoS 1
func validarQoS(m Mensaje) error {
	// QoS 0: No se garantiza la entrega del mensaje
	// QoS 1: Se garantiza la entrega del mensaje por lo menos una vez
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

// validarTamanoPayload Valida el tamaño del payload del mensaje
func validarTamanoPayload(m Mensaje) error {
	// Si el payload es mayor al tamaño máximo, se retorna un error
	if len(m.Payload) > tamanoMaximoPayload {
		return fmt.Errorf("%w: %d bytes (máximo %d)", errPayloadMuyGrande, len(m.Payload), tamanoMaximoPayload)
	}
	return nil
}
