package servidor

import "errors"

var (
	errQoSInvalido        = errors.New("qos invalido")
	errMensajeIDRequerido = errors.New("mensajeId requerido")
)

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
