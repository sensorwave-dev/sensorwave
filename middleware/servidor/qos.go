package servidor

import "errors"

var (
	errQoSInvalido        = errors.New("qos invalido")
	errMessageIDRequerido = errors.New("messageId requerido")
)

func validarQoS(m Mensaje) error {
	switch m.QoS {
	case 0:
		return nil
	case 1:
		if m.MessageID == "" {
			return errMessageIDRequerido
		}
		return nil
	default:
		return errQoSInvalido
	}
}
