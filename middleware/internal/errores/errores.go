// Package errores define los errores categorizados del middleware.
//
// Los errores sentinelas (ErrConexion, ErrPublicacion, ErrACK, ErrSuscripcion,
// ErrDesuscripcion) permiten clasificar fallos mediante errors.Is, mientras que
// ErrorACK transporta datos (MensajeID) accesibles mediante errors.As.
//
// Uso:
//
//	if errors.Is(err, errores.ErrPublicacion) { ... }
//	var ackErr *errores.ErrorACK
//	if errors.As(err, &ackErr) { id := ackErr.MensajeID }
package errores

import (
	"errors"
	"fmt"
)

// Errores sentinelas para categorizar fallos de cliente.
var (
	ErrConexion      = errors.New("error de conexión")
	ErrPublicacion   = errors.New("error de publicación")
	ErrACK           = errors.New("error de ACK")
	ErrSuscripcion   = errors.New("error de suscripción")
	ErrDesuscripcion = errors.New("error de desuscripción")
)

// ErrorACK representa el agotamiento de reintentos esperando el ACK de un
// mensaje QoS1. Transporta el MensajeID y se categoriza como ErrACK vía
// Unwrap, de modo que errors.Is(err, ErrACK) sea verdadero.
type ErrorACK struct {
	MensajeID string
}

// Error devuelve la descripción del fallo de ACK.
func (e *ErrorACK) Error() string {
	return fmt.Sprintf("no se recibió ACK para mensajeId %s", e.MensajeID)
}

// Unwrap permite que errors.Is(err, ErrACK) sea verdadero.
func (e *ErrorACK) Unwrap() error {
	return ErrACK
}

// Es es un helper sobre errors.Is para comparar un error contra un target.
func Es(err, target error) bool {
	return errors.Is(err, target)
}
