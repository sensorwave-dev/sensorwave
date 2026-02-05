package servidor

import (
	"log"
)

// almacena si el mensaje es original o replica
type Mensaje struct {
	Original  bool   `json:"original"`
	Topico    string `json:"topico"`
	Payload   []byte `json:"payload"`
	Interno   bool   `json:"interno"`
	QoS       int    `json:"qos,omitempty"`
	MessageID string `json:"messageId,omitempty"`
}

// loggerFatal imprime un mensaje en la consola de log y termina
func loggerFatal(logger string, mensaje string, args ...any) {
	mensaje = "[" + logger + "] " + mensaje
	log.Fatalf(mensaje, args...)
}

// loggerPrint imprime un mensaje en la consola de log
func loggerPrint(logger string, mensaje string, args ...any) {
	mensaje = "[" + logger + "] " + mensaje
	log.Printf(mensaje, args...)
}
