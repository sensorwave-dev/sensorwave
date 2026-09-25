package servidor

import (
	"log"

	"github.com/sensorwave-dev/sensorwave/middleware"
)

// Mensaje es un alias al tipo del paquete middleware para unificar la estructura
type Mensaje = middleware.Mensaje

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
