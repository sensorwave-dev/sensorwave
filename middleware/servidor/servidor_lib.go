package servidor

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"sync"

	"github.com/sensorwave-dev/sensorwave/middleware"
)

// Mensaje es un alias al tipo del paquete middleware para unificar la estructura
type Mensaje = middleware.Mensaje

var (
	idInstanciaServidor string
	idInstanciaOnce     sync.Once

	upstreamMu      sync.RWMutex
	clienteUpstream middleware.Cliente
)

func generarIDInstancia() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err == nil {
		return hex.EncodeToString(buf)
	}
	return "sw-unknown"
}

func obtenerIDInstancia() string {
	idInstanciaOnce.Do(func() {
		idInstanciaServidor = generarIDInstancia()
	})
	return idInstanciaServidor
}

// ConfigurarUpstream establece un cliente remoto opcional para federación.
// Si cliente es nil, deshabilita el reenvío upstream.
func ConfigurarUpstream(cliente middleware.Cliente) {
	upstreamMu.Lock()
	clienteUpstream = cliente
	upstreamMu.Unlock()

	if cliente == nil {
		loggerPrint("UPSTREAM", "Upstream deshabilitado")
		return
	}

	loggerPrint("UPSTREAM", "Upstream configurado - ID instancia: %s", obtenerIDInstancia())
}

func asignarOrigenSiVacio(m *Mensaje) {
	if m.Origen != "" {
		return
	}
	m.Origen = obtenerIDInstancia()
}

func reenviarUpstream(m Mensaje) {
	upstreamMu.RLock()
	cliente := clienteUpstream
	upstreamMu.RUnlock()

	if cliente == nil {
		return
	}

	idLocal := obtenerIDInstancia()
	// Si el mensaje vino de otra instancia, no reenviar para evitar bucles.
	if m.Origen != "" && m.Origen != idLocal {
		return
	}

	if m.Topico == "" {
		return
	}

	m.Original = true
	cliente.Publicar(m.Topico, m)
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
