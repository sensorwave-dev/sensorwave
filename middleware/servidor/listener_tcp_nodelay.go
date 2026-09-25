package servidor

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/mochi-mqtt/server/v2/listeners"
	"log/slog"
)

// tcpNoDelayListener es un listener TCP que desactiva el algoritmo de Nagle
// (TCP_NODELAY=true) en cada conexión aceptada. Esto reduce la latencia de
// paquetes pequeños (eco PUBLISH QoS0) que el broker envía al suscriptor.
type tcpNoDelayListener struct {
	sync.RWMutex
	id      string
	address string
	listen  net.Listener
	log     *slog.Logger
	end     uint32
}

// newTCPNoDelay crea un listener TCP con Nagle desactivado.
func newTCPNoDelay(id, address string) *tcpNoDelayListener {
	return &tcpNoDelayListener{
		id:      id,
		address: address,
	}
}

func (l *tcpNoDelayListener) ID() string       { return l.id }
func (l *tcpNoDelayListener) Address() string  { return l.address }
func (l *tcpNoDelayListener) Protocol() string { return listeners.TypeTCP }

func (l *tcpNoDelayListener) Init(log *slog.Logger) error {
	l.log = log
	var err error
	l.listen, err = net.Listen("tcp", l.address)
	return err
}

func (l *tcpNoDelayListener) Serve(establish listeners.EstablishFn) {
	for {
		if atomic.LoadUint32(&l.end) == 1 {
			return
		}
		conn, err := l.listen.Accept()
		if err != nil {
			return
		}
		// Desactivar Nagle en cada conexión aceptada.
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
		}
		if atomic.LoadUint32(&l.end) == 0 {
			go func() {
				err = establish(l.id, conn)
				if err != nil {
					l.log.Warn("", "error", err)
				}
			}()
		}
	}
}

func (l *tcpNoDelayListener) Close(closeClients listeners.CloseFn) {
	l.Lock()
	defer l.Unlock()
	if atomic.CompareAndSwapUint32(&l.end, 0, 1) {
		closeClients(l.id)
	}
	if l.listen != nil {
		_ = l.listen.Close()
	}
}
