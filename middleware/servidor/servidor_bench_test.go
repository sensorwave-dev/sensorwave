package servidor

import (
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"testing"

	"github.com/sensorwave-dev/sensorwave/middleware"
)

type upstreamMock struct {
	publicaciones atomic.Int64
}

func init() {
	log.SetOutput(io.Discard)
}

func (m *upstreamMock) Desconectar() {}

func (m *upstreamMock) Publicar(topico string, mensaje interface{}, opciones ...middleware.PublicarOpcion) error {
	m.publicaciones.Add(1)
	return nil
}

func (m *upstreamMock) Suscribir(topico string, manejador middleware.CallbackFunc) error {
	return nil
}

func (m *upstreamMock) Desuscribir(topico string) error {
	return nil
}

func prepararClientesHTTP(cantidad int, patron string) {
	mutexHTTP.Lock()
	defer mutexHTTP.Unlock()

	clientesPorTopico = map[string]map[string]*Cliente{}
	clientesPorID = map[string]*Cliente{}

	clientesPorTopico[patron] = make(map[string]*Cliente, cantidad)
	for i := 0; i < cantidad; i++ {
		id := fmt.Sprintf("bench-%d", i)
		c := &Cliente{
			ID:    id,
			Canal: make(chan Mensaje, 1),
		}
		clientesPorTopico[patron][id] = c
		clientesPorID[id] = c
	}
}

func BenchmarkEnviarHTTPQoS0_1Cliente(b *testing.B) {
	prepararClientesHTTP(1, "sensores/+/temperatura")
	payload := Mensaje{Topico: "sensores/sala1/temperatura", Payload: []byte("25.1"), QoS: 0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enviarHTTP("BENCH", payload)
	}
}

func BenchmarkEnviarHTTPQoS0_100Clientes(b *testing.B) {
	prepararClientesHTTP(100, "sensores/+/temperatura")
	payload := Mensaje{Topico: "sensores/sala1/temperatura", Payload: []byte("25.1"), QoS: 0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enviarHTTP("BENCH", payload)
	}
}

func BenchmarkReenviarUpstream_SinUpstream(b *testing.B) {
	ConfigurarUpstream(nil)
	m := Mensaje{Topico: "sensores/temperatura", Payload: []byte("25")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reenviarUpstream(m)
	}
}

func BenchmarkReenviarUpstream_OrigenLocal(b *testing.B) {
	mock := &upstreamMock{}
	ConfigurarUpstream(mock)
	m := Mensaje{Topico: "sensores/temperatura", Payload: []byte("25"), Origen: obtenerIDInstancia()}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reenviarUpstream(m)
	}
}

func BenchmarkReenviarUpstream_OrigenRemoto(b *testing.B) {
	mock := &upstreamMock{}
	ConfigurarUpstream(mock)
	m := Mensaje{Topico: "sensores/temperatura", Payload: []byte("25"), Origen: "instancia-remota"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reenviarUpstream(m)
	}
}
