package servidor

import (
	"fmt"
	"io"
	"log"
	"testing"
)

func init() {
	log.SetOutput(io.Discard)
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
