package servidor

import (
	"io"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/sensorwave-dev/sensorwave/middleware/cliente_mqtt"
)

func init() {
	log.SetOutput(io.Discard)
}

// puertoLibre devuelve un puerto TCP libre en localhost.
func puertoLibre(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("no se pudo obtener puerto libre: %v", err)
	}
	addr := l.Addr().(*net.TCPAddr)
	l.Close()
	return strconv.Itoa(addr.Port)
}

// iniciarBrokerTest arranca el broker embebido en un puerto libre y registra
// su cierre en t.Cleanup.
func iniciarBrokerTest(t *testing.T) string {
	t.Helper()
	puerto := puertoLibre(t)
	IniciarMQTT(puerto)
	t.Cleanup(func() {
		if brokerMQTT != nil {
			_ = brokerMQTT.Close()
			brokerMQTT = nil
		}
	})
	// Dar tiempo al goroutine de Serve() a enlazar el listener.
	time.Sleep(100 * time.Millisecond)
	return puerto
}

// conectarClienteMQTT conecta un cliente implementado (cliente_mqtt) al broker
// embebido, con reintentos para no acoplar el test al timing del Accept.
func conectarClienteMQTT(t *testing.T, puerto, id string) *cliente_mqtt.ClienteMQTT {
	t.Helper()
	var ultimoErr error
	for i := 0; i < 20; i++ {
		cl, err := cliente_mqtt.Conectar("127.0.0.1", puerto)
		if err == nil {
			_ = cl
			return cl
		}
		ultimoErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no se pudo conectar cliente MQTT implementado: %v", ultimoErr)
	return nil
}

// esperarRecibido espera un mensaje en el canal o agota timeout.
func esperarRecibido(recibido <-chan string, timeout time.Duration) (string, bool) {
	select {
	case t := <-recibido:
		return t, true
	case <-time.After(timeout):
		return "", false
	}
}

// Publicar y Suscribir usan los clientes implementados (cliente_mqtt), que
// canonizan el tópico en ambos extremos. Así se ejercita el flujo real de
// SensorWave end-to-end.

// TestMQTT_PubSlashSubSinSlash verifica que publicar a /test llegue a un
// suscriptor suscrito a test (canonización del lado publicación).
func TestMQTT_PubSlashSubSinSlash(t *testing.T) {
	puerto := iniciarBrokerTest(t)
	pub := conectarClienteMQTT(t, puerto, "pub")
	defer pub.Desconectar()
	sub := conectarClienteMQTT(t, puerto, "sub")
	defer sub.Desconectar()

	recibido := make(chan string, 1)
	if err := sub.Suscribir("test", func(topico string, _ []byte) {
		select {
		case recibido <- topico:
		default:
		}
	}); err != nil {
		t.Fatalf("Suscribir(test): %v", err)
	}

	if err := pub.Publicar("/test", "hello"); err != nil {
		t.Fatalf("Publicar(/test): %v", err)
	}

	topic, ok := esperarRecibido(recibido, 2*time.Second)
	if !ok {
		t.Fatalf("suscriptor a 'test' no recibió publicación a '/test'")
	}
	if topic != "test" {
		t.Fatalf("tópico entregado = %q, esperado %q", topic, "test")
	}
}

// TestMQTT_PubSinSlashSubSlash verifica que publicar a test llegue a un
// suscriptor suscrito a /test (canonización del lado suscripción).
func TestMQTT_PubSinSlashSubSlash(t *testing.T) {
	puerto := iniciarBrokerTest(t)
	pub := conectarClienteMQTT(t, puerto, "pub")
	defer pub.Desconectar()
	sub := conectarClienteMQTT(t, puerto, "sub")
	defer sub.Desconectar()

	recibido := make(chan string, 1)
	if err := sub.Suscribir("/test", func(topico string, _ []byte) {
		select {
		case recibido <- topico:
		default:
		}
	}); err != nil {
		t.Fatalf("Suscribir(/test): %v", err)
	}

	if err := pub.Publicar("test", "hello"); err != nil {
		t.Fatalf("Publicar(test): %v", err)
	}

	topic, ok := esperarRecibido(recibido, 2*time.Second)
	if !ok {
		t.Fatalf("suscriptor a '/test' no recibió publicación a 'test' (canonización del filtro falló)")
	}
	if topic != "test" {
		t.Fatalf("tópico entregado = %q, esperado %q", topic, "test")
	}
}

// TestMQTT_CanonizacionSlashesDobles verifica que //sensores//temp//
// (suscripción y publicación con slashes redundantes) ruteen al mismo filtro.
func TestMQTT_CanonizacionSlashesDobles(t *testing.T) {
	puerto := iniciarBrokerTest(t)
	pub := conectarClienteMQTT(t, puerto, "pub")
	defer pub.Desconectar()
	sub := conectarClienteMQTT(t, puerto, "sub")
	defer sub.Desconectar()

	recibido := make(chan string, 1)
	if err := sub.Suscribir("//sensores//temp//", func(topico string, _ []byte) {
		select {
		case recibido <- topico:
		default:
		}
	}); err != nil {
		t.Fatalf("Suscribir(//sensores//temp//): %v", err)
	}

	if err := pub.Publicar("sensores/temp", "hello"); err != nil {
		t.Fatalf("Publicar(sensores/temp): %v", err)
	}

	topic, ok := esperarRecibido(recibido, 2*time.Second)
	if !ok {
		t.Fatalf("suscriptor canonizado no recibió publicación")
	}
	if topic != "sensores/temp" {
		t.Fatalf("tópico entregado = %q, esperado %q", topic, "sensores/temp")
	}
}

// TestMQTT_AmbosExtremosConSlash verifica que ambos extremos usando /test
// (suscripción y publicación) ruteen al mismo filtro canonizado "test".
func TestMQTT_AmbosExtremosConSlash(t *testing.T) {
	puerto := iniciarBrokerTest(t)
	pub := conectarClienteMQTT(t, puerto, "pub")
	defer pub.Desconectar()
	sub := conectarClienteMQTT(t, puerto, "sub")
	defer sub.Desconectar()

	recibido := make(chan string, 1)
	if err := sub.Suscribir("/test", func(topico string, _ []byte) {
		select {
		case recibido <- topico:
		default:
		}
	}); err != nil {
		t.Fatalf("Suscribir(/test): %v", err)
	}

	if err := pub.Publicar("/test", "hello"); err != nil {
		t.Fatalf("Publicar(/test): %v", err)
	}

	topic, ok := esperarRecibido(recibido, 2*time.Second)
	if !ok {
		t.Fatalf("suscriptor a '/test' no recibió publicación a '/test'")
	}
	if topic != "test" {
		t.Fatalf("tópico entregado = %q, esperado %q", topic, "test")
	}
}

// TestMQTT_UnsubscribeCanonizado verifica que un cliente suscrito a /test pueda
// desuscribirse con test (o viceversa) gracias a la canonización del cliente
// implementado en Desuscribir.
func TestMQTT_UnsubscribeCanonizado(t *testing.T) {
	puerto := iniciarBrokerTest(t)
	pub := conectarClienteMQTT(t, puerto, "pub")
	defer pub.Desconectar()
	sub := conectarClienteMQTT(t, puerto, "sub")
	defer sub.Desconectar()

	recibido := make(chan string, 4)
	if err := sub.Suscribir("/test", func(topico string, _ []byte) {
		select {
		case recibido <- topico:
		default:
		}
	}); err != nil {
		t.Fatalf("Suscribir(/test): %v", err)
	}

	// 1ra publicación a "test": debe llegar.
	if err := pub.Publicar("test", "hello"); err != nil {
		t.Fatalf("Publicar(test): %v", err)
	}
	if _, ok := esperarRecibido(recibido, 2*time.Second); !ok {
		t.Fatalf("no se recibió publicación 1 (canonización del filtro /test falló)")
	}

	// Desuscripción usando la forma sin slash (canonizada a `test` por el
	// cliente implementado). El broker elimina la suscripción "test".
	if err := sub.Desuscribir("test"); err != nil {
		t.Fatalf("Desuscribir(test): %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 2da publicación: NO debe llegar.
	if err := pub.Publicar("test", "hello"); err != nil {
		t.Fatalf("Publicar(test) #2: %v", err)
	}
	if topic, ok := esperarRecibido(recibido, 400*time.Millisecond); ok {
		t.Fatalf("tras Desuscribir canonizado se recibió mensaje no esperado con topic %q", topic)
	}
}

// TestMQTT_SuscripcionInvalidaRechazada verifica que el cliente implementado
// rechace filtros de suscripción inválidos (wildcard mal formado) antes de
// enviarlos al broker.
func TestMQTT_SuscripcionInvalidaRechazada(t *testing.T) {
	puerto := iniciarBrokerTest(t)
	sub := conectarClienteMQTT(t, puerto, "sub")
	defer sub.Desconectar()

	if err := sub.Suscribir("sensores/te+mp", func(string, []byte) {}); err == nil {
		t.Fatalf("Suscribir('sensores/te+mp') debió rechazarse en el cliente")
	}
}

// TestMQTT_PublicacionInvalidaRechazada verifica que el cliente implementado
// rechaze tópicos de publicación inválidos (# no final, wildcards).
func TestMQTT_PublicacionInvalidaRechazada(t *testing.T) {
	puerto := iniciarBrokerTest(t)
	pub := conectarClienteMQTT(t, puerto, "pub")
	defer pub.Desconectar()

	casos := []string{"sensores/#/temp", "sensores/+/temp", "/sensores/#"}
	for _, topico := range casos {
		if err := pub.Publicar(topico, "x"); err == nil {
			t.Fatalf("Publicar(%q) debió rechazarse en el cliente", topico)
		}
	}
}

// TestMQTT_SuscripcionControlRechazada verifica que el cliente implementado
// rechace suscribirse al plano de control swctl/...
func TestMQTT_SuscripcionControlRechazada(t *testing.T) {
	puerto := iniciarBrokerTest(t)
	sub := conectarClienteMQTT(t, puerto, "sub")
	defer sub.Desconectar()

	if err := sub.Suscribir("swctl/#", func(string, []byte) {}); err == nil {
		t.Fatalf("Suscribir('swctl/#') debió rechazarse en el cliente (plano de control)")
	}
	if err := sub.Suscribir("swctl/nodos/x/latido", func(string, []byte) {}); err == nil {
		t.Fatalf("Suscribir('swctl/...') debió rechazarse en el cliente (plano de control)")
	}
}