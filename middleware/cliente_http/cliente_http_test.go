package clientehttp

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sensorwave-dev/sensorwave/middleware"
	"github.com/sensorwave-dev/sensorwave/middleware/internal/errores"
)

// nuevoClienteHTTPTest crea un ClienteHTTP apuntando a un servidor httptest.
func nuevoClienteHTTPTest(t *testing.T, server *httptest.Server) *ClienteHTTP {
	t.Helper()
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatal(err)
	}
	c, err := Conectar(host, port)
	if err != nil {
		t.Fatalf("Conectar: %v", err)
	}
	return c
}

// --- Fallo de conexión (validación) ---

func TestConectar_HostOPuertoVacios_Error(t *testing.T) {
	if _, err := Conectar("", "8080"); err == nil {
		t.Error("esperaba error por host vacío")
	} else if !errores.Es(err, errores.ErrConexion) {
		t.Errorf("esperaba ErrConexion, obtuvo: %v", err)
	}
	if _, err := Conectar("localhost", ""); err == nil {
		t.Error("esperaba error por puerto vacío")
	} else if !errores.Es(err, errores.ErrConexion) {
		t.Errorf("esperaba ErrConexion, obtuvo: %v", err)
	}
}

// --- Fallo de publicación QoS0 ---

func TestPublicar_QoS0_EstadoNoOK_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := nuevoClienteHTTPTest(t, srv)
	defer c.Desconectar()

	if err := c.Publicar("topico/test", "payload"); err == nil {
		t.Fatal("esperaba error de publicación")
	} else if !errores.Es(err, errores.ErrPublicacion) {
		t.Errorf("esperaba ErrPublicacion, obtuvo: %v", err)
	}
}

// --- Fallo de publicación QoS0 por error de red ---

func TestPublicar_QoS0_ErrorRed_Error(t *testing.T) {
	// Servidor que se cierra inmediatamente: el POST falla a nivel de red.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	c := nuevoClienteHTTPTest(t, srv)
	srv.Close() // cerrar antes de publicar

	if err := c.Publicar("topico/test", "payload"); err == nil {
		t.Fatal("esperaba error de publicación por red")
	} else if !errores.Es(err, errores.ErrPublicacion) {
		t.Errorf("esperaba ErrPublicacion, obtuvo: %v", err)
	}
}

// --- Fallo de ACK (QoS1 sin ACK coincidente) ---
// Test lento: retransmite según qos.MaxRetransmisiones con backoff exponencial.

func TestPublicar_QoS1_SinACK_ErrorACK(t *testing.T) {
	if testing.Short() {
		t.Skip("test de retransmisión QoS1 lento")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Responder 200 con un mensajeId que no coincide con el enviado.
		_ = json.NewEncoder(w).Encode(map[string]string{"mensajeId": "otro"})
	}))
	defer srv.Close()
	c := nuevoClienteHTTPTest(t, srv)
	defer c.Desconectar()

	err := c.Publicar("topico/test", "payload", middleware.ConQoS(1))
	if err == nil {
		t.Fatal("esperaba error de ACK")
	}
	if !errores.Es(err, errores.ErrACK) {
		t.Errorf("esperaba ErrACK, obtuvo: %v", err)
	}
	var ackErr *errores.ErrorACK
	if !errors.As(err, &ackErr) {
		t.Fatalf("esperaba *ErrorACK, obtuvo: %T", err)
	}
	if ackErr.MensajeID == "" {
		t.Error("MensajeID vacío en ErrorACK")
	}
}

// --- Suscribir: handshake OK + Desuscribir ---

func TestSuscribir_HandshakeOK_Y_Desuscribir(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, _ := w.(http.Flusher)
		fmt.Fprintf(w, "data: {\"clienteID\":\"test-id\"}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		for i := 0; i < 5; i++ {
			p := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%d", i)))
			fmt.Fprintf(w, "data: {\"topico\":\"t\",\"qos\":0,\"payload\":\"%s\"}\n\n", p)
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(500 * time.Millisecond)
	}))
	defer srv.Close()
	c := nuevoClienteHTTPTest(t, srv)
	defer c.Desconectar()

	var got atomic.Int32
	if err := c.Suscribir("t", func(topico string, payload []byte) {
		got.Add(1)
	}); err != nil {
		t.Fatalf("Suscribir: %v", err)
	}

	if err := waitFor(int32Func(&got, func(v int32) bool { return v >= 1 }), time.Second); err != nil {
		t.Fatalf("no se recibieron mensajes SSE: %v", err)
	}

	antes := got.Load()
	if err := c.Desuscribir("t"); err != nil {
		t.Fatalf("Desuscribir: %v", err)
	}
	// Tras desuscribir, dar tiempo a que la goroutine se detenga y verificar
	// que no siguen llegando mensajes de forma sostenida.
	time.Sleep(200 * time.Millisecond)
	despues := got.Load()
	if despues > antes+1 {
		t.Errorf("se recibieron mensajes tras Desuscribir: antes=%d despues=%d", antes, despues)
	}
}

// --- Suscribir: handshake falla (EOF inmediato) ---

func TestSuscribir_HandshakeFalla_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "no hijack", http.StatusInternalServerError)
			return
		}
		conn, _, _ := hj.Hijack()
		conn.Close()
	}))
	defer srv.Close()
	c := nuevoClienteHTTPTest(t, srv)
	defer c.Desconectar()

	err := c.Suscribir("t", func(string, []byte) {})
	if err == nil {
		t.Fatal("esperaba error de suscripción")
	}
	if !errores.Es(err, errores.ErrSuscripcion) {
		t.Errorf("esperaba ErrSuscripcion, obtuvo: %v", err)
	}
}

// --- Suscribir: timeout de handshake (10s) ---

func TestSuscribir_HandshakeTimeout_Error(t *testing.T) {
	if testing.Short() {
		t.Skip("test de timeout de handshake (10s)")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush() // enviar headers para que Get retorne
		}
		<-r.Context().Done() // nunca escribe data; se libera al cerrar el cliente
	}))
	defer srv.Close()
	c := nuevoClienteHTTPTest(t, srv)
	defer c.Desconectar()

	start := time.Now()
	err := c.Suscribir("t", func(string, []byte) {})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("esperaba error de suscripción por timeout")
	}
	if !errores.Es(err, errores.ErrSuscripcion) {
		t.Errorf("esperaba ErrSuscripcion, obtuvo: %v", err)
	}
	if elapsed < handshakeTimeout-200*time.Millisecond {
		t.Errorf("timeout prematuro: %v < %v", elapsed, handshakeTimeout)
	}
}

// --- FallosACK: contador atómico cuando clienteID está vacío ---

func TestFallosACK_ClienteIDVacio(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, _ := w.(http.Flusher)
		// Handshake sin clienteID: la handshake OK pero clienteID queda vacío.
		fmt.Fprintf(w, "data: {\"hola\":\"mundo\"}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		// Mensaje QoS1: dispara enviarAck con clienteID vacío -> fallo contado.
		p := base64.StdEncoding.EncodeToString([]byte("x"))
		fmt.Fprintf(w, "data: {\"topico\":\"t\",\"qos\":1,\"mensajeId\":\"m1\",\"payload\":\"%s\"}\n\n", p)
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(500 * time.Millisecond)
	}))
	defer srv.Close()
	c := nuevoClienteHTTPTest(t, srv)
	defer c.Desconectar()

	if err := c.Suscribir("t", func(string, []byte) {}); err != nil {
		t.Fatalf("Suscribir: %v", err)
	}
	if err := waitFor(int64Func(c.FallosACK, func(v int64) bool { return v >= 1 }), time.Second); err != nil {
		t.Fatalf("ackFallos no contó fallos: %v", err)
	}
	_ = c.Desuscribir("t")
}

// --- OnError: se invoca al agotar reconexiones SSE ---
// Test lento: agota maxIntentosReconexion con backoff exponencial.

func TestOnError_MaxReconexiones(t *testing.T) {
	if testing.Short() {
		t.Skip("test de reconexión SSE lento")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, _ := w.(http.Flusher)
		fmt.Fprintf(w, "data: {\"clienteID\":\"id\"}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(5 * time.Millisecond) // cierra tras handshake -> EOF -> reconecta
	}))
	defer srv.Close()
	c := nuevoClienteHTTPTest(t, srv)
	errCh := make(chan error, 1)
	c.OnError = func(err error) { errCh <- err }
	defer c.Desconectar()

	if err := c.Suscribir("t", func(string, []byte) {}); err != nil {
		t.Fatalf("Suscribir: %v", err)
	}

	select {
	case err := <-errCh:
		if !errores.Es(err, errores.ErrSuscripcion) {
			t.Errorf("OnError esperaba ErrSuscripcion: %v", err)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("OnError no fue invocado tras agotar reconexiones")
	}
}

// --- Desuscribir idempotente ---

func TestDesuscribir_Idempotente_NoSuscripto(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	c := nuevoClienteHTTPTest(t, srv)
	defer c.Desconectar()

	if err := c.Desuscribir("no-existe"); err != nil {
		t.Errorf("Desuscribir de tópico no suscrito debería ser nil, obtuvo: %v", err)
	}
}

// --- helpers de espera ---

func int32Func(a *atomic.Int32, ok func(int32) bool) func() bool {
	return func() bool { return ok(a.Load()) }
}

func int64Func(f func() int64, ok func(int64) bool) func() bool {
	return func() bool { return ok(f()) }
}

func waitFor(cond func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	if cond() {
		return nil
	}
	return fmt.Errorf("condición no satisfecha tras %v", timeout)
}
