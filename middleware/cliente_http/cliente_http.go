package clientehttp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sensorwave-dev/sensorwave/middleware"
	"github.com/sensorwave-dev/sensorwave/middleware/internal/errores"
	"github.com/sensorwave-dev/sensorwave/middleware/internal/mensaje"
	"github.com/sensorwave-dev/sensorwave/middleware/internal/qos"
)

type ClienteHTTP struct {
	baseURL string
	cliente *http.Client
	mu      sync.Mutex

	// clienteID asignado por el servidor durante el handshake SSE.
	clienteID string

	// stopChans permite detener la goroutine SSE por tópico desde Desuscribir.
	stopChans map[string]chan struct{}

	// OnError es invocado (síncronamente) cuando la goroutine SSE agota los
	// reintentos. Si es nil, se usa log.Printf como default.
	OnError func(error)

	// ackFallos cuenta los fallos de envío de ACK (no expuesto en la interfaz).
	ackFallos atomic.Int64
}

var ruta string = "/sensorwave"

// Constantes de reconexión SSE
const (
	maxIntentosReconexion = 5
	backoffInicial        = 1 * time.Second
	backoffFactor         = 2

	// handshakeTimeout es el timeout fijo del handshake SSE inicial.
	handshakeTimeout = 10 * time.Second
)

// Conectar crea un nuevo cliente HTTP validando host y puerto.
func Conectar(host string, puerto string) (*ClienteHTTP, error) {
	if host == "" || puerto == "" {
		return nil, fmt.Errorf("%w: host y puerto no pueden estar vacíos", errores.ErrConexion)
	}
	return &ClienteHTTP{
		baseURL:   "http://" + host + ":" + puerto,
		cliente:   &http.Client{},
		stopChans: make(map[string]chan struct{}),
	}, nil
}

// Publicar realiza un POST al servidor HTTP.
func (c *ClienteHTTP) Publicar(topico string, payload interface{}, opciones ...middleware.PublicarOpcion) error {
	mensaje, err := mensaje.Construir(topico, payload, opciones...)
	if err != nil {
		return fmt.Errorf("%w: %v", errores.ErrPublicacion, err)
	}

	// Serializar el mensaje a JSON
	mensajeBytes, err := json.Marshal(mensaje)
	if err != nil {
		return fmt.Errorf("%w: %v", errores.ErrPublicacion, err)
	}

	urlPub := fmt.Sprintf("%s%s?topico=%s", c.baseURL, ruta, url.QueryEscape(topico))
	if mensaje.QoS == 1 {
		reintentos := 0
		delay := qos.JitterAckTimeout()
		for {
			resp, err := c.cliente.Post(urlPub, "application/json", bytes.NewReader(mensajeBytes))
			if err == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					var ack struct {
						MensajeID string `json:"mensajeId"`
					}
					if json.Unmarshal(body, &ack) == nil && ack.MensajeID == mensaje.MensajeID {
						return nil
					}
				}
			}
			if reintentos >= qos.MaxRetransmisiones {
				return &errores.ErrorACK{MensajeID: mensaje.MensajeID}
			}
			time.Sleep(delay)
			delay *= qos.FactorBackoff
			reintentos++
		}
	}

	resp, err := c.cliente.Post(urlPub, "application/json", bytes.NewReader(mensajeBytes))
	if err != nil {
		return fmt.Errorf("%w: %s: %v", errores.ErrPublicacion, topico, err)
	}
	defer resp.Body.Close()

	// Verificar el código de respuesta
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: %s: %s", errores.ErrPublicacion, topico, string(body))
	}
	return nil
}

// Suscribir realiza el handshake SSE síncrono (primer mensaje con clienteID)
// con un timeout fijo de 10s. Si el handshake falla devuelve ErrSuscripcion.
// Tras el handshake, una goroutine asíncrona lee el flujo SSE con reconexión
// automática; los errores fatales se reportan vía OnError (default log.Printf).
func (c *ClienteHTTP) Suscribir(topico string, callback middleware.CallbackFunc) error {
	urlSub := fmt.Sprintf("%s%s?topico=%s", c.baseURL, ruta, url.QueryEscape(topico))
	resp, err := c.cliente.Get(urlSub)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", errores.ErrSuscripcion, topico, err)
	}

	// Handshake síncrono: leer el primer mensaje (clienteID) con timeout.
	type primer struct {
		linea string
		err   error
	}
	reader := bufio.NewReader(resp.Body)
	primerCh := make(chan primer, 1)
	go func() {
		linea, err := reader.ReadString('\n')
		primerCh <- primer{linea, err}
	}()

	select {
	case r := <-primerCh:
		if r.err != nil {
			resp.Body.Close()
			return fmt.Errorf("%w: %s: %v", errores.ErrSuscripcion, topico, r.err)
		}
		if strings.HasPrefix(r.linea, "data: ") {
			datos := strings.TrimSpace(strings.TrimPrefix(r.linea, "data: "))
			var msjClienteID map[string]string
			if json.Unmarshal([]byte(datos), &msjClienteID) == nil {
				if id, ok := msjClienteID["clienteID"]; ok {
					c.mu.Lock()
					c.clienteID = id
					c.mu.Unlock()
				}
			}
		}
	case <-time.After(handshakeTimeout):
		resp.Body.Close()
		return fmt.Errorf("%w: %s: timeout de handshake (%v)", errores.ErrSuscripcion, topico, handshakeTimeout)
	}

	// Handshake OK: registrar stop chan y lanzar la goroutine de lectura SSE.
	stop := make(chan struct{})
	c.mu.Lock()
	c.stopChans[topico] = stop
	c.mu.Unlock()

	go c.leerSSE(topico, urlSub, resp, reader, callback, stop)
	return nil
}

// leerSSE consume el flujo SSE con reconexión automática hasta que se cierre
// el stop chan (Desuscribir) o se agoten los reintentos (OnError).
func (c *ClienteHTTP) leerSSE(topico, urlSub string, resp *http.Response, reader *bufio.Reader, callback middleware.CallbackFunc, stop chan struct{}) {
	procesarLinea := func(linea string) {
		if !strings.HasPrefix(linea, "data: ") {
			return
		}
		datos := strings.TrimSpace(strings.TrimPrefix(linea, "data: "))
		var msjDatos struct {
			MensajeID string `json:"mensajeId,omitempty"`
			QoS       int    `json:"qos"`
			Topico    string `json:"topico"`
			Payload   []byte `json:"payload"`
		}
		if json.Unmarshal([]byte(datos), &msjDatos) == nil {
			if msjDatos.QoS == 1 && msjDatos.MensajeID != "" {
				c.enviarAck(msjDatos.MensajeID)
			}
			callback(msjDatos.Topico, msjDatos.Payload)
		}
	}

	// alimentar lanza una goroutine que vuelca las líneas del reader a dos
	// canales. Al cerrarse stop, el reader subyacente se cierra desde el loop
	// principal, desbloqueando ReadString.
	alimentar := func(rd *bufio.Reader) (chan string, chan error) {
		lineas := make(chan string)
		errCh := make(chan error, 1)
		go func() {
			for {
				linea, err := rd.ReadString('\n')
				if err != nil {
					errCh <- err
					return
				}
				select {
				case lineas <- linea:
				case <-stop:
					return
				}
			}
		}()
		return lineas, errCh
	}

	lineas, errCh := alimentar(reader)
	reconexiones := 0
	delay := backoffInicial

	for {
		select {
		case <-stop:
			resp.Body.Close()
			return
		case err := <-errCh:
			resp.Body.Close()
			if err == io.EOF {
				log.Printf("Conexión SSE de %s cerrada por el servidor, reconectando...", topico)
			} else {
				log.Printf("Error al leer el flujo SSE de %s: %v, reconectando...", topico, err)
			}
			reconexiones++
			if reconexiones >= maxIntentosReconexion {
				c.notificarError(fmt.Errorf("%w: %s: máximo de reconexiones alcanzado (%d)",
					errores.ErrSuscripcion, topico, maxIntentosReconexion))
				return
			}
			if !c.esperarReconexion(stop, delay) {
				return
			}
			delay *= backoffFactor

			// Reintentar la conexión HTTP hasta éxito o agotamiento.
			for {
				nuevoResp, err := c.cliente.Get(urlSub)
				if err == nil {
					resp = nuevoResp
					reader = bufio.NewReader(resp.Body)
					lineas, errCh = alimentar(reader)
					break
				}
				reconexiones++
				if reconexiones >= maxIntentosReconexion {
					c.notificarError(fmt.Errorf("%w: %s: máximo de reconexiones alcanzado (%d)",
						errores.ErrSuscripcion, topico, maxIntentosReconexion))
					return
				}
				log.Printf("Error SSE (intento %d/%d) en %s: %v, reintentando en %v...",
					reconexiones, maxIntentosReconexion, topico, err, delay)
				if !c.esperarReconexion(stop, delay) {
					return
				}
				delay *= backoffFactor
			}
		case linea := <-lineas:
			procesarLinea(linea)
		}
	}
}

// esperarReconexion duerme delay interrumpible por stop. Retorna false si se
// detuvo durante la espera.
func (c *ClienteHTTP) esperarReconexion(stop chan struct{}, delay time.Duration) bool {
	select {
	case <-stop:
		return false
	case <-time.After(delay):
		return true
	}
}

// notificarError invoca OnError si está configurado, o log.Printf por defecto.
func (c *ClienteHTTP) notificarError(err error) {
	if c.OnError != nil {
		c.OnError(err)
		return
	}
	log.Printf("%v", err)
}

func (c *ClienteHTTP) enviarAck(MensajeID string) {
	c.mu.Lock()
	clienteID := c.clienteID
	c.mu.Unlock()

	if clienteID == "" {
		c.ackFallos.Add(1)
		return
	}

	body, err := json.Marshal(map[string]string{
		"clienteId": clienteID,
		"mensajeId": MensajeID,
	})
	if err != nil {
		c.ackFallos.Add(1)
		return
	}

	urlAck := fmt.Sprintf("%s%s/ack", c.baseURL, ruta)
	resp, err := c.cliente.Post(urlAck, "application/json", bytes.NewReader(body))
	if err != nil {
		c.ackFallos.Add(1)
		return
	}
	defer resp.Body.Close()
}

// FallosACK devuelve el contador atómico de fallos de envío de ACK.
// No forma parte de la interfaz Cliente.
func (c *ClienteHTTP) FallosACK() int64 {
	return c.ackFallos.Load()
}

// Desuscribir detiene la goroutine SSE del tópico (vía stop chan) y envía la
// cancelación al servidor. Es idempotente: retorna nil si el tópico no estaba
// suscrito. Devuelve error sólo si falla la cancelación de red (DELETE).
func (c *ClienteHTTP) Desuscribir(topico string) error {
	c.mu.Lock()
	stop, ok := c.stopChans[topico]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	delete(c.stopChans, topico)
	clienteID := c.clienteID
	c.mu.Unlock()

	close(stop)

	if clienteID == "" {
		return nil
	}

	urlUnsub := fmt.Sprintf("%s%s?topico=%s&clienteID=%s", c.baseURL, ruta, url.QueryEscape(topico), url.QueryEscape(clienteID))
	req, err := http.NewRequest("DELETE", urlUnsub, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", errores.ErrDesuscripcion, err)
	}
	resp, err := c.cliente.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", errores.ErrDesuscripcion, topico, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: %s: %s", errores.ErrDesuscripcion, topico, string(body))
	}
	return nil
}

// Desconectar cierra las conexiones del cliente HTTP
func (c *ClienteHTTP) Desconectar() {
	if c.cliente != nil {
		if transporte, ok := c.cliente.Transport.(*http.Transport); ok {
			transporte.CloseIdleConnections()
		}
	}
}
