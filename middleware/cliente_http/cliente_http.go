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
	"time"

	"github.com/sensorwave-dev/sensorwave/middleware"
	"github.com/sensorwave-dev/sensorwave/middleware/internal/mensaje"
)

type ClienteHTTP struct {
	baseURL   string
	cliente   *http.Client
	clienteID string
	mu        sync.Mutex
}

var ruta string = "/sensorwave"

const (
	ackTimeout         = 2 * time.Second
	factorAleatorioAck = 1.5
	maxRetransmisiones = 4
)

// Constantes de reconexión SSE
const (
	maxIntentosReconexion = 5
	backoffInicial        = 1 * time.Second
	backoffFactor         = 2
)

// NuevoClienteHTTP crea un nuevo cliente HTTP
func Conectar(host string, puerto string) *ClienteHTTP {
	return &ClienteHTTP{
		baseURL: "http://" + host + ":" + puerto,
		cliente: &http.Client{},
	}
}

// Publicar realiza un POST al servidor HTTP
func (c *ClienteHTTP) Publicar(topico string, payload interface{}, opciones ...middleware.PublicarOpcion) {
	mensaje, err := mensaje.Construir(topico, payload, opciones...)
	if err != nil {
		log.Printf("Error al construir mensaje: %v", err)
		return
	}

	// Serializar el mensaje a JSON
	mensajeBytes, err := json.Marshal(mensaje)
	if err != nil {
		log.Printf("Error al serializar el mensaje: %v", err)
		return
	}

	url := fmt.Sprintf("%s%s?topico=%s", c.baseURL, ruta, url.QueryEscape(topico))
	if mensaje.QoS == 1 {
		reintentos := 0
		delay := ackTimeout
		for {
			resp, err := c.cliente.Post(url, "application/json", bytes.NewReader(mensajeBytes))
			if err == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					var ack struct {
						MensajeID string `json:"mensajeId"`
					}
					if json.Unmarshal(body, &ack) == nil && ack.MensajeID == mensaje.MensajeID {
						return
					}
				}
			}
			if reintentos >= maxRetransmisiones {
				log.Printf("No se recibió ACK para mensajeId %s después de %d intentos", mensaje.MensajeID, maxRetransmisiones)
				return
			}
			time.Sleep(delay)
			delay = time.Duration(float64(delay) * factorAleatorioAck)
			reintentos++
		}
	}

	resp, err := c.cliente.Post(url, "application/json", bytes.NewReader(mensajeBytes))
	if err != nil {
		log.Printf("Error al realizar el POST: %v", err)
		return
	}
	defer resp.Body.Close()

	// Verificar el código de respuesta
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Error en la respuesta del servidor: %s", string(body))
	}
}

func (c *ClienteHTTP) Suscribir(topico string, callback middleware.CallbackFunc) {
	go func() {
		reconexiones := 0
		delay := backoffInicial

		for reconexiones < maxIntentosReconexion {
			url := fmt.Sprintf("%s%s?topico=%s", c.baseURL, ruta, url.QueryEscape(topico))
			resp, err := c.cliente.Get(url)
			if err != nil {
				reconexiones++
				if reconexiones >= maxIntentosReconexion {
					log.Fatalf("Error al conectar SSE después de %d intentos: %v", maxIntentosReconexion, err)
				}
				log.Printf("Error SSE (intento %d/%d): %v, reintentando en %v...",
					reconexiones, maxIntentosReconexion, err, delay)
				time.Sleep(delay)
				delay *= backoffFactor
				continue
			}

			// Conexión exitosa: resetear contadores
			reconexiones = 0
			delay = backoffInicial

			// Lee el stream SSE
			reader := bufio.NewReader(resp.Body)
			primerMensaje := true
			sseError := false

			for {
				linea, err := reader.ReadString('\n')
				if err != nil {
					resp.Body.Close()
					if err == io.EOF {
						log.Printf("Conexión SSE cerrada por el servidor, reconectando...")
					} else {
						log.Printf("Error al leer el flujo SSE: %v, reconectando...", err)
					}
					sseError = true
					break
				}

				if strings.HasPrefix(linea, "data: ") {
					datos := strings.TrimPrefix(linea, "data: ")
					datos = strings.TrimSpace(datos)

					// Procesar el primer mensaje para obtener el clienteID
					var msjClienteID map[string]string
					if primerMensaje {
						if err := json.Unmarshal([]byte(datos), &msjClienteID); err == nil {
							if id, ok := msjClienteID["clienteID"]; ok {
								c.mu.Lock()
								c.clienteID = id
								c.mu.Unlock()
								primerMensaje = false
								continue
							}
						}
					}

					var msjDatos struct {
						MensajeID string `json:"mensajeId,omitempty"`
						QoS       int    `json:"qos"`
						Topico    string `json:"topico"`
						Payload   []byte `json:"payload"`
					}
					if err := json.Unmarshal([]byte(datos), &msjDatos); err == nil {
						if msjDatos.QoS == 1 && msjDatos.MensajeID != "" {
							c.enviarAck(msjDatos.MensajeID)
						}
					}
					callback(msjDatos.Topico, msjDatos.Payload)
				}
			}

			if sseError {
				reconexiones++
				if reconexiones >= maxIntentosReconexion {
					log.Fatalf("Máximo de reconexiones SSE alcanzado (%d)", maxIntentosReconexion)
				}
				log.Printf("Reconectando SSE (intento %d/%d) en %v...",
					reconexiones, maxIntentosReconexion, delay)
				time.Sleep(delay)
				delay *= backoffFactor
			}
		}
	}()
}

func (c *ClienteHTTP) enviarAck(MensajeID string) {
	c.mu.Lock()
	clienteID := c.clienteID
	c.mu.Unlock()

	if clienteID == "" {
		return
	}

	body, err := json.Marshal(map[string]string{
		"clienteId": clienteID,
		"mensajeId": MensajeID,
	})
	if err != nil {
		return
	}

	url := fmt.Sprintf("%s%s/ack", c.baseURL, ruta)
	resp, err := c.cliente.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func (c *ClienteHTTP) Desuscribir(topico string) {
	c.mu.Lock()
	clienteID := c.clienteID
	c.mu.Unlock()

	if clienteID == "" {
		return
	}

	url := fmt.Sprintf("%s%s?topico=%s&clienteID=%s", c.baseURL, ruta, url.QueryEscape(topico), url.QueryEscape(clienteID))
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("Error al crear la solicitud DELETE: %v", err)
		return
	}
	resp, err := c.cliente.Do(req)
	if err != nil {
		log.Printf("Error al realizar la solicitud DELETE: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Error en la respuesta del servidor: %s", string(body))
	}
}

// Desconectar cierra las conexiones del cliente HTTP
func (c *ClienteHTTP) Desconectar() {
	if c.cliente != nil {
		if transporte, ok := c.cliente.Transport.(*http.Transport); ok {
			transporte.CloseIdleConnections()
		}
	}
}
