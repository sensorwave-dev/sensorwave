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
	BaseURL   string
	Cliente   *http.Client
	clienteID string
	mu        sync.Mutex
}

var ruta string = "/sensorwave"

const (
	ackTimeout      = 2 * time.Second
	ackRandomFactor = 1.5
	maxRetransmit   = 4
)

// NuevoClienteHTTP crea un nuevo cliente HTTP
func Conectar(host string, puerto string) *ClienteHTTP {
	return &ClienteHTTP{
		BaseURL: "http://" + host + ":" + puerto,
		Cliente: &http.Client{},
	}
}

// Publicar realiza un POST al servidor HTTP
func (c *ClienteHTTP) Publicar(topico string, payload interface{}, opciones ...middleware.PublicarOpcion) {
	mensaje, err := mensaje.Construir(topico, payload, opciones...)
	if err != nil {
		log.Fatalf("Error al construir mensaje: %v", err)
	}

	// Serializar el mensaje a JSON
	mensajeBytes, err := json.Marshal(mensaje)
	if err != nil {
		log.Fatalf("Error al serializar el mensaje: %v", err)
	}

	url := fmt.Sprintf("%s%s?topico=%s", c.BaseURL, ruta, url.QueryEscape(topico))
	if mensaje.QoS == 1 {
		reintentos := 0
		delay := ackTimeout
		for {
			resp, err := c.Cliente.Post(url, "application/json", bytes.NewReader(mensajeBytes))
			if err == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					var ack struct {
						MessageID string `json:"messageId"`
					}
					if json.Unmarshal(body, &ack) == nil && ack.MessageID == mensaje.MessageID {
						return
					}
				}
			}
			if reintentos >= maxRetransmit {
				log.Fatalf("No se recibió ACK para messageId %s", mensaje.MessageID)
			}
			time.Sleep(delay)
			delay = time.Duration(float64(delay) * ackRandomFactor)
			reintentos++
		}
	}

	resp, err := c.Cliente.Post(url, "application/json", bytes.NewReader(mensajeBytes))
	if err != nil {
		log.Fatalf("Error al realizar el POST: %v", err)
	}
	defer resp.Body.Close()

	// Verificar el código de respuesta
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("Error en la respuesta del servidor: %s", string(body))
	}
}

func (c *ClienteHTTP) Suscribir(topico string, callback middleware.CallbackFunc) {
	go func() {
		url := fmt.Sprintf("%s%s?topico=%s", c.BaseURL, ruta, url.QueryEscape(topico))
		resp, err := c.Cliente.Get(url)
		if err != nil {
			log.Fatalf("Error al realizar el GET: %v", err)
		}
		defer resp.Body.Close()

		reader := bufio.NewReader(resp.Body)
		primerMensaje := true

		for {
			linea, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					log.Println("Conexión SSE cerrada por el servidor")
					break
				}
				log.Fatalf("Error al leer el flujo SSE: %v", err)
			}

			if strings.HasPrefix(linea, "data: ") {
				datos := strings.TrimPrefix(linea, "data: ")
				datos = strings.TrimSpace(datos)

				if primerMensaje {
					var msg map[string]string
					if err := json.Unmarshal([]byte(datos), &msg); err == nil {
						if id, ok := msg["clienteID"]; ok {
							c.mu.Lock()
							c.clienteID = id
							c.mu.Unlock()
							primerMensaje = false
							continue
						}
					}
				}

				var payloadStr string
				if strings.HasPrefix(datos, "{") {
					var msg struct {
						MessageID string `json:"messageId"`
						QoS       int    `json:"qos"`
						Payload   string `json:"payload"`
					}
					if err := json.Unmarshal([]byte(datos), &msg); err == nil {
						payloadStr = msg.Payload
						if msg.QoS == 1 && msg.MessageID != "" {
							c.enviarAck(msg.MessageID)
						}
					}
				}
				if payloadStr == "" {
					payloadStr = datos
				}
				callback(topico, payloadStr)
			}
		}
	}()
}

func (c *ClienteHTTP) enviarAck(messageID string) {
	c.mu.Lock()
	clienteID := c.clienteID
	c.mu.Unlock()

	if clienteID == "" {
		return
	}

	body, err := json.Marshal(map[string]string{
		"clienteId": clienteID,
		"messageId": messageID,
	})
	if err != nil {
		return
	}

	url := fmt.Sprintf("%s%s/ack", c.BaseURL, ruta)
	resp, err := c.Cliente.Post(url, "application/json", bytes.NewReader(body))
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
		log.Println("No hay clienteID disponible, no se puede desuscribir")
		return
	}

	url := fmt.Sprintf("%s%s?topico=%s&clienteID=%s", c.BaseURL, ruta, url.QueryEscape(topico), url.QueryEscape(clienteID))
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Fatalf("Error al crear la solicitud DELETE: %v", err)
	}
	resp, err := c.Cliente.Do(req)
	if err != nil {
		log.Fatalf("Error al realizar la solicitud DELETE: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("Error en la respuesta del servidor: %s", string(body))
	}
	fmt.Println("Desuscrito del tópico:", topico)
}

// Desconectar cierra las conexiones del cliente HTTP
func (c *ClienteHTTP) Desconectar() {
	if c.Cliente != nil {
		if transporte, ok := c.Cliente.Transport.(*http.Transport); ok {
			transporte.CloseIdleConnections()
		}
	}
	fmt.Println("Cliente HTTP desconectado")
}
