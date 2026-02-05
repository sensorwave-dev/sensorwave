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

	"github.com/sensorwave-dev/sensorwave/middleware"
)

type ClienteHTTP struct {
	BaseURL   string
	Cliente   *http.Client
	clienteID string
	mu        sync.Mutex
}

var ruta string = "/sensorwave"

// NuevoClienteHTTP crea un nuevo cliente HTTP
func Conectar(host string, puerto string) *ClienteHTTP {
	return &ClienteHTTP{
		BaseURL: "http://" + host + ":" + puerto,
		Cliente: &http.Client{},
	}
}

// Publicar realiza un POST al servidor HTTP
func (c *ClienteHTTP) Publicar(topico string, payload interface{}) {
	// Convertir el payload a []byte
	var data []byte
	switch v := payload.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	case int, int32, int64, float32, float64:
		data = []byte(fmt.Sprintf("%v", v))
	default:
		// Serializar a JSON para otros tipos
		var err error
		data, err = json.Marshal(v)
		if err != nil {
			log.Fatalf("Error al serializar el payload: %v", err)
		}
	}

	mensaje := middleware.Mensaje{Original: true, Topico: topico, Payload: data, Interno: false}

	// Serializar el mensaje a JSON
	mensajeBytes, err := json.Marshal(mensaje)
	if err != nil {
		log.Fatalf("Error al serializar el mensaje: %v", err)
	}

	// Realizar la solicitud POST
	url := fmt.Sprintf("%s%s?topico=%s", c.BaseURL, ruta, url.QueryEscape(topico))
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

				callback(topico, datos)
			}
		}
	}()
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
