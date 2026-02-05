// Package edge implementa ejecutores de acciones para el motor de reglas.
// Los ejecutores permiten publicar mensajes a través del middleware
// (MQTT, HTTP, CoAP) cuando se activan las reglas.
package edge

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/sensorwave-dev/sensorwave/middleware"
	"github.com/sensorwave-dev/sensorwave/middleware/cliente_coap"
	clientehttp "github.com/sensorwave-dev/sensorwave/middleware/cliente_http"
	"github.com/sensorwave-dev/sensorwave/middleware/cliente_mqtt"
)

// PayloadActuador define la estructura estándar para mensajes de actuadores.
// Esta estructura se serializa a JSON y se envía a través del middleware.
type PayloadActuador struct {
	// Comando es la acción a ejecutar (ej: "encender", "apagar", "ajustar")
	Comando string `json:"comando"`

	// Timestamp es el momento en que se generó el comando
	Timestamp time.Time `json:"timestamp"`

	// ReglaID identifica la regla que generó este comando
	ReglaID string `json:"regla_id"`

	// Parametros contiene parámetros adicionales del comando
	// (ej: velocidad, nivel, duración)
	Parametros map[string]string `json:"parametros,omitempty"`

	// Contexto contiene los valores de las series que dispararon la regla
	Contexto map[string]interface{} `json:"contexto,omitempty"`
}

// --- Funciones para manejo de plantillas ---

// variableRegex reconoce variables en formato {nombre}
var variableRegex = regexp.MustCompile(`\{([a-zA-Z_][a-zA-Z0-9_]*)\}`)

// ExtraerVariables extrae todas las variables {nombre} de una plantilla.
// Retorna un slice con los nombres de las variables encontradas (sin llaves).
//
// Ejemplo:
//
//	ExtraerVariables("actuadores/{nodo_id}/{tipo}") → ["nodo_id", "tipo"]
func ExtraerVariables(plantilla string) []string {
	matches := variableRegex.FindAllStringSubmatch(plantilla, -1)
	variables := make([]string, 0, len(matches))

	seen := make(map[string]bool)
	for _, match := range matches {
		if len(match) > 1 && !seen[match[1]] {
			variables = append(variables, match[1])
			seen[match[1]] = true
		}
	}

	return variables
}

// ValidarPlantilla verifica que una plantilla tenga sintaxis válida.
// Retorna error si hay llaves desbalanceadas o nombres de variables inválidos.
//
// Reglas de validación:
//   - Las llaves deben estar balanceadas
//   - Los nombres de variables deben comenzar con letra o guión bajo
//   - Los nombres solo pueden contener letras, números y guiones bajos
func ValidarPlantilla(plantilla string) error {
	// Verificar llaves desbalanceadas
	abierta := 0
	for i, c := range plantilla {
		if c == '{' {
			abierta++
			if abierta > 1 {
				return fmt.Errorf("llave '{' anidada en posición %d", i)
			}
		} else if c == '}' {
			abierta--
			if abierta < 0 {
				return fmt.Errorf("llave '}' sin abrir en posición %d", i)
			}
		}
	}
	if abierta != 0 {
		return fmt.Errorf("llave '{' sin cerrar")
	}

	// Verificar nombres de variables válidos
	invalidVarRegex := regexp.MustCompile(`\{([^}]*)\}`)
	matches := invalidVarRegex.FindAllStringSubmatch(plantilla, -1)

	for _, match := range matches {
		if len(match) > 1 {
			nombre := match[1]
			if nombre == "" {
				return fmt.Errorf("nombre de variable vacío")
			}
			if !variableRegex.MatchString("{" + nombre + "}") {
				return fmt.Errorf("nombre de variable inválido: '%s'", nombre)
			}
		}
	}

	return nil
}

// variablesContexto son las variables que se resuelven desde el contexto de ejecución
// y no necesitan estar en Params.
var variablesContexto = map[string]bool{
	"serie":        true,
	"serie_0":      true,
	"serie_1":      true,
	"serie_2":      true,
	"serie_3":      true,
	"serie_4":      true,
	"serie_5":      true,
	"regla_id":     true,
	"regla_nombre": true,
}

// ValidarVariablesRequeridas verifica que las variables en la plantilla
// existan en params o sean variables de contexto.
// Retorna error si hay variables faltantes.
//
// Variables de contexto (no requieren estar en params):
//   - serie, serie_0, serie_1, ... (segmentos del path de la serie)
//   - regla_id, regla_nombre
func ValidarVariablesRequeridas(plantilla string, params map[string]string) error {
	variables := ExtraerVariables(plantilla)
	var faltantes []string

	for _, v := range variables {
		// Ignorar variables de contexto
		if variablesContexto[v] {
			continue
		}

		// Verificar si existe en params
		if _, existe := params[v]; !existe {
			faltantes = append(faltantes, v)
		}
	}

	if len(faltantes) > 0 {
		return fmt.Errorf("variables faltantes: %s", strings.Join(faltantes, ", "))
	}

	return nil
}

// ResolverPlantilla reemplaza las variables {nombre} en una plantilla
// con valores de params y del contexto de ejecución.
//
// Orden de resolución:
//  1. Variables de params: {nombre} → params["nombre"]
//  2. Variables de contexto: {serie}, {serie_0}, {regla_id}, etc.
//  3. Variables no resueltas se reemplazan con string vacío
//
// Ejemplo:
//
//	plantilla: "actuadores/{nodo_id}/{serie_0}/ventilador"
//	params: {"nodo_id": "zona_1"}
//	contexto: {"_serie_0": "nodo_03"}
//	resultado: "actuadores/zona_1/nodo_03/ventilador"
func ResolverPlantilla(plantilla string, params map[string]string, regla *Regla, contexto map[string]interface{}) string {
	resultado := plantilla

	// 1. Reemplazar variables de params
	for key, value := range params {
		placeholder := "{" + key + "}"
		resultado = strings.ReplaceAll(resultado, placeholder, value)
	}

	// 2. Reemplazar variables de contexto
	if regla != nil {
		resultado = strings.ReplaceAll(resultado, "{regla_id}", regla.ID)
		resultado = strings.ReplaceAll(resultado, "{regla_nombre}", regla.Nombre)
	}

	// 3. Reemplazar variables de serie desde contexto
	if contexto != nil {
		if serie, ok := contexto["_serie"].(string); ok {
			resultado = strings.ReplaceAll(resultado, "{serie}", serie)
		}

		// Segmentos de la serie: _serie_0, _serie_1, etc.
		for i := 0; i <= 5; i++ {
			key := fmt.Sprintf("_serie_%d", i)
			placeholder := fmt.Sprintf("{serie_%d}", i)
			if valor, ok := contexto[key].(string); ok {
				resultado = strings.ReplaceAll(resultado, placeholder, valor)
			}
		}
	}

	// 4. Reemplazar variables no resueltas con string vacío
	resultado = variableRegex.ReplaceAllString(resultado, "")

	return resultado
}

// --- Ejecutor genérico ---

// CrearEjecutorPublicar crea un ejecutor que publica mensajes a través
// del cliente de middleware proporcionado.
//
// El ejecutor:
//   - Resuelve la plantilla del destino (Accion.Destino) con variables
//   - Construye un PayloadActuador con el comando y contexto
//   - Publica al tópico resuelto
//
// Uso:
//
//	cliente := cliente_mqtt.Conectar("localhost", "1883")
//	ejecutor := CrearEjecutorPublicar(cliente)
//	motor.RegistrarEjecutor("publicar_mqtt", ejecutor)
func CrearEjecutorPublicar(cliente middleware.Cliente) EjecutorAccion {
	return func(accion Accion, regla *Regla, valores map[string]interface{}) error {
		if cliente == nil {
			return fmt.Errorf("cliente de middleware no configurado")
		}

		// Resolver el tópico con variables
		topico := ResolverPlantilla(accion.Destino, accion.Params, regla, valores)
		if topico == "" {
			return fmt.Errorf("tópico resuelto está vacío")
		}

		// Construir payload
		payload := PayloadActuador{
			Comando:    accion.Params["comando"],
			Timestamp:  time.Now(),
			ReglaID:    regla.ID,
			Parametros: filtrarParametrosInternos(accion.Params),
			Contexto:   filtrarContextoPublico(valores),
		}

		// Serializar a JSON
		data, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("error serializando payload: %v", err)
		}

		// Publicar
		cliente.Publicar(topico, data)
		log.Printf("Ejecutor: publicado en '%s' por regla '%s'", topico, regla.ID)

		return nil
	}
}

// filtrarParametrosInternos excluye parámetros que no deben ir en el payload
func filtrarParametrosInternos(params map[string]string) map[string]string {
	excluir := map[string]bool{
		"comando": true, // ya está como campo principal
	}

	resultado := make(map[string]string)
	for k, v := range params {
		if !excluir[k] {
			resultado[k] = v
		}
	}
	return resultado
}

// filtrarContextoPublico excluye metadatos internos del contexto
func filtrarContextoPublico(contexto map[string]interface{}) map[string]interface{} {
	resultado := make(map[string]interface{})
	for k, v := range contexto {
		// Excluir claves que comienzan con "_"
		if !strings.HasPrefix(k, "_") {
			resultado[k] = v
		}
	}
	return resultado
}

// --- Helpers de registro ---

// RegistrarEjecutorMQTT conecta a un broker MQTT, crea un ejecutor y lo registra
// en el motor de reglas.
//
// Retorna el cliente para que el llamador pueda hacer defer cliente.Desconectar()
//
// Ejemplo:
//
//	cliente, err := edge.RegistrarEjecutorMQTT(motor, "publicar_mqtt", "localhost", "1883")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer cliente.Desconectar()
func RegistrarEjecutorMQTT(motor *MotorReglas, nombre, direccion, puerto string) (middleware.Cliente, error) {
	if motor == nil {
		return nil, fmt.Errorf("motor de reglas no puede ser nil")
	}

	cliente := cliente_mqtt.Conectar(direccion, puerto)
	if cliente == nil {
		return nil, fmt.Errorf("no se pudo conectar a MQTT en %s:%s", direccion, puerto)
	}

	ejecutor := CrearEjecutorPublicar(cliente)
	if err := motor.RegistrarEjecutor(nombre, ejecutor); err != nil {
		cliente.Desconectar()
		return nil, fmt.Errorf("error registrando ejecutor: %v", err)
	}

	log.Printf("Ejecutor MQTT '%s' registrado (broker: %s:%s)", nombre, direccion, puerto)
	return cliente, nil
}

// RegistrarEjecutorHTTP conecta a un servidor HTTP, crea un ejecutor y lo registra
// en el motor de reglas.
//
// Retorna el cliente para que el llamador pueda hacer defer cliente.Desconectar()
func RegistrarEjecutorHTTP(motor *MotorReglas, nombre, direccion, puerto string) (middleware.Cliente, error) {
	if motor == nil {
		return nil, fmt.Errorf("motor de reglas no puede ser nil")
	}

	cliente := clientehttp.Conectar(direccion, puerto)
	if cliente == nil {
		return nil, fmt.Errorf("no se pudo crear cliente HTTP para %s:%s", direccion, puerto)
	}

	ejecutor := CrearEjecutorPublicar(cliente)
	if err := motor.RegistrarEjecutor(nombre, ejecutor); err != nil {
		cliente.Desconectar()
		return nil, fmt.Errorf("error registrando ejecutor: %v", err)
	}

	log.Printf("Ejecutor HTTP '%s' registrado (servidor: %s:%s)", nombre, direccion, puerto)
	return cliente, nil
}

// RegistrarEjecutorCoAP conecta a un servidor CoAP, crea un ejecutor y lo registra
// en el motor de reglas.
//
// Retorna el cliente para que el llamador pueda hacer defer cliente.Desconectar()
func RegistrarEjecutorCoAP(motor *MotorReglas, nombre, direccion, puerto string) (middleware.Cliente, error) {
	if motor == nil {
		return nil, fmt.Errorf("motor de reglas no puede ser nil")
	}

	cliente := cliente_coap.Conectar(direccion, puerto)
	if cliente == nil {
		return nil, fmt.Errorf("no se pudo conectar a CoAP en %s:%s", direccion, puerto)
	}

	ejecutor := CrearEjecutorPublicar(cliente)
	if err := motor.RegistrarEjecutor(nombre, ejecutor); err != nil {
		cliente.Desconectar()
		return nil, fmt.Errorf("error registrando ejecutor: %v", err)
	}

	log.Printf("Ejecutor CoAP '%s' registrado (servidor: %s:%s)", nombre, direccion, puerto)
	return cliente, nil
}
