package tipos

import (
	"path"
	"strings"
)

// Información de un nodo registrado
type Nodo struct {
	NodoID     string            `json:"nodo_id"`          // Identificador del nodo
	Direccion  string            `json:"direccion"`        // Dirección pública del nodo
	PuertoHTTP string            `json:"puerto_http"`      // Puerto HTTP del nodo
	Series     map[string]Serie  `json:"series"`           // Lista de series gestionadas por el nodo
	Tags       map[string]string `json:"tags,omitempty"`   // Metadatos libres del nodo (nombre, ubicación, etc.)
	Reglas     []Regla           `json:"reglas,omitempty"` // Lista de reglas del motor de reglas
}

// Regla representa una regla del motor de reglas (versión serializable)
type Regla struct {
	ID          string      `json:"id"`
	Nombre      string      `json:"nombre"`
	Activa      bool        `json:"activa"`
	Logica      string      `json:"logica"` // "AND" o "OR"
	Condiciones []Condicion `json:"condiciones"`
	Acciones    []Accion    `json:"acciones"`
}

// Condicion representa una condición de una regla
type Condicion struct {
	Path          string      `json:"path"`
	VentanaT      string      `json:"ventana_t"`  // ej: "5m", "1h"
	Agregacion    string      `json:"agregacion"` // "promedio", "maximo", etc.
	Operador      string      `json:"operador"`   // ">=", "<", "==", etc.
	Valor         interface{} `json:"valor"`      // número, string, bool
	AgregarSeries bool        `json:"agregar_series"`
}

// Accion representa una acción de una regla
type Accion struct {
	Tipo       string            `json:"tipo"`
	Destino    string            `json:"destino"`
	Parametros map[string]string `json:"params"`
}

// Serie representa una serie de datos de tiempo
type Serie struct {
	SerieId              int                  `json:"serie_id"`              // ID de la serie en la base de datos
	Path                 string               `json:"path"`                  // Path jerárquico: "dispositivo_001/temperatura"
	Tags                 map[string]string    `json:"tags"`                  // Tags: {"unidad": "Celsius", "tipo": "DHT22"}
	TipoDatos            TipoDatos            `json:"tipo_datos"`            // Tipo de datos almacenados
	CompresionBloque     TipoCompresionBloque `json:"compresion_bloque"`     // Compresión nivel bloque
	CompresionBytes      TipoCompresion       `json:"compresion_bytes"`      // Compresión nivel valores (algoritmos específicos por tipo)
	TamañoBloque         int                  `json:"tamaño_bloque"`         // Tamaño del bloque
	TiempoAlmacenamiento int64                `json:"tiempo_almacenamiento"` // Tiempo máximo de almacenamiento en nanosegundos (0 = sin límite)
}

// CoincidePath verifica si un path coincide con un patrón glob.
// Soporta wildcard '*' que matchea cualquier secuencia de caracteres.
// Cuando el último segmento del patrón es '*', matchea múltiples niveles.
// Ejemplos:
//   - CoincidePath("sensor_01/temp", "*/temp") -> true
//   - CoincidePath("sensor_01/temp", "sensor_01/*") -> true
//   - CoincidePath("sensor_01/temp", "*") -> true (caso especial)
//   - CoincidePath("dispositivo1/temp", "dispositivo*/temp") -> true
//   - CoincidePath("nodo/dev/sensor", "nodo/*") -> true (wildcard al final = múltiples niveles)
//   - CoincidePath("dispositivo1/temp/extra", "dispositivo*/*") -> true
func CoincidePath(pathStr, patron string) bool {
	if patron == "*" {
		return true
	}

	patronSegmentos := strings.Split(patron, "/")
	pathSegmentos := strings.Split(pathStr, "/")

	// Si el último segmento del patrón es "*", permite múltiples niveles
	if len(patronSegmentos) > 0 && patronSegmentos[len(patronSegmentos)-1] == "*" {
		// Necesitamos al menos tantos segmentos en el path como en el patrón (sin el último *)
		if len(pathSegmentos) < len(patronSegmentos)-1 {
			return false
		}

		// Verificar que todos los segmentos del patrón (excepto el último *) coincidan
		for i := 0; i < len(patronSegmentos)-1; i++ {
			matched, _ := path.Match(patronSegmentos[i], pathSegmentos[i])
			if !matched {
				return false
			}
		}
		return true
	}

	// Comportamiento estándar: mism número de segmentos requerido
	matched, _ := path.Match(patron, pathStr)
	return matched
}

// EsPatronWildcard determina si un path contiene wildcards
func EsPatronWildcard(path string) bool {
	return strings.Contains(path, "*")
}
