package tipos

import (
	"bytes"
	"encoding/gob"
)

// ============================================================================
// AGREGACIONES
// ============================================================================

// TipoAgregacion define los tipos de agregación soportados para consultas
type TipoAgregacion string

const (
	AgregacionPromedio TipoAgregacion = "promedio"
	AgregacionMaximo   TipoAgregacion = "maximo"
	AgregacionMinimo   TipoAgregacion = "minimo"
	AgregacionSuma     TipoAgregacion = "suma"
	AgregacionConteo   TipoAgregacion = "count"
)

// ============================================================================
// STRUCTS DE CONSULTA COMPARTIDOS
// ============================================================================

// SolicitudConsultaRango representa una solicitud de consulta por rango de tiempo
type SolicitudConsultaRango struct {
	Serie        string
	TiempoInicio int64 // Unix nanosegundos
	TiempoFin    int64 // Unix nanosegundos
}

// SolicitudConsultaPunto representa una solicitud de último punto
// Los campos TiempoInicio y TiempoFin son opcionales:
//   - Si ambos son nil: retorna el último punto absoluto de cada serie
//   - Si se especifican: retorna el último punto dentro del rango temporal
type SolicitudConsultaPunto struct {
	Serie        string
	TiempoInicio *int64 // nil = sin límite inferior (Unix nanosegundos)
	TiempoFin    *int64 // nil = sin límite superior (Unix nanosegundos)
}

// ResultadoConsultaPunto representa el último punto de múltiples series en formato columnar.
// Cada serie tiene su último punto (timestamp y valor).
// Series sin datos son excluidas del resultado.
type ResultadoConsultaPunto struct {
	Series             []string // Nombres de series ordenados alfabéticamente
	Tiempos            []int64  // Timestamp del punto por serie (Unix nanosegundos)
	Valores            []any    // Valor del punto por serie
	NodosNoDisponibles []string // IDs de nodos que no respondieron (solo en consultas globales)
}

// ResultadoConsultaRango representa el resultado de una consulta de rango en formato tabular.
// Cada serie temporal es una columna, los timestamps son las filas.
// Valores faltantes se representan como nil.
type ResultadoConsultaRango struct {
	Series             []string // Columnas: nombres de series ordenados alfabéticamente
	Tiempos            []int64  // Filas: timestamps únicos ordenados ascendente (Unix nanosegundos)
	Valores            [][]any  // Matriz [fila][columna], nil = valor faltante
	NodosNoDisponibles []string // IDs de nodos que no respondieron (solo en consultas globales)
}

// RespuestaConsultaRango respuesta con resultado tabular de consulta por rango
type RespuestaConsultaRango struct {
	Resultado ResultadoConsultaRango
	Error     string
}

// RespuestaConsultaPunto respuesta con resultado de consulta de último punto en formato columnar
type RespuestaConsultaPunto struct {
	Resultado ResultadoConsultaPunto
	Error     string
}

// SolicitudConsultaAgregacion representa una solicitud de agregación (soporta múltiples)
type SolicitudConsultaAgregacion struct {
	Serie        string
	TiempoInicio int64            // Unix nanosegundos
	TiempoFin    int64            // Unix nanosegundos
	Agregaciones []TipoAgregacion // Lista de agregaciones a calcular
}

// SolicitudConsultaAgregacionTemporal representa una solicitud de downsampling (soporta múltiples)
type SolicitudConsultaAgregacionTemporal struct {
	Serie        string
	TiempoInicio int64            // Unix nanosegundos
	TiempoFin    int64            // Unix nanosegundos
	Agregaciones []TipoAgregacion // Lista de agregaciones a calcular
	Intervalo    int64            // Duration en nanosegundos
}

// ResultadoAgregacion representa el resultado columnar de múltiples agregaciones.
// Soporta múltiples agregaciones en una sola consulta.
// Estructura de Valores: [agregacion][serie]
type ResultadoAgregacion struct {
	Series             []string         // Nombres de series ordenados alfabéticamente
	Agregaciones       []TipoAgregacion // Lista ordenada de agregaciones calculadas
	Valores            [][]float64      // Matriz [agregacion][serie]
	NodosNoDisponibles []string         // IDs de nodos que no respondieron (solo en consultas globales)
}

// RespuestaConsultaAgregacion respuesta con resultado de agregación columnar
type RespuestaConsultaAgregacion struct {
	Resultado ResultadoAgregacion
	Error     string
}

// ResultadoAgregacionTemporal representa el resultado de agregaciones temporales en formato matricial.
// Soporta múltiples agregaciones en una sola consulta (patrón IoTDB/QuestDB).
// Cada serie temporal es una columna, los buckets de tiempo son las filas.
// Valores faltantes se representan como math.NaN().
//
// Estructura de Valores: [agregacion][bucket][serie]
type ResultadoAgregacionTemporal struct {
	Series             []string         // Columnas: nombres de series ordenados alfabéticamente
	Tiempos            []int64          // Filas: inicio de cada bucket (Unix nanosegundos)
	Agregaciones       []TipoAgregacion // Lista ordenada de agregaciones calculadas
	Valores            [][][]float64    // Matriz [agregacion][bucket][serie], math.NaN() = sin datos
	NodosNoDisponibles []string         // IDs de nodos que no respondieron (solo en consultas globales)
}

// RespuestaConsultaAgregacionTemporal respuesta con resultado de downsampling en formato matricial
type RespuestaConsultaAgregacionTemporal struct {
	Resultado ResultadoAgregacionTemporal
	Error     string
}

// ============================================================================
// SERIALIZACIÓN GOB (persistencia Pebble)
// ============================================================================

// SerializarGob serializa un valor usando Gob
func SerializarGob(v any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// DeserializarGob deserializa bytes usando Gob
func DeserializarGob(data []byte, v any) error {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	return decoder.Decode(v)
}

// Se registran tipos que aparecen dentro de interface{} (p. ej. Medicion.Valor).
// Los structs de consulta/respuesta del plano de control no se registran aquí:
// viajan por MQTT/JSON (el wire Gob HTTP legacy quedó archivado fuera de este repo).
func init() {
	gob.Register(Medicion{})
	gob.Register([]Medicion{})
	gob.Register(Serie{})

	gob.Register([]any{})
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register(bool(false))
	gob.Register("")
}
