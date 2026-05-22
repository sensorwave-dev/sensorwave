package despachador

import (
	"github.com/sensorwave-dev/sensorwave/tipos"
)

// ============================================================================
// TIPOS DE RESPUESTA DE LA API REST
// Estos structs son usados por los handlers para serializar respuestas JSON
// ============================================================================

// StatusResponse respuesta del endpoint /api/status
type StatusResponse struct {
	NumNodos  int `json:"num_nodos"`
	NumSeries int `json:"num_series"`
}

// SerieResponse respuesta con información de serie para JSON
type SerieResponse struct {
	Path                 string                     `json:"path"`
	NodoID               string                     `json:"nodo_id"`
	TipoDatos            tipos.TipoDatos            `json:"tipo_datos"`
	Tags                 map[string]string          `json:"tags,omitempty"`
	TamanoBloque         int                        `json:"tamano_bloque"`
	TiempoAlmacenamiento int64                      `json:"tiempo_almacenamiento"`
	CompresionBytes      tipos.TipoCompresion       `json:"compresion_bytes"`
	CompresionBloque     tipos.TipoCompresionBloque `json:"compresion_bloque"`
}

// ConsultaRangoRequest solicitud de consulta por rango
type ConsultaRangoRequest struct {
	Serie        string `json:"serie"`
	TiempoInicio int64  `json:"tiempo_inicio"` // Unix nanosegundos
	TiempoFin    int64  `json:"tiempo_fin"`    // Unix nanosegundos
}

// ConsultaRangoResponse respuesta de consulta por rango
type ConsultaRangoResponse struct {
	Series             []string        `json:"series"`
	Tiempos            []int64         `json:"tiempos"`
	Valores            [][]interface{} `json:"valores"`
	NodosNoDisponibles []string        `json:"nodos_no_disponibles,omitempty"`
}

// ConsultaUltimoRequest solicitud de consulta de último punto
type ConsultaUltimoRequest struct {
	Serie        string `json:"serie"`
	TiempoInicio *int64 `json:"tiempo_inicio,omitempty"` // Unix nanosegundos, opcional
	TiempoFin    *int64 `json:"tiempo_fin,omitempty"`    // Unix nanosegundos, opcional
}

// ConsultaUltimoResponse respuesta de consulta de último punto
type ConsultaUltimoResponse struct {
	Series             []string      `json:"series"`
	Tiempos            []int64       `json:"tiempos"`
	Valores            []interface{} `json:"valores"`
	NodosNoDisponibles []string      `json:"nodos_no_disponibles,omitempty"`
}

// ConsultaAgregacionRequest solicitud de consulta de agregación
type ConsultaAgregacionRequest struct {
	Serie        string   `json:"serie"`
	TiempoInicio int64    `json:"tiempo_inicio"` // Unix nanosegundos
	TiempoFin    int64    `json:"tiempo_fin"`    // Unix nanosegundos
	Agregaciones []string `json:"agregaciones"`  // "promedio", "maximo", "minimo", "suma", "count"
}

// ConsultaAgregacionResponse respuesta de consulta de agregación
type ConsultaAgregacionResponse struct {
	Series             []string    `json:"series"`
	Agregaciones       []string    `json:"agregaciones"`
	Valores            [][]float64 `json:"valores"` // [agregacion][serie]
	NodosNoDisponibles []string    `json:"nodos_no_disponibles,omitempty"`
}

// ConsultaAgregacionTemporalRequest solicitud de consulta de agregación temporal
type ConsultaAgregacionTemporalRequest struct {
	Serie        string   `json:"serie"`
	TiempoInicio int64    `json:"tiempo_inicio"` // Unix nanosegundos
	TiempoFin    int64    `json:"tiempo_fin"`    // Unix nanosegundos
	Agregaciones []string `json:"agregaciones"`  // "promedio", "maximo", "minimo", "suma", "count"
	Intervalo    int64    `json:"intervalo"`     // Duration en nanosegundos
}

// ConsultaAgregacionTemporalResponse respuesta de consulta de agregación temporal
type ConsultaAgregacionTemporalResponse struct {
	Series             []string        `json:"series"`
	Tiempos            []int64         `json:"tiempos"`
	Agregaciones       []string        `json:"agregaciones"`
	Valores            [][][]FloatNulo `json:"valores"` // [agregacion][bucket][serie]
	NodosNoDisponibles []string        `json:"nodos_no_disponibles,omitempty"`
}
