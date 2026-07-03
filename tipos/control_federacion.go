package tipos

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	PrefijoControl = "swctl/"

	// Tópicos de consulta (nube -> borde)
	TopicoConsultaSolicitud = "swctl/nodos/%s/consulta/solicitud/%s"
	TopicoConsultaCancelar  = "swctl/nodos/%s/consulta/cancelar/%s"

	// Tópicos de respuesta (borde -> nube)
	TopicoConsultaParte = "swctl/consultas/%s/parte/%d"
	TopicoConsultaFin   = "swctl/consultas/%s/fin"
	TopicoConsultaError = "swctl/consultas/%s/error"

	// Tópicos de comando (nube -> borde)
	TopicoComandoSolicitud = "swctl/nodos/%s/comando/solicitud/%s"

	// Tópicos de respuesta de comando (borde -> nube)
	TopicoComandoFin   = "swctl/comandos/%s/fin"
	TopicoComandoError = "swctl/comandos/%s/error"

	// Tópicos de soporte (borde -> nube)
	TopicoLatido       = "swctl/nodos/%s/latido"
	TopicoCapacidades  = "swctl/nodos/%s/capacidades"
	TopicoWatermarks   = "swctl/nodos/%s/watermarks"
)

// EsTopicoControl determina si un tópico pertenece al plano de control
func EsTopicoControl(topico string) bool {
	return strings.HasPrefix(topico, PrefijoControl)
}

// ============================================================================
// SOLICITUD DE CONSULTA
// ============================================================================

type TipoConsulta string

const (
	ConsultaRango             TipoConsulta = "rango"
	ConsultaUltimo            TipoConsulta = "ultimo"
	ConsultaAgregacion        TipoConsulta = "agregacion"
	ConsultaAgregacionTemporal TipoConsulta = "agregacion_temporal"
)

// SolicitudControlConsulta representa una solicitud de consulta federada
type SolicitudControlConsulta struct {
	Version        int           `json:"version"`
	IDConsulta     string        `json:"id_consulta"`
	IDNodo         string        `json:"id_nodo"`
	TipoConsulta   TipoConsulta  `json:"tipo_consulta"`
	TiempoEsperaMs int           `json:"tiempo_espera_ms"`
	Argumentos     ConsultaArgs  `json:"argumentos"`
}

// ConsultaArgs contiene los argumentos específicos de cada tipo de consulta
type ConsultaArgs struct {
	Serie        string             `json:"serie,omitempty"`
	TiempoInicio int64              `json:"tiempo_inicio,omitempty"`
	TiempoFin    int64              `json:"tiempo_fin,omitempty"`
	Agregaciones []TipoAgregacion   `json:"agregaciones,omitempty"`
	Intervalo    int64              `json:"intervalo,omitempty"`
	// Para consulta de último punto
	TiempoInicioPtr *int64 `json:"tiempo_inicio_ptr,omitempty"`
	TiempoFinPtr    *int64 `json:"tiempo_fin_ptr,omitempty"`
}

// ============================================================================
// RESPUESTAS DE CONSULTA
// ============================================================================

// RespuestaControlConsultaParte representa una parte del resultado
type RespuestaControlConsultaParte struct {
	Version      int    `json:"version"`
	IDConsulta   string `json:"id_consulta"`
	IDNodo       string `json:"id_nodo"`
	IndiceParte  int    `json:"indice_parte"`
	EsUltimaParte bool  `json:"es_ultima_parte"`
	Resultado    json.RawMessage `json:"resultado"`
}

// RespuestaControlConsultaFin indica que la consulta terminó
type RespuestaControlConsultaFin struct {
	Version   int    `json:"version"`
	IDConsulta string `json:"id_consulta"`
	IDNodo    string `json:"id_nodo"`
	Estado    string `json:"estado"`
	Partes    int    `json:"partes"`
	Parcial   bool   `json:"parcial"`
}

// RespuestaControlConsultaError indica que la consulta falló
type RespuestaControlConsultaError struct {
	Version    int    `json:"version"`
	IDConsulta string `json:"id_consulta"`
	IDNodo     string `json:"id_nodo"`
	Codigo     string `json:"codigo"`
	Mensaje    string `json:"mensaje"`
}

// ============================================================================
// COMANDOS
// ============================================================================

type TipoOperacion string

const (
	OpSerieCrear      TipoOperacion = "serie.crear"
	OpReglaCrear      TipoOperacion = "regla.crear"
	OpReglaActualizar TipoOperacion = "regla.actualizar"
	OpReglaEliminar   TipoOperacion = "regla.eliminar"
	OpDatoInsertar    TipoOperacion = "dato.insertar"
)

// SolicitudControlComando representa un comando de la nube al borde
type SolicitudControlComando struct {
	Version         int             `json:"version"`
	IDComando       string          `json:"id_comando"`
	IDNodo          string          `json:"id_nodo"`
	Operacion       TipoOperacion   `json:"operacion"`
	ClaveIdempotencia string        `json:"clave_idempotencia,omitempty"`
	Argumentos      ComandoArgs     `json:"argumentos"`
}

// ComandoArgs contiene los argumentos específicos de cada operación
type ComandoArgs struct {
	// Para serie.crear
	Serie *Serie `json:"serie,omitempty"`

	// Para regla.*
	ReglaID string                 `json:"regla_id,omitempty"`
	Regla   map[string]interface{} `json:"regla,omitempty"`

	// Para dato.insertar
	Path      string      `json:"path,omitempty"`
	Timestamp int64       `json:"timestamp,omitempty"`
	Valor     interface{} `json:"valor,omitempty"`
}

// RespuestaControlComandoFin indica que el comando terminó
type RespuestaControlComandoFin struct {
	Version   int         `json:"version"`
	IDComando string      `json:"id_comando"`
	IDNodo    string      `json:"id_nodo"`
	Estado    string      `json:"estado"`
	Resultado interface{} `json:"resultado,omitempty"`
}

// RespuestaControlComandoError indica que el comando falló
type RespuestaControlComandoError struct {
	Version   int    `json:"version"`
	IDComando string `json:"id_comando"`
	IDNodo    string `json:"id_nodo"`
	Codigo    string `json:"codigo"`
	Mensaje   string `json:"mensaje"`
}

// ============================================================================
// LATIDO Y CAPACIDADES
// ============================================================================

// Latido representa un heartbeat del borde
type Latido struct {
	Version    int    `json:"version"`
	IDNodo     string `json:"id_nodo"`
	Timestamp  int64  `json:"timestamp"`
	NumSeries  int    `json:"num_series"`
	NumReglas  int    `json:"num_reglas"`
	Estado     string `json:"estado"`
}

// Capacidades indica qué operaciones soporta el nodo
type Capacidades struct {
	Version     int      `json:"version"`
	IDNodo      string   `json:"id_nodo"`
	Consultas   []string `json:"consultas"`
	Comandos    []string `json:"comandos"`
	VersionBorde string  `json:"version_borde,omitempty"`
}

// ============================================================================
// HELPERS DE TÓPICOS
// ============================================================================

// ConstruirTopicoConsultaSolicitud devuelve el tópico de solicitud de consulta
func ConstruirTopicoConsultaSolicitud(idNodo, idConsulta string) string {
	return fmt.Sprintf(TopicoConsultaSolicitud, idNodo, idConsulta)
}

// ConstruirTopicoConsultaParte devuelve el tópico de parte de respuesta
func ConstruirTopicoConsultaParte(idConsulta string, indice int) string {
	return fmt.Sprintf(TopicoConsultaParte, idConsulta, indice)
}

// ConstruirTopicoConsultaFin devuelve el tópico de fin de consulta
func ConstruirTopicoConsultaFin(idConsulta string) string {
	return fmt.Sprintf(TopicoConsultaFin, idConsulta)
}

// ConstruirTopicoConsultaError devuelve el tópico de error de consulta
func ConstruirTopicoConsultaError(idConsulta string) string {
	return fmt.Sprintf(TopicoConsultaError, idConsulta)
}

// ConstruirTopicoLatido devuelve el tópico de latido
func ConstruirTopicoLatido(idNodo string) string {
	return fmt.Sprintf(TopicoLatido, idNodo)
}

// ConstruirTopicoCapacidades devuelve el tópico de capacidades
func ConstruirTopicoCapacidades(idNodo string) string {
	return fmt.Sprintf(TopicoCapacidades, idNodo)
}
