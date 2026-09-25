package tipos

import (
	"encoding/json"
	"fmt"
)

const (
	// Tópicos de consulta (nube -> borde)
	TopicoConsultaSolicitud = "swctl/nodos/%s/consulta/solicitud/%s"
	TopicoConsultaCancelar  = "swctl/nodos/%s/consulta/cancelar/%s"

	// Tópicos de respuesta (borde -> despachador específico)
	// Permiten N réplicas de despachador sin compartir respuestas.
	TopicoConsultaParte = "swctl/despachadores/%s/consultas/%s/parte/%d"
	TopicoConsultaError = "swctl/despachadores/%s/consultas/%s/error"
)

// ============================================================================
// SOLICITUD DE CONSULTA
// ============================================================================

type TipoConsulta string

const (
	ConsultaRango              TipoConsulta = "rango"
	ConsultaUltimo             TipoConsulta = "ultimo"
	ConsultaAgregacion         TipoConsulta = "agregacion"
	ConsultaAgregacionTemporal TipoConsulta = "agregacion_temporal"
)

// SolicitudControlConsulta representa una solicitud de consulta federada
type SolicitudControlConsulta struct {
	Version        int          `json:"version"`
	IDConsulta     string       `json:"id_consulta"`
	IDNodo         string       `json:"id_nodo"`
	IDDespachador  string       `json:"id_despachador"` // réplica que debe recibir la respuesta
	TipoConsulta   TipoConsulta `json:"tipo_consulta"`
	TiempoEsperaMs int          `json:"tiempo_espera_ms"`
	Argumentos     ConsultaArgs `json:"argumentos"`
}

// ConsultaArgs contiene los argumentos específicos de cada tipo de consulta
type ConsultaArgs struct {
	Serie        string           `json:"serie,omitempty"`
	TiempoInicio int64            `json:"tiempo_inicio,omitempty"`
	TiempoFin    int64            `json:"tiempo_fin,omitempty"`
	Agregaciones []TipoAgregacion `json:"agregaciones,omitempty"`
	Intervalo    int64            `json:"intervalo,omitempty"`
	// Para consulta de último punto
	TiempoInicioPtr *int64 `json:"tiempo_inicio_ptr,omitempty"`
	TiempoFinPtr    *int64 `json:"tiempo_fin_ptr,omitempty"`
}

// ============================================================================
// RESPUESTAS DE CONSULTA
// ============================================================================

// RespuestaControlConsultaParte representa una parte del resultado
type RespuestaControlConsultaParte struct {
	Version       int             `json:"version"`
	IDConsulta    string          `json:"id_consulta"`
	IDNodo        string          `json:"id_nodo"`
	IndiceParte   int             `json:"indice_parte"`
	EsUltimaParte bool            `json:"es_ultima_parte"`
	Resultado     json.RawMessage `json:"resultado"`
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
// HELPERS DE TÓPICOS
// ============================================================================

// ConstruirTopicoConsultaSolicitud devuelve el tópico de solicitud de consulta
func ConstruirTopicoConsultaSolicitud(idNodo, idConsulta string) string {
	return fmt.Sprintf(TopicoConsultaSolicitud, idNodo, idConsulta)
}

// ConstruirTopicoConsultaCancelar devuelve el tópico de cancelación de consulta
func ConstruirTopicoConsultaCancelar(idNodo, idConsulta string) string {
	return fmt.Sprintf(TopicoConsultaCancelar, idNodo, idConsulta)
}

// ConstruirTopicoRespuestasDespachador devuelve el filtro de suscripción de una réplica
func ConstruirTopicoRespuestasDespachador(idDespachador string) string {
	return fmt.Sprintf("swctl/despachadores/%s/consultas/#", idDespachador)
}

// ConstruirTopicoConsultaParte devuelve el tópico de parte de respuesta
func ConstruirTopicoConsultaParte(idDespachador, idConsulta string, indice int) string {
	return fmt.Sprintf(TopicoConsultaParte, idDespachador, idConsulta, indice)
}

// ConstruirTopicoConsultaError devuelve el tópico de error de consulta
func ConstruirTopicoConsultaError(idDespachador, idConsulta string) string {
	return fmt.Sprintf(TopicoConsultaError, idDespachador, idConsulta)
}
