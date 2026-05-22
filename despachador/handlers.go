package despachador

import (
	"fmt"
	"net/http"
	"time"

	"github.com/sensorwave-dev/sensorwave/tipos"
)

// FloatNulo es un alias para tipos.FloatNulo para uso en este paquete
type FloatNulo = tipos.FloatNulo

// ============================================================================
// HANDLERS EXPORTADOS - API REST del despachador
// Cada handler recibe el gestor y retorna un http.HandlerFunc
// ============================================================================

// HandlerStatus retorna el estado actual del despachador (nodos y series)
func HandlerStatus(gestor *GestorDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats := gestor.ObtenerEstadisticas()
		respuesta := StatusResponse{
			NumNodos:  stats.NumNodos,
			NumSeries: stats.NumSeries,
		}
		tipos.EnviarJSON(w, respuesta)
	}
}

// HandlerListarNodos lista todos los nodos registrados
func HandlerListarNodos(gestor *GestorDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodos := gestor.ListarNodos()
		tipos.EnviarJSON(w, nodos)
	}
}

// HandlerListarSeries lista series según patrón de búsqueda
// Query param: ?patron=* (opcional, default: "*")
func HandlerListarSeries(gestor *GestorDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		patron := r.URL.Query().Get("patron")
		if patron == "" {
			patron = "*"
		}

		series := gestor.ListarSeries(patron)

		// Convertir a formato JSON
		respuesta := make([]SerieResponse, len(series))
		for i, si := range series {
			respuesta[i] = serieToResponse(si)
		}

		tipos.EnviarJSON(w, respuesta)
	}
}

// HandlerObtenerSerie obtiene información de una serie específica por path
// Path param: /api/series/{path...}
func HandlerObtenerSerie(gestor *GestorDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.PathValue("path")
		if path == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "path de serie requerido")
			return
		}

		serie := gestor.ObtenerSerie(path)
		if serie == nil {
			tipos.EnviarError(w, http.StatusNotFound, fmt.Sprintf("serie '%s' no encontrada", path))
			return
		}

		tipos.EnviarJSON(w, serieToResponse(*serie))
	}
}

// HandlerConsultarRango consulta datos de una serie en un rango de tiempo
// POST /api/consulta/rango
// Body: {"serie": "...", "tiempo_inicio": nanos, "tiempo_fin": nanos}
func HandlerConsultarRango(gestor *GestorDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ConsultaRangoRequest
		if err := tipos.LeerJSON(r, &req); err != nil {
			tipos.EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if req.Serie == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "serie requerida")
			return
		}

		tiempoInicio := time.Unix(0, req.TiempoInicio)
		tiempoFin := time.Unix(0, req.TiempoFin)

		resultado, err := gestor.ConsultarRango(req.Serie, tiempoInicio, tiempoFin)
		if err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		respuesta := ConsultaRangoResponse{
			Series:             resultado.Series,
			Tiempos:            resultado.Tiempos,
			Valores:            resultado.Valores,
			NodosNoDisponibles: resultado.NodosNoDisponibles,
		}

		tipos.EnviarJSON(w, respuesta)
	}
}

// HandlerConsultarUltimo consulta el último punto de una serie
// POST /api/consulta/ultimo
// Body: {"serie": "...", "tiempo_inicio": nanos (opc), "tiempo_fin": nanos (opc)}
func HandlerConsultarUltimo(gestor *GestorDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ConsultaUltimoRequest
		if err := tipos.LeerJSON(r, &req); err != nil {
			tipos.EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if req.Serie == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "serie requerida")
			return
		}

		var tiempoInicio, tiempoFin *time.Time
		if req.TiempoInicio != nil {
			t := time.Unix(0, *req.TiempoInicio)
			tiempoInicio = &t
		}
		if req.TiempoFin != nil {
			t := time.Unix(0, *req.TiempoFin)
			tiempoFin = &t
		}

		resultado, err := gestor.ConsultarUltimoPunto(req.Serie, tiempoInicio, tiempoFin)
		if err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		respuesta := ConsultaUltimoResponse{
			Series:             resultado.Series,
			Tiempos:            resultado.Tiempos,
			Valores:            resultado.Valores,
			NodosNoDisponibles: resultado.NodosNoDisponibles,
		}

		tipos.EnviarJSON(w, respuesta)
	}
}

// HandlerConsultarAgregacion consulta agregaciones de una serie
// POST /api/consulta/agregacion
// Body: {"serie": "...", "tiempo_inicio": nanos, "tiempo_fin": nanos, "agregaciones": ["promedio", "maximo"]}
func HandlerConsultarAgregacion(gestor *GestorDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ConsultaAgregacionRequest
		if err := tipos.LeerJSON(r, &req); err != nil {
			tipos.EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if req.Serie == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "serie requerida")
			return
		}
		if len(req.Agregaciones) == 0 {
			tipos.EnviarError(w, http.StatusBadRequest, "debe especificar al menos una agregación")
			return
		}

		// Convertir strings a TipoAgregacion
		agregaciones := make([]tipos.TipoAgregacion, len(req.Agregaciones))
		for i, a := range req.Agregaciones {
			agregaciones[i] = tipos.TipoAgregacion(a)
		}

		tiempoInicio := time.Unix(0, req.TiempoInicio)
		tiempoFin := time.Unix(0, req.TiempoFin)

		resultado, err := gestor.ConsultarAgregacion(req.Serie, tiempoInicio, tiempoFin, agregaciones)
		if err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Convertir TipoAgregacion a strings
		agregacionesStr := make([]string, len(resultado.Agregaciones))
		for i, a := range resultado.Agregaciones {
			agregacionesStr[i] = string(a)
		}

		respuesta := ConsultaAgregacionResponse{
			Series:             resultado.Series,
			Agregaciones:       agregacionesStr,
			Valores:            resultado.Valores,
			NodosNoDisponibles: resultado.NodosNoDisponibles,
		}

		tipos.EnviarJSON(w, respuesta)
	}
}

// HandlerConsultarAgregacionTemporal consulta agregaciones temporales (downsampling)
// POST /api/consulta/agregacion-temporal
// Body: {"serie": "...", "tiempo_inicio": nanos, "tiempo_fin": nanos, "agregaciones": [...], "intervalo": nanos}
func HandlerConsultarAgregacionTemporal(gestor *GestorDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ConsultaAgregacionTemporalRequest
		if err := tipos.LeerJSON(r, &req); err != nil {
			tipos.EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if req.Serie == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "serie requerida")
			return
		}
		if len(req.Agregaciones) == 0 {
			tipos.EnviarError(w, http.StatusBadRequest, "debe especificar al menos una agregación")
			return
		}
		if req.Intervalo <= 0 {
			tipos.EnviarError(w, http.StatusBadRequest, "intervalo debe ser mayor a cero")
			return
		}

		// Convertir strings a TipoAgregacion
		agregaciones := make([]tipos.TipoAgregacion, len(req.Agregaciones))
		for i, a := range req.Agregaciones {
			agregaciones[i] = tipos.TipoAgregacion(a)
		}

		tiempoInicio := time.Unix(0, req.TiempoInicio)
		tiempoFin := time.Unix(0, req.TiempoFin)
		intervalo := time.Duration(req.Intervalo)

		resultado, err := gestor.ConsultarAgregacionTemporal(req.Serie, tiempoInicio, tiempoFin, agregaciones, intervalo)
		if err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Convertir TipoAgregacion a strings
		agregacionesStr := make([]string, len(resultado.Agregaciones))
		for i, a := range resultado.Agregaciones {
			agregacionesStr[i] = string(a)
		}

		// Convertir [][][]float64 a [][][]FloatNulo para serializar NaN como null en JSON
		valoresFloatNulo := make([][][]FloatNulo, len(resultado.Valores))
		for i, agregacion := range resultado.Valores {
			valoresFloatNulo[i] = make([][]FloatNulo, len(agregacion))
			for j, bucket := range agregacion {
				valoresFloatNulo[i][j] = make([]FloatNulo, len(bucket))
				for k, valor := range bucket {
					valoresFloatNulo[i][j][k] = FloatNulo(valor)
				}
			}
		}

		respuesta := ConsultaAgregacionTemporalResponse{
			Series:             resultado.Series,
			Tiempos:            resultado.Tiempos,
			Agregaciones:       agregacionesStr,
			Valores:            valoresFloatNulo,
			NodosNoDisponibles: resultado.NodosNoDisponibles,
		}

		tipos.EnviarJSON(w, respuesta)
	}
}

// ============================================================================
// HANDLERS DE REGLAS
// ============================================================================

// ReglaResponse es la respuesta JSON para una regla
type ReglaResponse struct {
	ID          string            `json:"id"`
	Nombre      string            `json:"nombre"`
	Activa      bool              `json:"activa"`
	Logica      string            `json:"logica"`
	NodoID      string            `json:"nodo_id"`
	Condiciones []tipos.Condicion `json:"condiciones"`
	Acciones    []tipos.Accion    `json:"acciones"`
}

// HandlerListarReglas lista todas las reglas de todos los nodos
// Query param: ?nodo=xxx (opcional, filtra por nodo específico)
// Query param: ?activas=true (opcional, solo reglas activas)
func HandlerListarReglas(gestor *GestorDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodoFiltro := r.URL.Query().Get("nodo")
		soloActivas := r.URL.Query().Get("activas") == "true"

		reglas := gestor.ListarReglas(nodoFiltro, soloActivas)

		// Convertir a formato de respuesta
		respuesta := make([]ReglaResponse, len(reglas))
		for i, reglaInfo := range reglas {
			respuesta[i] = ReglaResponse{
				ID:          reglaInfo.ID,
				Nombre:      reglaInfo.Nombre,
				Activa:      reglaInfo.Activa,
				Logica:      reglaInfo.Logica,
				NodoID:      reglaInfo.NodoID,
				Condiciones: reglaInfo.Condiciones,
				Acciones:    reglaInfo.Acciones,
			}
		}

		tipos.EnviarJSON(w, respuesta)
	}
}

// HandlerObtenerRegla obtiene una regla específica por ID
// Path param: /api/reglas/{id}
func HandlerObtenerRegla(gestor *GestorDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "id de regla requerido")
			return
		}

		regla, nodoID, encontrada := gestor.ObtenerRegla(id)
		if !encontrada {
			tipos.EnviarError(w, http.StatusNotFound, fmt.Sprintf("regla '%s' no encontrada", id))
			return
		}

		respuesta := ReglaResponse{
			ID:          regla.ID,
			Nombre:      regla.Nombre,
			Activa:      regla.Activa,
			Logica:      regla.Logica,
			NodoID:      nodoID,
			Condiciones: regla.Condiciones,
			Acciones:    regla.Acciones,
		}

		tipos.EnviarJSON(w, respuesta)
	}
}

// HandlerListarReglasPorNodo lista reglas de un nodo específico
// Path param: /api/nodos/{nodoID}/reglas
func HandlerListarReglasPorNodo(gestor *GestorDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodoID := r.PathValue("nodoID")
		if nodoID == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "nodoID requerido")
			return
		}

		reglas := gestor.ListarReglasPorNodo(nodoID)

		respuesta := make([]ReglaResponse, len(reglas))
		for i, regla := range reglas {
			respuesta[i] = ReglaResponse{
				ID:          regla.ID,
				Nombre:      regla.Nombre,
				Activa:      regla.Activa,
				Logica:      regla.Logica,
				NodoID:      nodoID,
				Condiciones: regla.Condiciones,
				Acciones:    regla.Acciones,
			}
		}

		tipos.EnviarJSON(w, respuesta)
	}
}

// serieToResponse convierte SerieInfo a SerieResponse
func serieToResponse(si SerieInfo) SerieResponse {
	return SerieResponse{
		Path:                 si.Path,
		NodoID:               si.NodoID,
		TipoDatos:            si.TipoDatos,
		Tags:                 si.Tags,
		TamanoBloque:         si.TamañoBloque,
		TiempoAlmacenamiento: si.TiempoAlmacenamiento,
		CompresionBytes:      si.CompresionBytes,
		CompresionBloque:     si.CompresionBloque,
	}
}
