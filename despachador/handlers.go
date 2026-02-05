package despachador

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/sensorwave-dev/sensorwave/tipos"
)

// FloatNulo es un alias para tipos.FloatNulo para uso en este paquete
type FloatNulo = tipos.FloatNulo

// ============================================================================
// HANDLERS EXPORTADOS - API REST del despachador
// Cada handler recibe el manager y retorna un http.HandlerFunc
// ============================================================================

// HandlerStatus retorna el estado actual del despachador (nodos y series)
func HandlerStatus(manager *ManagerDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats := manager.ObtenerEstadisticas()
		respuesta := StatusResponse{
			NumNodos:  stats.NumNodos,
			NumSeries: stats.NumSeries,
		}
		EnviarJSON(w, respuesta)
	}
}

// HandlerListarNodos lista todos los nodos registrados
func HandlerListarNodos(manager *ManagerDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodos := manager.ListarNodos()
		EnviarJSON(w, nodos)
	}
}

// HandlerListarSeries lista series según patrón de búsqueda
// Query param: ?patron=* (opcional, default: "*")
func HandlerListarSeries(manager *ManagerDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		patron := r.URL.Query().Get("patron")
		if patron == "" {
			patron = "*"
		}

		series := manager.ListarSeries(patron)

		// Convertir a formato JSON
		respuesta := make([]SerieResponse, len(series))
		for i, si := range series {
			respuesta[i] = serieToResponse(si)
		}

		EnviarJSON(w, respuesta)
	}
}

// HandlerObtenerSerie obtiene información de una serie específica por path
// Path param: /api/series/{path...}
func HandlerObtenerSerie(manager *ManagerDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.PathValue("path")
		if path == "" {
			EnviarError(w, http.StatusBadRequest, "path de serie requerido")
			return
		}

		serie := manager.ObtenerSerie(path)
		if serie == nil {
			EnviarError(w, http.StatusNotFound, fmt.Sprintf("serie '%s' no encontrada", path))
			return
		}

		EnviarJSON(w, serieToResponse(*serie))
	}
}

// HandlerConsultarRango consulta datos de una serie en un rango de tiempo
// POST /api/consulta/rango
// Body: {"serie": "...", "tiempo_inicio": nanos, "tiempo_fin": nanos}
func HandlerConsultarRango(manager *ManagerDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ConsultaRangoRequest
		if err := LeerJSON(r, &req); err != nil {
			EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if req.Serie == "" {
			EnviarError(w, http.StatusBadRequest, "serie requerida")
			return
		}

		tiempoInicio := time.Unix(0, req.TiempoInicio)
		tiempoFin := time.Unix(0, req.TiempoFin)

		resultado, err := manager.ConsultarRango(req.Serie, tiempoInicio, tiempoFin)
		if err != nil {
			EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		respuesta := ConsultaRangoResponse{
			Series:             resultado.Series,
			Tiempos:            resultado.Tiempos,
			Valores:            resultado.Valores,
			NodosNoDisponibles: resultado.NodosNoDisponibles,
		}

		EnviarJSON(w, respuesta)
	}
}

// HandlerConsultarUltimo consulta el último punto de una serie
// POST /api/consulta/ultimo
// Body: {"serie": "...", "tiempo_inicio": nanos (opc), "tiempo_fin": nanos (opc)}
func HandlerConsultarUltimo(manager *ManagerDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ConsultaUltimoRequest
		if err := LeerJSON(r, &req); err != nil {
			EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if req.Serie == "" {
			EnviarError(w, http.StatusBadRequest, "serie requerida")
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

		resultado, err := manager.ConsultarUltimoPunto(req.Serie, tiempoInicio, tiempoFin)
		if err != nil {
			EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		respuesta := ConsultaUltimoResponse{
			Series:             resultado.Series,
			Tiempos:            resultado.Tiempos,
			Valores:            resultado.Valores,
			NodosNoDisponibles: resultado.NodosNoDisponibles,
		}

		EnviarJSON(w, respuesta)
	}
}

// HandlerConsultarAgregacion consulta agregaciones de una serie
// POST /api/consulta/agregacion
// Body: {"serie": "...", "tiempo_inicio": nanos, "tiempo_fin": nanos, "agregaciones": ["promedio", "maximo"]}
func HandlerConsultarAgregacion(manager *ManagerDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ConsultaAgregacionRequest
		if err := LeerJSON(r, &req); err != nil {
			EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if req.Serie == "" {
			EnviarError(w, http.StatusBadRequest, "serie requerida")
			return
		}
		if len(req.Agregaciones) == 0 {
			EnviarError(w, http.StatusBadRequest, "debe especificar al menos una agregación")
			return
		}

		// Convertir strings a TipoAgregacion
		agregaciones := make([]tipos.TipoAgregacion, len(req.Agregaciones))
		for i, a := range req.Agregaciones {
			agregaciones[i] = tipos.TipoAgregacion(a)
		}

		tiempoInicio := time.Unix(0, req.TiempoInicio)
		tiempoFin := time.Unix(0, req.TiempoFin)

		resultado, err := manager.ConsultarAgregacion(req.Serie, tiempoInicio, tiempoFin, agregaciones)
		if err != nil {
			EnviarError(w, http.StatusInternalServerError, err.Error())
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

		EnviarJSON(w, respuesta)
	}
}

// HandlerConsultarAgregacionTemporal consulta agregaciones temporales (downsampling)
// POST /api/consulta/agregacion-temporal
// Body: {"serie": "...", "tiempo_inicio": nanos, "tiempo_fin": nanos, "agregaciones": [...], "intervalo": nanos}
func HandlerConsultarAgregacionTemporal(manager *ManagerDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ConsultaAgregacionTemporalRequest
		if err := LeerJSON(r, &req); err != nil {
			EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if req.Serie == "" {
			EnviarError(w, http.StatusBadRequest, "serie requerida")
			return
		}
		if len(req.Agregaciones) == 0 {
			EnviarError(w, http.StatusBadRequest, "debe especificar al menos una agregación")
			return
		}
		if req.Intervalo <= 0 {
			EnviarError(w, http.StatusBadRequest, "intervalo debe ser mayor a cero")
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

		resultado, err := manager.ConsultarAgregacionTemporal(req.Serie, tiempoInicio, tiempoFin, agregaciones, intervalo)
		if err != nil {
			EnviarError(w, http.StatusInternalServerError, err.Error())
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

		EnviarJSON(w, respuesta)
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
func HandlerListarReglas(manager *ManagerDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodoFiltro := r.URL.Query().Get("nodo")
		soloActivas := r.URL.Query().Get("activas") == "true"

		reglas := manager.ListarReglas(nodoFiltro, soloActivas)

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

		EnviarJSON(w, respuesta)
	}
}

// HandlerObtenerRegla obtiene una regla específica por ID
// Path param: /api/reglas/{id}
func HandlerObtenerRegla(manager *ManagerDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			EnviarError(w, http.StatusBadRequest, "id de regla requerido")
			return
		}

		regla, nodoID, encontrada := manager.ObtenerRegla(id)
		if !encontrada {
			EnviarError(w, http.StatusNotFound, fmt.Sprintf("regla '%s' no encontrada", id))
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

		EnviarJSON(w, respuesta)
	}
}

// HandlerListarReglasPorNodo lista reglas de un nodo específico
// Path param: /api/nodos/{nodoID}/reglas
func HandlerListarReglasPorNodo(manager *ManagerDespachador) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodoID := r.PathValue("nodoID")
		if nodoID == "" {
			EnviarError(w, http.StatusBadRequest, "nodoID requerido")
			return
		}

		reglas := manager.ListarReglasPorNodo(nodoID)

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

		EnviarJSON(w, respuesta)
	}
}

// ============================================================================
// HELPERS EXPORTADOS - Utilidades HTTP reutilizables
// ============================================================================

// EnviarJSON envía una respuesta JSON con Content-Type application/json
func EnviarJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Error encodificando JSON: %v", err)
	}
}

// EnviarError envía una respuesta de error en formato JSON
func EnviarError(w http.ResponseWriter, codigo int, mensaje string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(codigo)
	json.NewEncoder(w).Encode(map[string]string{"error": mensaje})
}

// LeerJSON lee y parsea el body de una request como JSON
func LeerJSON(r *http.Request, v interface{}) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("error leyendo body: %v", err)
	}
	defer r.Body.Close()

	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("error parseando JSON: %v", err)
	}
	return nil
}

// serieToResponse convierte SerieInfo a SerieResponse
func serieToResponse(si SerieInfo) SerieResponse {
	return SerieResponse{
		Path:                 si.Path,
		NodoID:               si.NodoID,
		TipoDatos:            si.TipoDatos.String(),
		Tags:                 si.Tags,
		TamanoBloque:         si.TamañoBloque,
		TiempoAlmacenamiento: si.TiempoAlmacenamiento,
		CompresionBytes:      string(si.CompresionBytes),
		CompresionBloque:     string(si.CompresionBloque),
	}
}
