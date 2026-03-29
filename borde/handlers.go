package borde

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sensorwave-dev/sensorwave/tipos"
)

// ============================================================================
// HANDLERS EXPORTADOS - API REST del borde
// Cada handler recibe el gestor y retorna un http.HandlerFunc
// ============================================================================

// HandlerStatus retorna el estado actual del borde (series y reglas)
func HandlerStatus(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		series, _ := gestor.ListarSeries()
		reglas := gestor.ListarReglas()
		estadoMotor := gestor.ObtenerEstadoMotorReglas()

		respuesta := map[string]interface{}{
			"nodo_id":       gestor.ObtenerNodoID(),
			"series_count":  len(series),
			"rules_count":   len(reglas),
			"rules_enabled": estadoMotor.Habilitado,
			"rules_active":  estadoMotor.ReglasActivas,
		}

		if tags := gestor.ObtenerTags(); len(tags) > 0 {
			respuesta["tags"] = tags
		}

		tipos.EnviarJSON(w, respuesta)
	}
}

// HandlerListarSeries lista todas las series del nodo
func HandlerListarSeries(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		patron := r.URL.Query().Get("patron")
		if patron == "" {
			patron = "*"
		}

		var series []tipos.Serie
		var err error

		if patron == "*" {
			paths, err := gestor.ListarSeries()
			if err != nil {
				tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
				return
			}
			for _, p := range paths {
				s, err := gestor.ObtenerSeries(p)
				if err == nil {
					series = append(series, s)
				}
			}
		} else {
			series, err = gestor.ListarSeriesPorPath(patron)
			if err != nil {
				tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		tipos.EnviarJSON(w, series)
	}
}

// HandlerObtenerSerie obtiene información de una serie específica por path
func HandlerObtenerSerie(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.PathValue("path")
		if path == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "path de serie requerido")
			return
		}

		serie, err := gestor.ObtenerSeries(path)
		if err != nil {
			tipos.EnviarError(w, http.StatusNotFound, fmt.Sprintf("serie '%s' no encontrada", path))
			return
		}

		tipos.EnviarJSON(w, serie)
	}
}

// HandlerCrearSerie crea una nueva serie
func HandlerCrearSerie(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		var req struct {
			Path             string            `json:"path"`
			Tipo             string            `json:"tipo"`
			TamañoBloque     int               `json:"tamano_bloque,omitempty"`
			CompresionBytes  string            `json:"compresion_bytes,omitempty"`
			CompresionBloque string            `json:"compresion_bloque,omitempty"`
			Tags             map[string]string `json:"tags,omitempty"`
		}

		if err := tipos.LeerJSON(r, &req); err != nil {
			tipos.EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if req.Path == "" || req.Tipo == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "se requiere path y tipo")
			return
		}

		// Crear serie con defaults
		serie := tipos.Serie{
			Path:             req.Path,
			TipoDatos:        parsearTipoDatos(req.Tipo),
			TamañoBloque:     100,
			Tags:             req.Tags,
			CompresionBytes:  tipos.TipoCompresion(req.CompresionBytes),
			CompresionBloque: tipos.TipoCompresionBloque(req.CompresionBloque),
		}

		if req.TamañoBloque > 0 {
			serie.TamañoBloque = req.TamañoBloque
		}
		if serie.CompresionBytes == "" {
			serie.CompresionBytes = tipos.SinCompresion
		}
		if serie.CompresionBloque == "" {
			serie.CompresionBloque = tipos.LZ4
		}

		if err := gestor.CrearSerie(serie); err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		tipos.EnviarJSON(w, map[string]interface{}{
			"exito":   true,
			"mensaje": fmt.Sprintf("Serie %s creada correctamente", req.Path),
		})
	}
}

// HandlerEliminarSerie elimina una serie
func HandlerEliminarSerie(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		path := r.PathValue("path")
		if path == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "path de serie requerido")
			return
		}

		if err := gestor.EliminarSerie(path); err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		tipos.EnviarJSON(w, map[string]interface{}{
			"exito":   true,
			"mensaje": fmt.Sprintf("Serie %s eliminada", path),
		})
	}
}

// HandlerInsertar inserta un dato en una serie
func HandlerInsertar(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		var req struct {
			Path        string      `json:"path"`
			Valor       interface{} `json:"valor"`
			MarcaTiempo int64       `json:"marca_tiempo,omitempty"`
		}

		if err := tipos.LeerJSON(r, &req); err != nil {
			tipos.EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if req.Path == "" || req.Valor == nil {
			tipos.EnviarError(w, http.StatusBadRequest, "se requiere path y valor")
			return
		}

		if req.MarcaTiempo == 0 {
			req.MarcaTiempo = time.Now().UnixNano()
		}

		if err := gestor.Insertar(req.Path, req.MarcaTiempo, req.Valor); err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		tipos.EnviarJSON(w, map[string]interface{}{
			"exito":   true,
			"mensaje": "Dato insertado correctamente",
		})
	}
}

// HandlerConsultarRango consulta datos de una serie en un rango de tiempo
func HandlerConsultarRango(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		var req struct {
			Serie        string `json:"serie"`
			TiempoInicio int64  `json:"tiempo_inicio"`
			TiempoFin    int64  `json:"tiempo_fin"`
		}

		if r.Method == http.MethodPost {
			if err := tipos.LeerJSON(r, &req); err != nil {
				tipos.EnviarError(w, http.StatusBadRequest, err.Error())
				return
			}
		} else {
			req.Serie = r.URL.Query().Get("serie")
			req.TiempoInicio = 0
			req.TiempoFin = time.Now().UnixNano()
		}

		if req.Serie == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "se requiere el parámetro 'serie'")
			return
		}

		inicio := time.Unix(0, req.TiempoInicio)
		fin := time.Unix(0, req.TiempoFin)

		resultado, err := gestor.ConsultarRango(req.Serie, inicio, fin)
		if err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		tipos.EnviarJSON(w, resultado)
	}
}

// HandlerConsultarUltimo consulta el último punto de una serie
func HandlerConsultarUltimo(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		path := r.URL.Query().Get("path")
		if path == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "se requiere el parámetro 'path'")
			return
		}

		resultado, err := gestor.ConsultarUltimoPunto(path, nil, nil)
		if err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		tipos.EnviarJSON(w, resultado)
	}
}

// HandlerConsultarAgregacion consulta agregaciones de una serie
func HandlerConsultarAgregacion(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		var req struct {
			Serie        string   `json:"serie"`
			TiempoInicio int64    `json:"tiempo_inicio"`
			TiempoFin    int64    `json:"tiempo_fin"`
			Agregaciones []string `json:"agregaciones"`
		}

		if r.Method == http.MethodPost {
			if err := tipos.LeerJSON(r, &req); err != nil {
				tipos.EnviarError(w, http.StatusBadRequest, err.Error())
				return
			}
		} else {
			req.Serie = r.URL.Query().Get("serie")
			req.Agregaciones = r.URL.Query()["agregacion"]
			req.TiempoInicio = 0
			req.TiempoFin = time.Now().UnixNano()
		}

		if req.Serie == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "se requiere el parámetro 'serie'")
			return
		}

		if len(req.Agregaciones) == 0 {
			tipos.EnviarError(w, http.StatusBadRequest, "se requiere al menos una agregación")
			return
		}

		var tiposAgregacion []tipos.TipoAgregacion
		for _, nombre := range req.Agregaciones {
			tipo := parsearTipoAgregacion(nombre)
			if tipo == "" {
				tipos.EnviarError(w, http.StatusBadRequest, fmt.Sprintf("agregación desconocida: %s", nombre))
				return
			}
			tiposAgregacion = append(tiposAgregacion, tipo)
		}

		inicio := time.Unix(0, req.TiempoInicio)
		fin := time.Unix(0, req.TiempoFin)

		resultado, err := gestor.ConsultarAgregacion(req.Serie, inicio, fin, tiposAgregacion)
		if err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		tipos.EnviarJSON(w, resultado)
	}
}

// HandlerConsultarAgregacionTemporal consulta agregaciones temporales
func HandlerConsultarAgregacionTemporal(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		var req struct {
			Serie        string   `json:"serie"`
			TiempoInicio int64    `json:"tiempo_inicio"`
			TiempoFin    int64    `json:"tiempo_fin"`
			Agregaciones []string `json:"agregaciones"`
			Intervalo    string   `json:"intervalo"`
		}

		if r.Method == http.MethodPost {
			if err := tipos.LeerJSON(r, &req); err != nil {
				tipos.EnviarError(w, http.StatusBadRequest, err.Error())
				return
			}
		} else {
			req.Serie = r.URL.Query().Get("serie")
			req.Agregaciones = r.URL.Query()["agregacion"]
			req.Intervalo = r.URL.Query().Get("intervalo")
			req.TiempoInicio = 0
			req.TiempoFin = time.Now().UnixNano()
		}

		if req.Serie == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "se requiere el parámetro 'serie'")
			return
		}

		if len(req.Agregaciones) == 0 {
			tipos.EnviarError(w, http.StatusBadRequest, "se requiere al menos una agregación")
			return
		}

		if req.Intervalo == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "se requiere el parámetro 'intervalo'")
			return
		}

		var tiposAgregacion []tipos.TipoAgregacion
		for _, nombre := range req.Agregaciones {
			tipo := parsearTipoAgregacion(nombre)
			if tipo == "" {
				tipos.EnviarError(w, http.StatusBadRequest, fmt.Sprintf("agregación desconocida: %s", nombre))
				return
			}
			tiposAgregacion = append(tiposAgregacion, tipo)
		}

		intervalo, err := time.ParseDuration(req.Intervalo)
		if err != nil {
			tipos.EnviarError(w, http.StatusBadRequest, fmt.Sprintf("intervalo inválido: %s", req.Intervalo))
			return
		}

		inicio := time.Unix(0, req.TiempoInicio)
		fin := time.Unix(0, req.TiempoFin)

		resultado, err := gestor.ConsultarAgregacionTemporal(req.Serie, inicio, fin, tiposAgregacion, intervalo)
		if err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		tipos.EnviarJSON(w, resultado)
	}
}

// HandlerListarReglas lista todas las reglas
func HandlerListarReglas(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reglas := gestor.ListarReglas()

		var lista []*Regla
		for _, regla := range reglas {
			lista = append(lista, regla)
		}

		tipos.EnviarJSON(w, lista)
	}
}

// HandlerCrearRegla crea una nueva regla
func HandlerCrearRegla(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		var regla Regla
		if err := tipos.LeerJSON(r, &regla); err != nil {
			tipos.EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if regla.Nombre == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "se requiere el campo 'nombre'")
			return
		}

		if regla.ID == "" {
			regla.ID = fmt.Sprintf("regla-%d", time.Now().UnixNano())
		}

		if err := gestor.AgregarRegla(&regla); err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		tipos.EnviarJSON(w, map[string]interface{}{
			"exito":   true,
			"mensaje": fmt.Sprintf("Regla '%s' creada correctamente", regla.Nombre),
			"datos":   regla,
		})
	}
}

// HandlerObtenerRegla obtiene una regla específica por ID
func HandlerObtenerRegla(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "id de regla requerido")
			return
		}

		reglas := gestor.ListarReglas()
		regla, existe := reglas[id]
		if !existe {
			tipos.EnviarError(w, http.StatusNotFound, fmt.Sprintf("regla '%s' no encontrada", id))
			return
		}

		tipos.EnviarJSON(w, regla)
	}
}

// HandlerEliminarRegla elimina una regla
func HandlerEliminarRegla(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		id := r.PathValue("id")
		if id == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "id de regla requerido")
			return
		}

		if err := gestor.EliminarRegla(id); err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		tipos.EnviarJSON(w, map[string]interface{}{
			"exito":   true,
			"mensaje": fmt.Sprintf("Regla %s eliminada", id),
		})
	}
}

// HandlerHabilitarRegla habilita/deshabilita una regla
func HandlerHabilitarRegla(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		id := r.PathValue("id")
		if id == "" {
			tipos.EnviarError(w, http.StatusBadRequest, "id de regla requerido")
			return
		}

		var req struct {
			Activa bool `json:"activa"`
		}

		if err := tipos.LeerJSON(r, &req); err != nil {
			tipos.EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if err := gestor.HabilitarRegla(id, req.Activa); err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		estado := "deshabilitada"
		if req.Activa {
			estado = "habilitada"
		}

		tipos.EnviarJSON(w, map[string]interface{}{
			"exito":   true,
			"mensaje": fmt.Sprintf("Regla %s %s", id, estado),
		})
	}
}

// HandlerHabilitarMotorReglas habilita todas las reglas
func HandlerHabilitarMotorReglas(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		gestor.HabilitarMotorReglas(true)

		tipos.EnviarJSON(w, map[string]interface{}{
			"exito":   true,
			"mensaje": "Motor de reglas habilitado",
		})
	}
}

// HandlerDeshabilitarMotorReglas deshabilita todas las reglas
func HandlerDeshabilitarMotorReglas(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		gestor.HabilitarMotorReglas(false)

		tipos.EnviarJSON(w, map[string]interface{}{
			"exito":   true,
			"mensaje": "Motor de reglas deshabilitado",
		})
	}
}

// HandlerObtenerTags obtiene los tags del nodo
func HandlerObtenerTags(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tags := gestor.ObtenerTags()
		tipos.EnviarJSON(w, tags)
	}
}

// HandlerActualizarTags actualiza los tags del nodo
func HandlerActualizarTags(gestor *GestorBorde) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			tipos.EnviarError(w, http.StatusMethodNotAllowed, "método no permitido")
			return
		}

		var tags map[string]string
		if err := tipos.LeerJSON(r, &tags); err != nil {
			tipos.EnviarError(w, http.StatusBadRequest, err.Error())
			return
		}

		if err := gestor.ActualizarTags(tags); err != nil {
			tipos.EnviarError(w, http.StatusInternalServerError, err.Error())
			return
		}

		tipos.EnviarJSON(w, map[string]interface{}{
			"exito":   true,
			"mensaje": "Tags actualizados correctamente",
		})
	}
}

// ============================================================================
// HELPERS
// ============================================================================

func parsearTipoDatos(s string) tipos.TipoDatos {
	s = strings.ToLower(s)
	switch s {
	case "boolean", "bool":
		return tipos.Boolean
	case "integer", "int":
		return tipos.Integer
	case "real", "float", "float64", "double":
		return tipos.Real
	case "text", "string":
		return tipos.Text
	default:
		return tipos.Real
	}
}

func parsearTipoAgregacion(s string) tipos.TipoAgregacion {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "avg", "average", "promedio":
		return tipos.AgregacionPromedio
	case "max", "maximum", "maximo":
		return tipos.AgregacionMaximo
	case "min", "minimum", "minimo":
		return tipos.AgregacionMinimo
	case "sum", "suma":
		return tipos.AgregacionSuma
	case "count", "conteo":
		return tipos.AgregacionConteo
	default:
		return ""
	}
}
