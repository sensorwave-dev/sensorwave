package edge

import (
	"fmt"

	"github.com/sensorwave-dev/sensorwave/tipos"
)

// ObtenerSeries retorna la metadata de una serie por su path
func (me *ManagerEdge) ObtenerSeries(path string) (tipos.Serie, error) {
	// Lectura sin bloqueo para casos comunes
	me.cache.mu.RLock()
	if meta, existe := me.cache.datos[path]; existe {
		me.cache.mu.RUnlock()
		return meta, nil
	}
	me.cache.mu.RUnlock()

	// Cache miss - error
	return tipos.Serie{}, fmt.Errorf("Serie no encontrada")
}

// ListarSeries retorna una lista de todos los paths de series existentes
func (me *ManagerEdge) ListarSeries() ([]string, error) {
	me.cache.mu.RLock()
	defer me.cache.mu.RUnlock()

	series := make([]string, 0, len(me.cache.datos))
	for serie := range me.cache.datos {
		series = append(series, serie)
	}
	return series, nil
}

// ListarSeriesPorPath retorna todas las series que coincidan con un patrón de path
// Soporta wildcards: "dispositivo_001/*" o "*/temperatura"
func (me *ManagerEdge) ListarSeriesPorPath(pathPattern string) ([]tipos.Serie, error) {
	me.cache.mu.RLock()
	defer me.cache.mu.RUnlock()

	var series []tipos.Serie
	for _, serie := range me.cache.datos {
		if tipos.MatchPath(serie.Path, pathPattern) {
			series = append(series, serie)
		}
	}
	return series, nil
}

// ListarSeriesPorTags retorna todas las series que tengan todos los tags especificados
func (me *ManagerEdge) ListarSeriesPorTags(tags map[string]string) ([]tipos.Serie, error) {
	me.cache.mu.RLock()
	defer me.cache.mu.RUnlock()

	var series []tipos.Serie
	for _, serie := range me.cache.datos {
		if matchTags(serie.Tags, tags) {
			series = append(series, serie)
		}
	}
	return series, nil
}

// ListarSeriesPorDispositivo retorna todas las series de un dispositivo específico
// Asume que el path es "dispositivo_XXX/metrica"
func (me *ManagerEdge) ListarSeriesPorDispositivo(dispositivoID string) ([]tipos.Serie, error) {
	pathPattern := dispositivoID + "/*"
	return me.ListarSeriesPorPath(pathPattern)
}
