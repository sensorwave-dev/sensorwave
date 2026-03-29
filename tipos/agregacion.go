package tipos

// TipoAgregacion define los tipos de agregación soportados para consultas
type TipoAgregacion string

const (
	AgregacionPromedio TipoAgregacion = "promedio"
	AgregacionMaximo   TipoAgregacion = "maximo"
	AgregacionMinimo   TipoAgregacion = "minimo"
	AgregacionSuma     TipoAgregacion = "suma"
	AgregacionConteo   TipoAgregacion = "count"
)

// ResultadoAgregacionTemporal representa el resultado de agregaciones temporales en formato matricial.
// Soporta múltiples agregaciones en una sola consulta (patrón IoTDB/QuestDB).
// Cada serie temporal es una columna, los buckets de tiempo son las filas.
// Valores faltantes se representan como math.NaN().
//
// Estructura de Valores: [agregacion][bucket][serie]
// Ejemplo de acceso:
//
//	resultado.Valores[0][bucket][serie] // Primera agregación
//	resultado.Agregaciones[0]           // Tipo de la primera agregación
type ResultadoAgregacionTemporal struct {
	Series             []string         // Columnas: nombres de series ordenados alfabéticamente
	Tiempos            []int64          // Filas: inicio de cada bucket (Unix nanosegundos)
	Agregaciones       []TipoAgregacion // Lista ordenada de agregaciones calculadas
	Valores            [][][]float64    // Matriz [agregacion][bucket][serie], math.NaN() = sin datos
	NodosNoDisponibles []string         // IDs de nodos que no respondieron (solo en consultas globales)
}

// ObtenerAgregacion retorna la matriz de valores para un tipo de agregación específico.
// Retorna nil, false si la agregación no está presente en el resultado.
func (r *ResultadoAgregacionTemporal) ObtenerAgregacion(tipo TipoAgregacion) ([][]float64, bool) {
	for i, t := range r.Agregaciones {
		if t == tipo {
			return r.Valores[i], true
		}
	}
	return nil, false
}
