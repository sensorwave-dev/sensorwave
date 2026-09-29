// Package almacen define el contrato del almacenamiento local del borde.
// Una implementación guarda los puntos de ingesta, los bloques comprimidos
// y los catálogos. El borde sigue eligiendo la compresión.
package almacen

// Punto es una medición todavía no compactada.
// Datos es el gob de la medición.
type Punto struct {
	SerieID int
	Tiempo  int64
	Datos   []byte
}

// Bloque es un tramo de tiempo ya comprimido.
// Datos es el blob que produjo el compresor.
type Bloque struct {
	SerieID int
	Inicio  int64
	Fin     int64
	Datos   []byte
}

// Almacen es el almacenamiento local de un nodo.
// Quien lo implementa cubre ingesta, bloques y catálogos en un solo abrir.
type Almacen interface {
	// AgregarPunto persiste una medición. Insertar lo usa, y si con ese
	// punto se llega a TamañoBloque el coordinador sigue con
	// PuntosIngestaAntiguos y CerrarBloque.
	AgregarPunto(Punto) error

	// PuntosIngestaAntiguos devuelve los n puntos de menor tiempo de la serie.
	// No los borra. Equivale a leer los más antiguos de la ingesta.
	PuntosIngestaAntiguos(serieID, n int) ([]Punto, error)

	// PuntosEnRango devuelve los puntos de ingesta con tiempo en [desde, hasta].
	// Lo usan ConsultarRango y las consultas que parten de ahí.
	PuntosEnRango(serieID int, desde, hasta int64) ([]Punto, error)

	// UltimoPunto devuelve la medición de ingesta con mayor tiempo.
	// ConsultarUltimoPunto sin rango lo usa.
	UltimoPunto(serieID int) (Punto, bool, error)

	// CerrarBloque publica el bloque y borra de la ingesta los tiempos
	// indicados, o no cambia nada. tiempos son los que devolvió
	// PuntosIngestaAntiguos y el borde conservó en memoria: no se vuelven
	// a leer. En SQLite la transacción hace INSERT del bloque y
	// DELETE FROM ingesta WHERE serie_id = ? AND tiempo IN (esos tiempos).
	// No es un DELETE por rango ni un segundo SELECT de los N más antiguos.
	// Si Datos está vacío, solo borra esos tiempos y no escribe bloque.
	CerrarBloque(bloque Bloque, tiempos []int64) error

	// BloquesEnRango recorre bloques que solapan [desde, hasta].
	// Puede devolver un bloque que se sale del rango.
	BloquesEnRango(serieID int, desde, hasta int64) (IterBloques, error)

	// BorrarBloque elimina un bloque cerrado. EjecutarCicloS3 lo usa
	// después de subir ese bloque.
	BorrarBloque(serieID int, inicio, fin int64) error

	// BorrarDatosDeSerie elimina los puntos y los bloques de la serie.
	BorrarDatosDeSerie(serieID int) error

	// GuardarSerie persiste la ficha. CrearSerie lo usa.
	// ObtenerSeries y los listados públicos leen la caché que Crear llena.
	GuardarSerie(path string, valor []byte) error
	ObtenerSerie(path string) ([]byte, bool, error)
	ListarSeries() (IterClave, error)

	// BorrarSerie saca la ficha, los puntos y los bloques en un solo paso.
	BorrarSerie(path string, serieID int) error

	// GuardarRegla persiste una regla. AgregarRegla, ActualizarRegla y
	// HabilitarRegla lo usan. ListarReglas de Almacen lo usa Crear.
	GuardarRegla(id string, valor []byte) error
	ObtenerRegla(id string) ([]byte, bool, error)
	ListarReglas() (IterClave, error)
	BorrarRegla(id string) error

	// El identificador del nodo. ObtenerNodoID público lee la memoria
	// que Crear cargó con estos métodos.
	GuardarNodoID(id string) error
	ObtenerNodoID() (string, bool, error)

	// Tags del nodo. GuardarTagsNodo(nil) los borra.
	GuardarTagsNodo(valor []byte) error
	ObtenerTagsNodo() ([]byte, bool, error)

	// Contador del nodo: el último SerieId asignado, no un conteo por serie.
	GuardarContador(valor []byte) error
	ObtenerContador() ([]byte, bool, error)

	// Eliminaciones pendientes de subir. EliminarSerie guarda; EjecutarCicloS3
	// lista, reescribe y borra.
	GuardarPendiente(serieID int, valor []byte) error
	ListarPendientes() (IterPendiente, error)
	BorrarPendiente(serieID int) error

	Cerrar() error
}

// IterBloques recorre bloques. Bloque solo vale hasta el siguiente movimiento.
type IterBloques interface {
	Siguiente() bool
	Bloque() Bloque
	Error() error
	Close() error
}

// IterClave recorre fichas identificadas por un nombre.
// Clave y Valor solo valen hasta el siguiente movimiento.
type IterClave interface {
	Siguiente() bool
	Clave() string
	Valor() []byte
	Error() error
	Close() error
}

// IterPendiente recorre eliminaciones pendientes.
// Valor solo vale hasta el siguiente movimiento.
type IterPendiente interface {
	Siguiente() bool
	SerieID() int
	Valor() []byte
	Error() error
	Close() error
}
