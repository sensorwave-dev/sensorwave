package tipos

// Algoritmos de compresión para series temporales
//
// El sistema soporta dos niveles de compresión:
// - Nivel 1 (TipoCompresion): Compresión específica por tipo de datos
// - Nivel 2 (TipoCompresionBloque): Compresión genérica de bloques
//
// Cada TipoDatos tiene asociado un conjunto de algoritmos compatibles.
// Ver tipos/tipo_datos.go para el mapeo completo.

// TipoCompresionBloque - Algoritmos de compresión de nivel 2 (bloques completos)
type TipoCompresionBloque string

// Valores posibles para TipoCompresionBloque
const (
	SinCompresionBloque TipoCompresionBloque = "SinCompresionBloque" // Sin compresión
	LZ4                 TipoCompresionBloque = "LZ4"                 // LZ4 - rápido, compresión moderada
	ZSTD                TipoCompresionBloque = "ZSTD"                // Zstandard - mejor compresión, más lento
	Snappy              TipoCompresionBloque = "Snappy"              // Snappy - muy rápido, compresión baja
	Gzip                TipoCompresionBloque = "Gzip"                // Gzip - compatible, compresión moderada
)

// TipoCompresion - Algoritmos de compresión de nivel 1 (valores específicos)
type TipoCompresion string

// Valores posibles para TipoCompresion
const (
	// Sin compresión - Compatible con todos los tipos
	SinCompresionBytes TipoCompresion = "SinCompresionBytes"

	// Algoritmos para datos numéricos (Integer, Real)
	DeltaDelta TipoCompresion = "DeltaDelta" // Delta-of-delta - óptimo para series monótonas y timestamps
	Xor        TipoCompresion = "Xor"        // XOR (Gorilla) - óptimo para flotantes con cambios pequeños
	Bits       TipoCompresion = "Bits"       // Compresión de bits - para enteros en rangos pequeños

	// Algoritmos universales - Compatible con todos los tipos
	RLE TipoCompresion = "RLE" // Run-Length Encoding - óptimo para datos repetitivos

	// Algoritmos para datos categóricos/texto (Text)
	Diccionario TipoCompresion = "Diccionario" // Codificación por diccionario - óptimo para texto con vocabulario limitado
)
