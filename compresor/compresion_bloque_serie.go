package compresor

import (
	"fmt"

	"github.com/sensorwave-dev/sensorwave/tipos"
)

// DescomprimirBloqueSerie descomprime un bloque de datos de serie temporal.
// Esta función es usada tanto por el edge como por el despachador para lectura de datos.
//
// Parámetros:
//   - datosComprimidos: bloque de datos comprimido
//   - tipoDatos: tipo de datos de la serie (Boolean, Integer, Real, Text)
//   - compresionBytes: algoritmo de compresión de valores (DeltaDelta, Xor, RLE, etc.)
//   - compresionBloque: algoritmo de compresión de bloque (LZ4, ZSTD, Snappy, Gzip, Ninguna)
//
// Retorna:
//   - []tipos.Medicion: slice de mediciones descomprimidas
//   - error: error si falla la descompresión
func DescomprimirBloqueSerie(datosComprimidos []byte, tipoDatos tipos.TipoDatos,
	compresionBytes tipos.TipoCompresion, compresionBloque tipos.TipoCompresionBloque) ([]tipos.Medicion, error) {

	// NIVEL 2: Descompresión de bloque
	compresorBloque := ObtenerCompresorBloque(compresionBloque)
	bloqueDescomprimido, err := compresorBloque.Descomprimir(datosComprimidos)
	if err != nil {
		return nil, fmt.Errorf("error en descompresión de bloque: %v", err)
	}

	// NIVEL 1: Separar datos de tiempos y valores
	tiemposComprimidos, valoresComprimidos, err := SepararDatos(bloqueDescomprimido)
	if err != nil {
		return nil, fmt.Errorf("error al separar datos: %v", err)
	}

	// Descomprimir tiempos usando DeltaDelta (siempre)
	tiempos, err := DescompresionDeltaDeltaTiempo(tiemposComprimidos)
	if err != nil {
		return nil, fmt.Errorf("error al descomprimir tiempos: %v", err)
	}

	// Descomprimir valores según el tipo de datos de la serie
	var mediciones []tipos.Medicion

	switch tipoDatos {
	case tipos.Integer:
		mediciones, err = descomprimirValoresInteger(tiempos, valoresComprimidos, compresionBytes)
	case tipos.Real:
		mediciones, err = descomprimirValoresReal(tiempos, valoresComprimidos, compresionBytes)
	case tipos.Boolean:
		mediciones, err = descomprimirValoresBoolean(tiempos, valoresComprimidos, compresionBytes)
	case tipos.Text:
		mediciones, err = descomprimirValoresText(tiempos, valoresComprimidos, compresionBytes)
	default:
		return nil, fmt.Errorf("tipo de datos no soportado: %v", tipoDatos)
	}

	if err != nil {
		return nil, err
	}

	// Verificar que tengamos el mismo número de tiempos y valores
	if len(tiempos) != len(mediciones) {
		return nil, fmt.Errorf("número de tiempos (%d) y valores (%d) no coinciden", len(tiempos), len(mediciones))
	}

	return mediciones, nil
}

// descomprimirValoresInteger descomprime valores de tipo Integer
func descomprimirValoresInteger(tiempos []int64, valoresComprimidos []byte, compresionBytes tipos.TipoCompresion) ([]tipos.Medicion, error) {
	var valores []int64
	var err error

	switch compresionBytes {
	case tipos.DeltaDelta:
		comp := &CompresorDeltaDeltaGenerico[int64]{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	case tipos.RLE:
		comp := &CompresorRLEGenerico[int64]{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	case tipos.Bits:
		comp := &CompresorBitsGenerico[int64]{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	case tipos.SinCompresion:
		comp := &CompresorNingunoGenerico[int64]{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	default:
		return nil, fmt.Errorf("compresión no soportada para tipo Integer: %v", compresionBytes)
	}

	if err != nil {
		return nil, fmt.Errorf("error al descomprimir valores enteros (%s): %v", compresionBytes, err)
	}

	mediciones := make([]tipos.Medicion, len(tiempos))
	for i := range tiempos {
		mediciones[i] = tipos.Medicion{Tiempo: tiempos[i], Valor: valores[i]}
	}
	return mediciones, nil
}

// descomprimirValoresReal descomprime valores de tipo Real (float64)
func descomprimirValoresReal(tiempos []int64, valoresComprimidos []byte, compresionBytes tipos.TipoCompresion) ([]tipos.Medicion, error) {
	var valores []float64
	var err error

	switch compresionBytes {
	case tipos.DeltaDelta:
		comp := &CompresorDeltaDeltaGenerico[float64]{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	case tipos.Xor:
		comp := &CompresorXor{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	case tipos.RLE:
		comp := &CompresorRLEGenerico[float64]{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	case tipos.SinCompresion:
		comp := &CompresorNingunoGenerico[float64]{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	default:
		return nil, fmt.Errorf("compresión no soportada para tipo Real: %v", compresionBytes)
	}

	if err != nil {
		return nil, fmt.Errorf("error al descomprimir valores reales (%s): %v", compresionBytes, err)
	}

	mediciones := make([]tipos.Medicion, len(tiempos))
	for i := range tiempos {
		mediciones[i] = tipos.Medicion{Tiempo: tiempos[i], Valor: valores[i]}
	}
	return mediciones, nil
}

// descomprimirValoresBoolean descomprime valores de tipo Boolean
func descomprimirValoresBoolean(tiempos []int64, valoresComprimidos []byte, compresionBytes tipos.TipoCompresion) ([]tipos.Medicion, error) {
	var valores []bool
	var err error

	switch compresionBytes {
	case tipos.RLE:
		comp := &CompresorRLEGenerico[bool]{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	case tipos.SinCompresion:
		comp := &CompresorNingunoGenerico[bool]{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	default:
		return nil, fmt.Errorf("compresión no soportada para tipo Boolean: %v", compresionBytes)
	}

	if err != nil {
		return nil, fmt.Errorf("error al descomprimir valores booleanos (%s): %v", compresionBytes, err)
	}

	mediciones := make([]tipos.Medicion, len(tiempos))
	for i := range tiempos {
		mediciones[i] = tipos.Medicion{Tiempo: tiempos[i], Valor: valores[i]}
	}
	return mediciones, nil
}

// descomprimirValoresText descomprime valores de tipo Text (string)
func descomprimirValoresText(tiempos []int64, valoresComprimidos []byte, compresionBytes tipos.TipoCompresion) ([]tipos.Medicion, error) {
	var valores []string
	var err error

	switch compresionBytes {
	case tipos.Diccionario:
		comp := &CompresorDiccionario{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	case tipos.RLE:
		comp := &CompresorRLEGenerico[string]{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	case tipos.SinCompresion:
		comp := &CompresorNingunoGenerico[string]{}
		valores, err = comp.Descomprimir(valoresComprimidos)
	default:
		return nil, fmt.Errorf("compresión no soportada para tipo Text: %v", compresionBytes)
	}

	if err != nil {
		return nil, fmt.Errorf("error al descomprimir valores texto (%s): %v", compresionBytes, err)
	}

	mediciones := make([]tipos.Medicion, len(tiempos))
	for i := range tiempos {
		mediciones[i] = tipos.Medicion{Tiempo: tiempos[i], Valor: valores[i]}
	}
	return mediciones, nil
}
