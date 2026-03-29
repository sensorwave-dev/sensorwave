/*
## Algoritmo de Compresión Delta-Delta - Versión Genérica para Valores

Objetivo: Comprimir secuencias de valores numéricos usando diferencias de diferencias.

Este algoritmo es similar a TS_2DIFF de IoTDB pero aplicado a valores en lugar de timestamps.

Algoritmo:

1. Primer valor: Se almacena sin comprimir
2. Segunda delta: Se almacena la diferencia (valor2 - valor1)
3. Valores subsiguientes: Se almacena la diferencia de diferencias (delta-delta)

La delta-delta se comprime usando codificación de longitud variable:
- Si cabe en 1 byte (-128 a 127): flag 0x00 + 1 byte
- Si cabe en 2 bytes (-32768 a 32767): flag 0x01 + 2 bytes
- Si cabe en 4 bytes: flag 0x02 + 4 bytes
- Si requiere 8 bytes: flag 0x03 + 8 bytes

Complejidad: O(n) tiempo, O(1) espacio adicional
*/

package compresor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// Numeric es una restricción de tipo para valores numéricos
// IMPORTANTE: El sistema normaliza todos los enteros a int64 y todos los flotantes a float64
// antes de la compresión. Ver borde/utils.go:inferirTipo() y compresor/compresion_utils.go:ConvertirA*Array()
type Numeric interface {
	int64 | float64
}

// CompresorDeltaDeltaGenerico implementa compresión delta-delta para valores numéricos
type CompresorDeltaDeltaGenerico[T Numeric] struct{}

// Comprimir comprime valores numéricos usando delta-delta encoding
func (c *CompresorDeltaDeltaGenerico[T]) Comprimir(valores []T) ([]byte, error) {
	if len(valores) == 0 {
		return []byte{}, nil
	}

	var buffer bytes.Buffer

	// Almacenar el primer valor sin compresión
	primerValor := valores[0]
	if err := binary.Write(&buffer, binary.LittleEndian, primerValor); err != nil {
		return nil, err
	}

	if len(valores) == 1 {
		return buffer.Bytes(), nil
	}

	// Calcular y almacenar primera delta
	var deltaPrevio int64
	if len(valores) > 1 {
		deltaPrevio = aInt64(valores[1]) - aInt64(valores[0])
		if err := binary.Write(&buffer, binary.LittleEndian, deltaPrevio); err != nil {
			return nil, err
		}
	}

	if len(valores) == 2 {
		return buffer.Bytes(), nil
	}

	// Aplicar compresión delta-delta para el resto
	for i := 2; i < len(valores); i++ {
		valorActual := aInt64(valores[i])
		valorAnterior := aInt64(valores[i-1])

		// Calcular delta
		delta := valorActual - valorAnterior
		deltaDelta := delta - deltaPrevio

		// Comprimir delta-delta con longitud variable
		if deltaDelta >= -128 && deltaDelta <= 127 {
			// 1 byte + flag
			buffer.WriteByte(0x00)
			buffer.WriteByte(byte(deltaDelta))
		} else if deltaDelta >= -32768 && deltaDelta <= 32767 {
			// 2 bytes + flag
			buffer.WriteByte(0x01)
			buffer.WriteByte(byte(deltaDelta >> 8))
			buffer.WriteByte(byte(deltaDelta))
		} else if deltaDelta >= -2147483648 && deltaDelta <= 2147483647 {
			// 4 bytes + flag
			buffer.WriteByte(0x02)
			binary.Write(&buffer, binary.LittleEndian, int32(deltaDelta))
		} else {
			// 8 bytes + flag
			buffer.WriteByte(0x03)
			binary.Write(&buffer, binary.LittleEndian, deltaDelta)
		}

		deltaPrevio = delta
	}

	return buffer.Bytes(), nil
}

// Descomprimir descomprime datos delta-delta
func (c *CompresorDeltaDeltaGenerico[T]) Descomprimir(datos []byte) ([]T, error) {
	if len(datos) == 0 {
		return []T{}, nil
	}

	buffer := bytes.NewReader(datos)
	resultado := make([]T, 0)

	// Leer primer valor
	var primerValor T
	if err := binary.Read(buffer, binary.LittleEndian, &primerValor); err != nil {
		return nil, fmt.Errorf("error leyendo primer valor: %v", err)
	}
	resultado = append(resultado, primerValor)

	if buffer.Len() == 0 {
		return resultado, nil
	}

	// Leer primera delta
	var deltaPrevio int64
	if err := binary.Read(buffer, binary.LittleEndian, &deltaPrevio); err != nil {
		return nil, fmt.Errorf("error leyendo primera delta: %v", err)
	}

	segundoValor := desdeInt64[T](aInt64(primerValor) + deltaPrevio)
	resultado = append(resultado, segundoValor)

	if buffer.Len() == 0 {
		return resultado, nil
	}

	// Descomprimir el resto
	valorAnterior := aInt64(segundoValor)

	for buffer.Len() > 0 {
		// Leer flag
		flag, err := buffer.ReadByte()
		if err != nil {
			break
		}

		var deltaDelta int64

		switch flag {
		case 0x00: // 1 byte
			b, err := buffer.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("error leyendo delta-delta de 1 byte: %v", err)
			}
			deltaDelta = int64(int8(b))

		case 0x01: // 2 bytes
			var b1, b2 byte
			b1, _ = buffer.ReadByte()
			b2, _ = buffer.ReadByte()
			deltaDelta = int64(int16(b1)<<8 | int16(b2))

		case 0x02: // 4 bytes
			var val int32
			if err := binary.Read(buffer, binary.LittleEndian, &val); err != nil {
				return nil, fmt.Errorf("error leyendo delta-delta de 4 bytes: %v", err)
			}
			deltaDelta = int64(val)

		case 0x03: // 8 bytes
			if err := binary.Read(buffer, binary.LittleEndian, &deltaDelta); err != nil {
				return nil, fmt.Errorf("error leyendo delta-delta de 8 bytes: %v", err)
			}

		default:
			return nil, fmt.Errorf("flag de compresión desconocido: %x", flag)
		}

		// Reconstruir el valor
		delta := deltaPrevio + deltaDelta
		valorActual := valorAnterior + delta

		resultado = append(resultado, desdeInt64[T](valorActual))

		deltaPrevio = delta
		valorAnterior = valorActual
	}

	return resultado, nil
}

// aInt64 convierte valores numéricos a int64 para cálculos de delta
// Para int64: conversión directa
// Para float64: usa representación binaria IEEE 754 (math.Float64bits)
func aInt64[T Numeric](v T) int64 {
	switch val := any(v).(type) {
	case int64:
		return val
	case float64:
		return int64(math.Float64bits(val))
	default:
		return 0
	}
}

// desdeInt64 convierte int64 de vuelta al tipo T
// Reversa de aInt64: reconstruye el tipo original
func desdeInt64[T Numeric](v int64) T {
	var zero T
	switch any(zero).(type) {
	case int64:
		return any(v).(T)
	case float64:
		return any(math.Float64frombits(uint64(v))).(T)
	default:
		return zero
	}
}
