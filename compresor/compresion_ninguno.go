package compresor

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// CompresorNingunoGenerico implementa serialización sin compresión para cualquier tipo
// Soporta: int64, float64, bool, string
//
// NORMALIZACIÓN DE TIPOS:
// El sistema acepta múltiples tipos de Go en la entrada (int, int32, float32, etc.)
// pero los normaliza a tipos canónicos antes de la serialización:
//   - Todos los enteros (int, int8, int16, int32, int64, uint*) → int64
//   - Todos los flotantes (float32, float64) → float64
//   - bool → bool (sin cambio)
//   - string → string (sin cambio)
//
// Ver borde/utils.go:inferirTipo() y compresor/compresion_utils.go:ConvertirA*Array()
type CompresorNingunoGenerico[T any] struct{}

// Comprimir serializa valores sin compresión según el tipo
func (c *CompresorNingunoGenerico[T]) Comprimir(valores []T) ([]byte, error) {
	if len(valores) == 0 {
		return []byte{}, nil
	}

	var buffer bytes.Buffer

	// Detectar el tipo y serializar apropiadamente
	// Usamos type assertion en el primer elemento para determinar el tipo
	var ejemplo T

	switch any(ejemplo).(type) {
	case int64:
		// int64: 8 bytes por valor
		valoresInt64 := any(valores).([]int64)
		for _, v := range valoresInt64 {
			if err := binary.Write(&buffer, binary.LittleEndian, v); err != nil {
				return nil, err
			}
		}

	case float64:
		// float64: 8 bytes por valor
		valoresFloat64 := any(valores).([]float64)
		for _, v := range valoresFloat64 {
			if err := binary.Write(&buffer, binary.LittleEndian, v); err != nil {
				return nil, err
			}
		}

	case bool:
		// bool: 1 byte por valor (0 o 1)
		valoresBool := any(valores).([]bool)
		for _, v := range valoresBool {
			var b byte
			if v {
				b = 1
			}
			if err := buffer.WriteByte(b); err != nil {
				return nil, err
			}
		}

	case string:
		// string: [longitud:2 bytes][datos]
		valoresString := any(valores).([]string)
		for _, v := range valoresString {
			if len(v) > 65535 {
				return nil, fmt.Errorf("string demasiado largo: %d bytes (máximo 65535)", len(v))
			}
			// Escribir longitud
			if err := binary.Write(&buffer, binary.LittleEndian, uint16(len(v))); err != nil {
				return nil, err
			}
			// Escribir string
			if _, err := buffer.WriteString(v); err != nil {
				return nil, err
			}
		}

	default:
		return nil, fmt.Errorf("tipo no soportado para CompresorNinguno: %T", ejemplo)
	}

	return buffer.Bytes(), nil
}

// Descomprimir deserializa valores según el tipo
func (c *CompresorNingunoGenerico[T]) Descomprimir(datos []byte) ([]T, error) {
	if len(datos) == 0 {
		return []T{}, nil
	}

	var ejemplo T
	buffer := bytes.NewReader(datos)

	switch any(ejemplo).(type) {
	case int64:
		// int64: 8 bytes por valor
		if len(datos)%8 != 0 {
			return nil, fmt.Errorf("datos corruptos: tamaño no múltiplo de 8 para int64")
		}
		numValores := len(datos) / 8
		resultados := make([]T, numValores)
		for i := 0; i < numValores; i++ {
			var v int64
			if err := binary.Read(buffer, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			resultados[i] = any(v).(T)
		}
		return resultados, nil

	case float64:
		// float64: 8 bytes por valor
		if len(datos)%8 != 0 {
			return nil, fmt.Errorf("datos corruptos: tamaño no múltiplo de 8 para float64")
		}
		numValores := len(datos) / 8
		resultados := make([]T, numValores)
		for i := 0; i < numValores; i++ {
			var v float64
			if err := binary.Read(buffer, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			resultados[i] = any(v).(T)
		}
		return resultados, nil

	case bool:
		// bool: 1 byte por valor
		resultados := make([]T, len(datos))
		for i := 0; i < len(datos); i++ {
			b, err := buffer.ReadByte()
			if err != nil {
				return nil, err
			}
			resultados[i] = any(b != 0).(T)
		}
		return resultados, nil

	case string:
		// string: [longitud:2 bytes][datos]
		resultados := make([]T, 0)
		for buffer.Len() > 0 {
			// Leer longitud
			var longitud uint16
			if err := binary.Read(buffer, binary.LittleEndian, &longitud); err != nil {
				return nil, fmt.Errorf("error leyendo longitud de string: %v", err)
			}

			// Si el string es vacio, no hay bytes que leer
			if longitud == 0 {
				resultados = append(resultados, any("").(T))
				continue
			}

			// Leer string
			strBytes := make([]byte, longitud)
			if _, err := buffer.Read(strBytes); err != nil {
				return nil, fmt.Errorf("error leyendo string: %v", err)
			}

			resultados = append(resultados, any(string(strBytes)).(T))
		}
		return resultados, nil

	default:
		return nil, fmt.Errorf("tipo no soportado para CompresorNinguno: %T", ejemplo)
	}
}
