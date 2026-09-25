/*
## Algoritmo de Compresión RLE (Run-Length Encoding) - Versión Genérica

Objetivo: Comprimir secuencias de valores detectando repeticiones consecutivas.

Entrada: Array de valores de tipo T [v₀, v₁, v₂, ..., vₙ]

Salida: Secuencia de pares (run_count, valor) donde:

• run_count: entero sin signo de 8 bits (1-255) indicando cuántas veces se repite consecutivamente
• valor: valor que se repite

Algoritmo:

1. Inicializar:
 • Si el input está vacío, retornar output vacío
 • previous_value ← primer valor del input
 • previous_run_count ← 1
 • output ← lista vacía

2. Procesar cada valor vᵢ restante (i = 1 hasta n):
 • Si vᵢ es idéntico a previous_value Y previous_run_count < 255:
  • previous_run_count ← previous_run_count + 1
 • Sino:
  • Agregar par (previous_run_count, previous_value) a output
  • previous_value ← vᵢ
  • previous_run_count ← 1

3. Finalizar:
 • Agregar último par (previous_run_count, previous_value) a output
 • Retornar output

Restricciones:

• Máximo 255 repeticiones por par (límite de u8)
• float64 se compara por bits IEEE 754; el resto, con ==
• Formato little-endian para serialización
• Para strings se usa serialización de longitud variable

Complejidad: O(n) tiempo, O(k) espacio donde k ≤ n es el número de runs distintos.
*/

package compresor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// CompresorRLEGenerico implementa el algoritmo de compresión RLE para tipos comparables
type CompresorRLEGenerico[T comparable] struct{}

func iguales[T comparable](a, b T) bool {
	af, aok := any(a).(float64)
	bf, bok := any(b).(float64)
	if aok && bok {
		return math.Float64bits(af) == math.Float64bits(bf)
	}
	return a == b
}

// Comprimir comprime una serie de valores usando RLE
// Para tipos numéricos y booleanos usa binary.Write
// Para strings usa serialización de longitud variable
func (c *CompresorRLEGenerico[T]) Comprimir(valores []T) ([]byte, error) {
	if len(valores) == 0 {
		return []byte{}, nil
	}

	var buffer bytes.Buffer
	valorPrevio := valores[0]
	cantidad := uint8(1)

	for i := 1; i < len(valores); i++ {
		if iguales(valores[i], valorPrevio) && cantidad < 255 {
			cantidad++
		} else {
			// Escribir el par (cantidad, valorPrevio)
			if err := buffer.WriteByte(cantidad); err != nil {
				return nil, err
			}
			if err := escribirValor(&buffer, valorPrevio); err != nil {
				return nil, err
			}
			valorPrevio = valores[i]
			cantidad = 1
		}
	}

	// Escribir el último par
	if err := buffer.WriteByte(cantidad); err != nil {
		return nil, err
	}
	if err := escribirValor(&buffer, valorPrevio); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// Descomprimir descomprime datos RLE a una serie de valores
func (c *CompresorRLEGenerico[T]) Descomprimir(datos []byte) ([]T, error) {
	if len(datos) == 0 {
		return []T{}, nil
	}

	var resultados []T
	buffer := bytes.NewReader(datos)

	for {
		var cantidad uint8
		err := binary.Read(buffer, binary.LittleEndian, &cantidad)
		if err != nil {
			break // Fin de datos
		}

		valor, err := leerValor[T](buffer)
		if err != nil {
			return nil, fmt.Errorf("error al leer valor: %v", err)
		}

		for i := 0; i < int(cantidad); i++ {
			resultados = append(resultados, valor)
		}
	}

	return resultados, nil
}

// escribirValor escribe un valor al buffer usando binary.Write
// Esta función usa type assertion en runtime para manejar diferentes tipos
func escribirValor[T any](buffer *bytes.Buffer, valor T) error {
	// Intentar usar binary.Write para tipos básicos
	err := binary.Write(buffer, binary.LittleEndian, valor)
	if err != nil {
		// Si falla, intentar con string (caso especial)
		if str, ok := any(valor).(string); ok {
			// Para strings: escribir longitud (uint16) + bytes
			if len(str) > 65535 {
				return fmt.Errorf("string demasiado largo: %d bytes", len(str))
			}
			if err := binary.Write(buffer, binary.LittleEndian, uint16(len(str))); err != nil {
				return err
			}
			_, err := buffer.WriteString(str)
			return err
		}
		return err
	}
	return nil
}

// leerValor lee un valor del buffer
func leerValor[T any](buffer *bytes.Reader) (T, error) {
	var valor T

	// Intentar usar binary.Read para tipos básicos
	err := binary.Read(buffer, binary.LittleEndian, &valor)
	if err == nil {
		return valor, nil
	}

	// Si falla, intentar con string (caso especial)
	if _, ok := any(valor).(string); ok {
		var longitud uint16
		if err := binary.Read(buffer, binary.LittleEndian, &longitud); err != nil {
			return valor, err
		}

		// Si el string es vacio, no hay bytes que leer
		if longitud == 0 {
			return any("").(T), nil
		}

		strBytes := make([]byte, longitud)
		if _, err := buffer.Read(strBytes); err != nil {
			return valor, err
		}

		// Convertir []byte a string y luego a T
		str := string(strBytes)
		return any(str).(T), nil
	}

	return valor, err
}
