package compresor

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// CompresorDiccionario implementa compresión por diccionario para datos categóricos
//
// Algoritmo:
// 1. Construir diccionario de strings únicos con IDs (uint16)
// 2. Reemplazar cada string con su ID
// 3. Comprimir IDs usando RLE
//
// Formato de salida:
// [num_entries: 2 bytes uint16]
// [entry1_len: 2 bytes][entry1_data: bytes]...
// [rle_compressed_indices]
//
// Límites:
// - Máximo 65535 strings únicos
// - Máximo 65535 caracteres por string
type CompresorDiccionario struct{}

func (c *CompresorDiccionario) Comprimir(valores []string) ([]byte, error) {
	if len(valores) == 0 {
		return []byte{}, nil
	}

	// Construir diccionario de strings únicos
	diccionario := make(map[string]uint16)
	entradas := []string{}
	indices := make([]uint16, len(valores))

	siguienteID := uint16(0)
	for i, val := range valores {
		if id, existe := diccionario[val]; existe {
			// String ya existe en diccionario
			indices[i] = id
		} else {
			// Nuevo string
			if siguienteID == 65535 {
				return nil, fmt.Errorf("demasiadas entradas únicas (máximo 65535)")
			}
			diccionario[val] = siguienteID
			entradas = append(entradas, val)
			indices[i] = siguienteID
			siguienteID++
		}
	}

	var buffer bytes.Buffer

	// Escribir número de entradas en el diccionario
	if err := binary.Write(&buffer, binary.LittleEndian, uint16(len(entradas))); err != nil {
		return nil, err
	}

	// Escribir cada entrada del diccionario
	for _, entry := range entradas {
		if len(entry) > 65535 {
			return nil, fmt.Errorf("string demasiado largo: %d bytes (máximo 65535)", len(entry))
		}
		// Escribir longitud del string
		if err := binary.Write(&buffer, binary.LittleEndian, uint16(len(entry))); err != nil {
			return nil, err
		}
		// Escribir el string
		if _, err := buffer.WriteString(entry); err != nil {
			return nil, err
		}
	}

	// Comprimir índices usando RLE genérico
	compresorRLE := &CompresorRLEGenerico[uint16]{}
	indicesComprimidos, err := compresorRLE.Comprimir(indices)
	if err != nil {
		return nil, fmt.Errorf("error al comprimir índices: %v", err)
	}

	// Agregar índices comprimidos
	buffer.Write(indicesComprimidos)

	return buffer.Bytes(), nil
}

func (c *CompresorDiccionario) Descomprimir(datos []byte) ([]string, error) {
	if len(datos) == 0 {
		return []string{}, nil
	}

	reader := bytes.NewReader(datos)

	// Leer número de entradas en el diccionario
	var numEntradas uint16
	if err := binary.Read(reader, binary.LittleEndian, &numEntradas); err != nil {
		return nil, fmt.Errorf("error leyendo número de entradas: %v", err)
	}

	// Leer diccionario
	entradas := make([]string, numEntradas)
	for i := uint16(0); i < numEntradas; i++ {
		// Leer longitud del string
		var strLen uint16
		if err := binary.Read(reader, binary.LittleEndian, &strLen); err != nil {
			return nil, fmt.Errorf("error leyendo longitud de string %d: %v", i, err)
		}

		// Leer el string
		strBytes := make([]byte, strLen)
		if _, err := reader.Read(strBytes); err != nil {
			return nil, fmt.Errorf("error leyendo string %d: %v", i, err)
		}
		entradas[i] = string(strBytes)
	}

	// Leer índices comprimidos (resto de los datos)
	indicesComprimidos := make([]byte, reader.Len())
	if _, err := reader.Read(indicesComprimidos); err != nil {
		return nil, fmt.Errorf("error leyendo índices comprimidos: %v", err)
	}

	// Descomprimir índices usando RLE
	compresorRLE := &CompresorRLEGenerico[uint16]{}
	indices, err := compresorRLE.Descomprimir(indicesComprimidos)
	if err != nil {
		return nil, fmt.Errorf("error al descomprimir índices: %v", err)
	}

	// Reconstruir strings usando el diccionario
	resultado := make([]string, len(indices))
	for i, idx := range indices {
		if idx >= numEntradas {
			return nil, fmt.Errorf("índice fuera de rango: %d (máximo %d)", idx, numEntradas-1)
		}
		resultado[i] = entradas[idx]
	}

	return resultado, nil
}
