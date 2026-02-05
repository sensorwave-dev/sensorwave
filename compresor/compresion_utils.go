package compresor

import (
	"fmt"

	"github.com/sensorwave-dev/sensorwave/tipos"
)

// Utilidades para conversión de bytes

// Int64ToBytes convierte un int64 a 8 bytes
func Int64ToBytes(i int64) []byte {
	bytes := make([]byte, 8)
	for j := 0; j < 8; j++ {
		bytes[j] = byte(i >> (56 - j*8))
	}
	return bytes
}

// BytesToInt64 convierte 8 bytes a int64
func BytesToInt64(bytes []byte) int64 {
	if len(bytes) < 8 {
		return 0
	}
	var i int64
	for j := 0; j < 8; j++ {
		i |= int64(bytes[j]) << (56 - j*8)
	}
	return i
}

// int32ToBytes convierte un int32 a 4 bytes
func int32ToBytes(i int32) []byte {
	bytes := make([]byte, 4)
	for j := 0; j < 4; j++ {
		bytes[j] = byte(i >> (24 - j*8))
	}
	return bytes
}

// bytesToInt32 convierte 4 bytes a int32
func bytesToInt32(bytes []byte) int32 {
	if len(bytes) < 4 {
		return 0
	}
	var i int32
	for j := 0; j < 4; j++ {
		i |= int32(bytes[j]) << (24 - j*8)
	}
	return i
}

// extraerValores extrae los valores de un slice de mediciones
func ExtraerValores(mediciones []tipos.Medicion) []interface{} {
	valores := make([]interface{}, len(mediciones))
	for i, medicion := range mediciones {
		valores[i] = medicion.Valor
	}
	return valores
}

// extraerTiempos extrae los tiempos de un slice de mediciones
func ExtraerTiempos(mediciones []tipos.Medicion) []int64 {
	tiempos := make([]int64, len(mediciones))
	for i, medicion := range mediciones {
		tiempos[i] = medicion.Tiempo
	}
	return tiempos
}

// Funciones de conversión de tipo para valores

// ConvertirAInt64Array convierte []interface{} a []int64
func ConvertirAInt64Array(valores []interface{}) ([]int64, error) {
	resultado := make([]int64, len(valores))
	for i, v := range valores {
		switch val := v.(type) {
		case int:
			resultado[i] = int64(val)
		case int32:
			resultado[i] = int64(val)
		case int64:
			resultado[i] = val
		case float64:
			resultado[i] = int64(val)
		default:
			return nil, fmt.Errorf("no se puede convertir tipo %T a int64 en índice %d", v, i)
		}
	}
	return resultado, nil
}

// ConvertirAFloat64Array convierte []interface{} a []float64
func ConvertirAFloat64Array(valores []interface{}) ([]float64, error) {
	resultado := make([]float64, len(valores))
	for i, v := range valores {
		switch val := v.(type) {
		case float32:
			resultado[i] = float64(val)
		case float64:
			resultado[i] = val
		case int:
			resultado[i] = float64(val)
		case int32:
			resultado[i] = float64(val)
		case int64:
			resultado[i] = float64(val)
		default:
			return nil, fmt.Errorf("no se puede convertir tipo %T a float64 en índice %d", v, i)
		}
	}
	return resultado, nil
}

// ConvertirAStringArray convierte []interface{} a []string
func ConvertirAStringArray(valores []interface{}) ([]string, error) {
	resultado := make([]string, len(valores))
	for i, v := range valores {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("no se puede convertir tipo %T a string en índice %d", v, i)
		}
		resultado[i] = str
	}
	return resultado, nil
}

// ConvertirABoolArray convierte []interface{} a []bool
func ConvertirABoolArray(valores []interface{}) ([]bool, error) {
	resultado := make([]bool, len(valores))
	for i, v := range valores {
		switch val := v.(type) {
		case bool:
			resultado[i] = val
		case int:
			resultado[i] = val != 0
		case int64:
			resultado[i] = val != 0
		case float64:
			resultado[i] = val != 0.0
		default:
			return nil, fmt.Errorf("no se puede convertir tipo %T a bool en índice %d", v, i)
		}
	}
	return resultado, nil
}

// combinarDatos combina datos de tiempo y valores comprimidos con metadata
func CombinarDatos(tiemposComprimidos, valoresComprimidos []byte) []byte {
	// Header: 4 bytes tamaño tiempos + 4 bytes tamaño valores
	tamañoTiempos := len(tiemposComprimidos)
	tamañoValores := len(valoresComprimidos)

	resultado := make([]byte, 8+tamañoTiempos+tamañoValores)

	// Header de tiempos (4 bytes)
	resultado[0] = byte(tamañoTiempos >> 24)
	resultado[1] = byte(tamañoTiempos >> 16)
	resultado[2] = byte(tamañoTiempos >> 8)
	resultado[3] = byte(tamañoTiempos)

	// Header de valores (4 bytes)
	resultado[4] = byte(tamañoValores >> 24)
	resultado[5] = byte(tamañoValores >> 16)
	resultado[6] = byte(tamañoValores >> 8)
	resultado[7] = byte(tamañoValores)

	// Datos de tiempos
	copy(resultado[8:8+tamañoTiempos], tiemposComprimidos)

	// Datos de valores
	copy(resultado[8+tamañoTiempos:], valoresComprimidos)

	return resultado
}

// separarDatos separa los datos combinados en tiempos y valores
func SepararDatos(datos []byte) ([]byte, []byte, error) {
	if len(datos) < 8 {
		return nil, nil, fmt.Errorf("datos insuficientes para separar")
	}

	// Leer headers
	tamañoTiempos := int(datos[0])<<24 | int(datos[1])<<16 | int(datos[2])<<8 | int(datos[3])
	tamañoValores := int(datos[4])<<24 | int(datos[5])<<16 | int(datos[6])<<8 | int(datos[7])

	if len(datos) < 8+tamañoTiempos+tamañoValores {
		return nil, nil, fmt.Errorf("tamaño de datos inconsistente")
	}

	// Extraer datos
	tiempos := make([]byte, tamañoTiempos)
	valores := make([]byte, tamañoValores)

	copy(tiempos, datos[8:8+tamañoTiempos])
	copy(valores, datos[8+tamañoTiempos:8+tamañoTiempos+tamañoValores])

	return tiempos, valores, nil
}
