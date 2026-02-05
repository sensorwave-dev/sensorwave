package compresor

import (
	"fmt"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

// compresionDeltaDeltaTiempo aplica compresión DeltaDelta específicamente para tiempos
// Siempre usa esta función para comprimir tiempos, independiente de la configuración de la serie
func CompresionDeltaDeltaTiempo(mediciones []tipos.Medicion) []byte {
	if len(mediciones) == 0 {
		return []byte{}
	}

	tiempos := ExtraerTiempos(mediciones)

	// Buffer para almacenar los datos comprimidos
	resultado := make([]byte, 0)

	// Almacenar el primer tiempo sin compresión (8 bytes)
	primerTiempo := tiempos[0]
	resultado = append(resultado, Int64ToBytes(primerTiempo)...)

	if len(tiempos) == 1 {
		return resultado
	}

	// Calcular y almacenar primera delta (8 bytes)
	var deltaTiempoAnterior int64
	if len(tiempos) > 1 {
		deltaTiempoAnterior = tiempos[1] - tiempos[0]
		resultado = append(resultado, Int64ToBytes(deltaTiempoAnterior)...)
	}

	if len(tiempos) == 2 {
		return resultado
	}

	// Aplicar compresión DeltaDelta para el resto de elementos (desde el índice 2)
	for i := 2; i < len(tiempos); i++ {
		tiempoActual := tiempos[i]

		// Calcular delta de tiempo
		deltaTiempo := tiempoActual - tiempos[i-1]
		deltaDeltaTiempo := deltaTiempo - deltaTiempoAnterior

		// Determinar cuántos bytes necesitamos para la delta-delta
		var deltaBytes []byte
		if deltaDeltaTiempo >= -128 && deltaDeltaTiempo <= 127 {
			// 1 byte + flag
			deltaBytes = append(deltaBytes, 0x00) // Flag para 1 byte
			deltaBytes = append(deltaBytes, byte(deltaDeltaTiempo))
		} else if deltaDeltaTiempo >= -32768 && deltaDeltaTiempo <= 32767 {
			// 2 bytes + flag
			deltaBytes = append(deltaBytes, 0x01) // Flag para 2 bytes
			deltaBytes = append(deltaBytes, byte(deltaDeltaTiempo>>8))
			deltaBytes = append(deltaBytes, byte(deltaDeltaTiempo))
		} else if deltaDeltaTiempo >= -2147483648 && deltaDeltaTiempo <= 2147483647 {
			// 4 bytes + flag
			deltaBytes = append(deltaBytes, 0x02) // Flag para 4 bytes
			deltaBytes = append(deltaBytes, int32ToBytes(int32(deltaDeltaTiempo))...)
		} else {
			// 8 bytes + flag
			deltaBytes = append(deltaBytes, 0x03) // Flag para 8 bytes
			deltaBytes = append(deltaBytes, Int64ToBytes(deltaDeltaTiempo)...)
		}

		resultado = append(resultado, deltaBytes...)

		// Actualizar delta anterior
		deltaTiempoAnterior = deltaTiempo
	}

	return resultado
}

// descompresionDeltaDeltaTiempo descomprime tiempos usando DeltaDelta
func DescompresionDeltaDeltaTiempo(datos []byte) ([]int64, error) {
	if len(datos) == 0 {
		return []int64{}, nil
	}

	if len(datos) < 8 {
		return nil, fmt.Errorf("datos insuficientes para descomprimir tiempo")
	}

	resultado := make([]int64, 0)
	offset := 0

	// Leer primer tiempo (8 bytes)
	primerTiempo := BytesToInt64(datos[offset : offset+8])
	resultado = append(resultado, primerTiempo)
	offset += 8

	if offset >= len(datos) {
		return resultado, nil
	}

	// Leer primera delta (8 bytes)
	if offset+8 > len(datos) {
		return nil, fmt.Errorf("datos insuficientes para primera delta")
	}

	deltaTiempoAnterior := BytesToInt64(datos[offset : offset+8])
	segundoTiempo := primerTiempo + deltaTiempoAnterior
	resultado = append(resultado, segundoTiempo)
	offset += 8

	if offset >= len(datos) {
		return resultado, nil
	}

	// Descomprimir el resto usando delta-delta
	tiempoAnterior := segundoTiempo

	for offset < len(datos) {
		if offset+1 > len(datos) {
			break
		}

		// Leer flag
		flag := datos[offset]
		offset++

		var deltaDeltaTiempo int64

		switch flag {
		case 0x00: // 1 byte
			if offset+1 > len(datos) {
				return nil, fmt.Errorf("datos insuficientes para delta-delta de 1 byte")
			}
			deltaDeltaTiempo = int64(int8(datos[offset]))
			offset++

		case 0x01: // 2 bytes
			if offset+2 > len(datos) {
				return nil, fmt.Errorf("datos insuficientes para delta-delta de 2 bytes")
			}
			deltaDeltaTiempo = int64(int16(datos[offset])<<8 | int16(datos[offset+1]))
			offset += 2

		case 0x02: // 4 bytes
			if offset+4 > len(datos) {
				return nil, fmt.Errorf("datos insuficientes para delta-delta de 4 bytes")
			}
			deltaDeltaTiempo = int64(bytesToInt32(datos[offset : offset+4]))
			offset += 4

		case 0x03: // 8 bytes
			if offset+8 > len(datos) {
				return nil, fmt.Errorf("datos insuficientes para delta-delta de 8 bytes")
			}
			deltaDeltaTiempo = BytesToInt64(datos[offset : offset+8])
			offset += 8

		default:
			return nil, fmt.Errorf("flag de compresión desconocido: %x", flag)
		}

		// Reconstruir el tiempo
		deltaTiempo := deltaTiempoAnterior + deltaDeltaTiempo
		tiempoActual := tiempoAnterior + deltaTiempo

		resultado = append(resultado, tiempoActual)

		// Actualizar para siguiente iteración
		deltaTiempoAnterior = deltaTiempo
		tiempoAnterior = tiempoActual
	}

	return resultado, nil
}
