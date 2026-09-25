package borde

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/sensorwave-dev/sensorwave/tipos"
)

// generarNodoID genera un ID único para el nodo borde
func generarNodoID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	UUID := uuid.New().String()
	return fmt.Sprintf("borde-%s-%s", hostname, UUID)
}

// generarClaveDatos genera una clave PebbleDB incluyendo el tipo de datos
func generarClaveDatos(serieId int, tiempoInicio, tiempoFin int64) []byte {
	clave := fmt.Sprintf("datos/%010d/%020d_%020d", serieId, tiempoInicio, tiempoFin)
	return []byte(clave)
}

// esPathValido valida que un path de serie tenga el formato correcto
func esPathValido(path string) bool {
	if path == "" || strings.HasPrefix(path, "/") || strings.HasSuffix(path, "/") {
		return false
	}

	// Verificar que no tenga componentes vacíos
	parts := strings.Split(path, "/")
	for _, part := range parts {
		if part == "" {
			return false
		}
		// Verificar caracteres válidos (solo alfanuméricos, _ y -)
		if !regexp.MustCompile(`^[a-zA-Z0-9_-]+$`).MatchString(part) {
			return false
		}
	}
	return true
}

// inferirTipo determina el tipo de datos basado en el valor proporcionado
func inferirTipo(valor any) tipos.TipoDatos {
	switch valor.(type) {
	case bool:
		return tipos.Boolean
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return tipos.Integer
	case float32, float64:
		return tipos.Real
	case string:
		return tipos.Text
	default:
		return tipos.Desconocido
	}
}

// esCompatibleConTipo verifica si un valor es compatible con el tipo de serie
func esCompatibleConTipo(valor any, tipoDatos tipos.TipoDatos) bool {
	tipoValor := inferirTipo(valor)

	if tipoValor == tipos.Desconocido {
		return false
	}

	return tipoValor == tipoDatos
}

// coincidirTags verifica si una serie tiene todos los tags especificados
func coincidirTags(serieTags, filterTags map[string]string) bool {
	if len(filterTags) == 0 {
		return true
	}

	for clave, valor := range filterTags {
		if serieValue, existe := serieTags[clave]; !existe || serieValue != valor {
			return false
		}
	}

	return true
}
