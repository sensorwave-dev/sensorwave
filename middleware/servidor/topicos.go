package servidor

import (
	"errors"
	"strings"
)

var errTopicoInvalido = errors.New("topico invalido")

// normalizarYValidarTopico limpia el topico y valida su formato.
// - Quita espacios, slash inicial/final y colapsa multiples slashes.
// - Si permitirWildcards es false, no admite + ni #.
// - Si permitirWildcards es true, valida sintaxis MQTT: + y # como segmentos completos, # solo al final.
func normalizarYValidarTopico(topico string, permitirWildcards bool) (string, error) {
	t := strings.TrimSpace(topico)
	if t == "" {
		return "", errTopicoInvalido
	}

	for strings.Contains(t, "//") {
		t = strings.ReplaceAll(t, "//", "/")
	}

	t = strings.TrimPrefix(t, "/")
	t = strings.TrimSuffix(t, "/")
	if t == "" {
		return "", errTopicoInvalido
	}

	partes := strings.Split(t, "/")
	for i, parte := range partes {
		if parte == "" {
			return "", errTopicoInvalido
		}
		if strings.Contains(parte, "#") && parte != "#" {
			return "", errTopicoInvalido
		}
		if strings.Contains(parte, "+") && parte != "+" {
			return "", errTopicoInvalido
		}
		if parte == "#" {
			if !permitirWildcards || i != len(partes)-1 {
				return "", errTopicoInvalido
			}
			continue
		}
		if parte == "+" {
			if !permitirWildcards {
				return "", errTopicoInvalido
			}
			continue
		}
		if !permitirWildcards && (strings.Contains(parte, "+") || strings.Contains(parte, "#")) {
			return "", errTopicoInvalido
		}
	}

	return t, nil
}
