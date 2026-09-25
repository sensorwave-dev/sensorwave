package middleware

import (
	"errors"
	"strings"
)

// ErrTopicoInvalido indica que el tópico/fliltro no supera la validación.
var ErrTopicoInvalido = errors.New("topico invalido")

// PrefijoControl identifica al plano de control federado (swctl/).
// Se define en este paquete (y no se importa desde tipos) para no
// arrastrar las dependencias pesadas de tipos hacia los clientes livianos.
const PrefijoControl = "swctl/"

// EsTopicoControl indica si un tópico pertenece al plano de control federado.
func EsTopicoControl(topico string) bool {
	return strings.HasPrefix(topico, PrefijoControl)
}

// NormalizarYValidarTopico canoniza el topico y valida su formato.
// - Quita espacios, slash inicial/final y colapsa multiples slashes.
// - Si permitirWildcards es false, no admite + ni #.
// - Si permitirWildcards es true, valida sintaxis MQTT: + y # como segmentos
//   completos, # solo al final.
// Si el topico normaliza bien, devuelve (canon, nil). Si es invalido,
// devuelve ("", ErrTopicoInvalido). Nunca devuelve canon y error a la vez.
func NormalizarYValidarTopico(topico string, permitirWildcards bool) (string, error) {
	t := strings.TrimSpace(topico)
	if t == "" {
		return "", ErrTopicoInvalido
	}

	for strings.Contains(t, "//") {
		t = strings.ReplaceAll(t, "//", "/")
	}

	t = strings.TrimPrefix(t, "/")
	t = strings.TrimSuffix(t, "/")
	if t == "" {
		return "", ErrTopicoInvalido
	}

	partes := strings.Split(t, "/")
	for i, parte := range partes {
		if parte == "" {
			return "", ErrTopicoInvalido
		}
		if strings.Contains(parte, "#") && parte != "#" {
			return "", ErrTopicoInvalido
		}
		if strings.Contains(parte, "+") && parte != "+" {
			return "", ErrTopicoInvalido
		}
		if parte == "#" {
			if !permitirWildcards || i != len(partes)-1 {
				return "", ErrTopicoInvalido
			}
			continue
		}
		if parte == "+" {
			if !permitirWildcards {
				return "", ErrTopicoInvalido
			}
			continue
		}
		if !permitirWildcards && (strings.Contains(parte, "+") || strings.Contains(parte, "#")) {
			return "", ErrTopicoInvalido
		}
	}

	return t, nil
}