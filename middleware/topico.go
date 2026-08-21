package middleware

import (
	"errors"
	"strings"
)

// ErrTopicoInvalido indica que el tópico/fliltro no supera la validación.
var ErrTopicoInvalido = errors.New("topico invalido")

// PrefijoControl identifica al plano de control federado.
// Debe mantenerse en sincronía con tipos.PrefijoControl (definido en
// tipos/control_federacion.go). Se duplica acá para no arrastrar las
// dependencias del paquete tipos (AWS SDK, pebble) hacia los clientes
// livianos (cliente_mqtt, etc.) que importan este paquete.
const PrefijoControl = "swctl/"

// EsTopicoControl indica si un tópico pertenece al plano de control federado.
func EsTopicoControl(topico string) bool {
	return strings.HasPrefix(topico, PrefijoControl)
}

// EsTopicoPermitidoParaProtocolo indica si un tópico puede usarse en un protocolo
// dado. swctl/# sólo está permitido en MQTT.
func EsTopicoPermitidoParaProtocolo(topico string, protocolo string) bool {
	if !EsTopicoControl(topico) {
		return true
	}
	return strings.ToLower(protocolo) == "mqtt"
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