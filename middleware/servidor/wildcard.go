package servidor

import "strings"

// coincidePatron verifica si un topico de publicacion coincide con un patron de suscripcion.
// Soporta wildcards MQTT: + (un nivel) y # (multiples niveles, solo al final).
func coincidePatron(topico, patron string) bool {
	if patron == "" {
		return false
	}

	if patron == "#" {
		return true
	}

	if strings.Count(patron, "#") > 1 {
		return false
	}

	partesTopico := strings.Split(topico, "/")
	partesPatron := strings.Split(patron, "/")

	for i, partePatron := range partesPatron {
		if strings.Contains(partePatron, "#") && partePatron != "#" {
			return false
		}
		if strings.Contains(partePatron, "+") && partePatron != "+" {
			return false
		}

		if partePatron == "#" {
			return i == len(partesPatron)-1
		}

		if i >= len(partesTopico) {
			return false
		}

		if partePatron == "+" {
			continue
		}

		if partePatron != partesTopico[i] {
			return false
		}
	}

	return len(partesTopico) == len(partesPatron)
}
