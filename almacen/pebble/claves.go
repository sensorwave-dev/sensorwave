package pebble

import (
	"fmt"
	"strconv"
	"strings"
)

func claveIngesta(serieID int, tiempo int64) []byte {
	return fmt.Appendf(nil, "ingesta/%010d/%020d", serieID, tiempo)
}

func cotaIngesta(serieID int) (desde, hasta []byte) {
	desde = fmt.Appendf(nil, "ingesta/%010d/", serieID)
	hasta = fmt.Appendf(nil, "ingesta/%010d0", serieID)
	return desde, hasta
}

func claveDatos(serieID int, inicio, fin int64) []byte {
	return fmt.Appendf(nil, "datos/%010d/%020d_%020d", serieID, inicio, fin)
}

func cotaDatos(serieID int) (desde, hasta []byte) {
	desde = fmt.Appendf(nil, "datos/%010d/", serieID)
	hasta = fmt.Appendf(nil, "datos/%010d0", serieID)
	return desde, hasta
}

func claveSerie(path string) []byte {
	return []byte("series/" + path)
}

func claveRegla(id string) []byte {
	return []byte("reglas/" + id)
}

func clavePendiente(serieID int) []byte {
	return fmt.Appendf(nil, "pendientes/eliminar/%010d", serieID)
}

func tiempoDeClaveIngesta(clave string) (int64, bool) {
	resto, ok := strings.CutPrefix(clave, "ingesta/")
	if !ok {
		return 0, false
	}
	_, tiempo, ok := strings.Cut(resto, "/")
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(tiempo, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func parseClaveDatos(clave string) (inicio, fin int64, ok bool) {
	partes := strings.Split(clave, "/")
	if len(partes) != 3 || partes[0] != "datos" {
		return 0, 0, false
	}
	tiempos := strings.Split(partes[2], "_")
	if len(tiempos) != 2 {
		return 0, 0, false
	}
	inicio, err1 := strconv.ParseInt(tiempos[0], 10, 64)
	fin, err2 := strconv.ParseInt(tiempos[1], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return inicio, fin, true
}

func serieDeClavePendiente(clave string) (int, bool) {
	resto, ok := strings.CutPrefix(clave, "pendientes/eliminar/")
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(resto)
	if err != nil {
		return 0, false
	}
	return n, true
}
