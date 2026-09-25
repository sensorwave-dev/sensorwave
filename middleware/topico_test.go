package middleware

import "testing"

func TestNormalizarYValidarTopico(t *testing.T) {
	casos := []struct {
		entrada           string
		permitirWildcards bool
		esperado          string
		ok                bool
	}{
		{entrada: "/sensores/temp", permitirWildcards: false, esperado: "sensores/temp", ok: true},
		{entrada: "//sensores//temp//", permitirWildcards: false, esperado: "sensores/temp", ok: true},
		{entrada: "sensores/+/temp", permitirWildcards: true, esperado: "sensores/+/temp", ok: true},
		{entrada: "sensores/#", permitirWildcards: true, esperado: "sensores/#", ok: true},
		{entrada: "sensores/#/temp", permitirWildcards: true, ok: false},
		{entrada: "sensores/te+mp", permitirWildcards: true, ok: false},
		{entrada: "sensores/te#mp", permitirWildcards: true, ok: false},
		{entrada: "sensores/#", permitirWildcards: false, ok: false},
		{entrada: "", permitirWildcards: true, ok: false},
		{entrada: "/", permitirWildcards: true, ok: false},
	}

	for _, caso := range casos {
		got, err := NormalizarYValidarTopico(caso.entrada, caso.permitirWildcards)
		if caso.ok {
			if err != nil {
				t.Fatalf("NormalizarYValidarTopico(%q) error inesperado: %v", caso.entrada, err)
			}
			if got != caso.esperado {
				t.Fatalf("NormalizarYValidarTopico(%q) = %q, esperado %q", caso.entrada, got, caso.esperado)
			}
			continue
		}
		if err == nil {
			t.Fatalf("NormalizarYValidarTopico(%q) esperaba error", caso.entrada)
		}
	}
}

func TestEsTopicoControl(t *testing.T) {
	casos := []struct {
		topico   string
		esperado bool
	}{
		{"swctl/nodos/x/consulta/solicitud/1", true},
		{"swctl/", true},
		{"swctl", false},
		{"sensores/temp", false},
		{"", false},
	}
	for _, c := range casos {
		if got := EsTopicoControl(c.topico); got != c.esperado {
			t.Fatalf("EsTopicoControl(%q) = %v, esperado %v", c.topico, got, c.esperado)
		}
	}
}