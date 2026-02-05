package servidor

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
		got, err := normalizarYValidarTopico(caso.entrada, caso.permitirWildcards)
		if caso.ok {
			if err != nil {
				t.Fatalf("normalizarYValidarTopico(%q) error inesperado: %v", caso.entrada, err)
			}
			if got != caso.esperado {
				t.Fatalf("normalizarYValidarTopico(%q) = %q, esperado %q", caso.entrada, got, caso.esperado)
			}
			continue
		}
		if err == nil {
			t.Fatalf("normalizarYValidarTopico(%q) esperaba error", caso.entrada)
		}
	}
}
