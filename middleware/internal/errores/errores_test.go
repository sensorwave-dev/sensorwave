package errores

import (
	"errors"
	"fmt"
	"testing"
)

func TestSentinelas_NoNil(t *testing.T) {
	sentinels := []error{ErrConexion, ErrPublicacion, ErrACK, ErrSuscripcion, ErrDesuscripcion}
	for _, s := range sentinels {
		if s == nil {
			t.Error("sentinela nil")
		}
	}
}

func TestErrorACK_Error_Y_Unwrap(t *testing.T) {
	e := &ErrorACK{MensajeID: "abc-123"}
	if e.Error() == "" {
		t.Error("Error() vacío")
	}
	if !errors.Is(e, ErrACK) {
		t.Error("errors.Is(*ErrorACK, ErrACK) debería ser true")
	}
}

func TestErrorACK_As_ExtraeMensajeID(t *testing.T) {
	err := &ErrorACK{MensajeID: "xyz"}
	var target *ErrorACK
	if !errors.As(err, &target) {
		t.Fatal("errors.As debería extraer *ErrorACK")
	}
	if target.MensajeID != "xyz" {
		t.Errorf("MensajeID = %q, want %q", target.MensajeID, "xyz")
	}
}

func TestEs_Helper(t *testing.T) {
	if !Es(ErrPublicacion, ErrPublicacion) {
		t.Error("Es(ErrPublicacion, ErrPublicacion) debería ser true")
	}
	if Es(ErrPublicacion, ErrACK) {
		t.Error("Es(ErrPublicacion, ErrACK) debería ser false")
	}
}

func TestWrapping_CategorizaConErrorsIs(t *testing.T) {
	wrapped := fmt.Errorf("%w: detalle %v", ErrPublicacion, "algo")
	if !Es(wrapped, ErrPublicacion) {
		t.Error("un error envuelto con %%w debería satisfacer errors.Is(ErrPublicacion)")
	}
}

func TestErrorACK_NoEsOtrasCategorias(t *testing.T) {
	e := &ErrorACK{MensajeID: "1"}
	if Es(e, ErrPublicacion) {
		t.Error("ErrorACK no debería ser ErrPublicacion")
	}
	if Es(e, ErrConexion) {
		t.Error("ErrorACK no debería ser ErrConexion")
	}
}
