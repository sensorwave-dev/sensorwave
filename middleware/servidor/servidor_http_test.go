package servidor

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMuxHTTP_MetodoNoPermitido(t *testing.T) {
	mux := muxHTTP()

	casos := []struct {
		metodo string
		ruta   string
	}{
		{http.MethodPut, "/sensorwave"},
		{http.MethodPatch, "/sensorwave"},
		{http.MethodHead, "/sensorwave"},
		{http.MethodGet, "/sensorwave/ack"},
	}

	for _, caso := range casos {
		t.Run(caso.metodo+" "+caso.ruta, func(t *testing.T) {
			req := httptest.NewRequest(caso.metodo, caso.ruta, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			if w.Code != http.StatusMethodNotAllowed {
				t.Fatalf("código = %d, esperado %d", w.Code, http.StatusMethodNotAllowed)
			}
		})
	}
}
