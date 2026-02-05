package servidor

import "testing"

func TestCoincidePatron(t *testing.T) {
	casos := []struct {
		topico string
		patron string
		espera bool
		motivo string
	}{
		{topico: "casa/sala/temp", patron: "casa/+/temp", espera: true, motivo: "+ coincide un nivel"},
		{topico: "casa/sala/temp", patron: "casa/#", espera: true, motivo: "# coincide multiples niveles"},
		{topico: "casa", patron: "#", espera: true, motivo: "# coincide con todo"},
		{topico: "casa/sala/temp", patron: "casa/+/humedad", espera: false, motivo: "nivel final distinto"},
		{topico: "otra/casa/temp", patron: "casa/#", espera: false, motivo: "prefijo distinto"},
		{topico: "casa/sala/temp", patron: "casa/#/temp", espera: false, motivo: "# en medio"},
		{topico: "casa/sala/temp", patron: "casa/te+mp", espera: false, motivo: "+ no es segmento completo"},
		{topico: "casa/sala/temp", patron: "casa/te#mp", espera: false, motivo: "# no es segmento completo"},
	}

	for _, caso := range casos {
		t.Run(caso.motivo, func(t *testing.T) {
			got := coincidePatron(caso.topico, caso.patron)
			if got != caso.espera {
				t.Fatalf("coincidePatron(%q, %q) = %v, esperado %v", caso.topico, caso.patron, got, caso.espera)
			}
		})
	}
}
