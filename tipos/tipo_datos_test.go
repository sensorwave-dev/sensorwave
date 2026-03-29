package tipos

import (
	"encoding/json"
	"testing"
)

// ==================== Tests de TipoDatos.String() ====================

// TestTipoDatos_String_Boolean verifica String() para Boolean
func TestTipoDatos_String_Boolean(t *testing.T) {
	resultado := Boolean.String()
	if resultado != "Boolean" {
		t.Errorf("Boolean.String() esperado 'Boolean', obtenido '%s'", resultado)
	}
	t.Log("✓ Boolean.String() retorna 'Boolean'")
}

// TestTipoDatos_String_Integer verifica String() para Integer
func TestTipoDatos_String_Integer(t *testing.T) {
	resultado := Integer.String()
	if resultado != "Integer" {
		t.Errorf("Integer.String() esperado 'Integer', obtenido '%s'", resultado)
	}
	t.Log("✓ Integer.String() retorna 'Integer'")
}

// TestTipoDatos_String_Real verifica String() para Real
func TestTipoDatos_String_Real(t *testing.T) {
	resultado := Real.String()
	if resultado != "Real" {
		t.Errorf("Real.String() esperado 'Real', obtenido '%s'", resultado)
	}
	t.Log("✓ Real.String() retorna 'Real'")
}

// TestTipoDatos_String_Text verifica String() para Text
func TestTipoDatos_String_Text(t *testing.T) {
	resultado := Text.String()
	if resultado != "Text" {
		t.Errorf("Text.String() esperado 'Text', obtenido '%s'", resultado)
	}
	t.Log("✓ Text.String() retorna 'Text'")
}

// TestTipoDatos_String_Desconocido verifica String() para Desconocido
func TestTipoDatos_String_Desconocido(t *testing.T) {
	resultado := Desconocido.String()
	if resultado != "" {
		t.Errorf("Desconocido.String() esperado '', obtenido '%s'", resultado)
	}
	t.Log("✓ Desconocido.String() retorna cadena vacía")
}

// TestTipoDatos_String_TodosTipos verifica String() para todos los tipos en tabla
func TestTipoDatos_String_TodosTipos(t *testing.T) {
	casosDePrueba := []struct {
		tipo     TipoDatos
		esperado string
	}{
		{Boolean, "Boolean"},
		{Integer, "Integer"},
		{Real, "Real"},
		{Text, "Text"},
		{Desconocido, ""},
	}

	for _, tc := range casosDePrueba {
		t.Run(tc.esperado, func(t *testing.T) {
			resultado := tc.tipo.String()
			if resultado != tc.esperado {
				t.Errorf("esperado '%s', obtenido '%s'", tc.esperado, resultado)
			}
		})
	}
}

// ==================== Tests de TipoDatos.AlgoritmosCompresion() ====================

// TestTipoDatos_AlgoritmosCompresion_Boolean verifica algoritmos para Boolean
func TestTipoDatos_AlgoritmosCompresion_Boolean(t *testing.T) {
	algoritmos := Boolean.AlgoritmosCompresion()

	if len(algoritmos) != 2 {
		t.Errorf("Boolean debería tener 2 algoritmos, tiene %d", len(algoritmos))
	}

	// Verificar que contiene SinCompresion y RLE
	tieneNinguno := false
	tieneRLE := false
	for _, alg := range algoritmos {
		if alg == SinCompresion {
			tieneNinguno = true
		}
		if alg == RLE {
			tieneRLE = true
		}
	}

	if !tieneNinguno {
		t.Error("Boolean debería soportar SinCompresion")
	}
	if !tieneRLE {
		t.Error("Boolean debería soportar RLE")
	}

	t.Logf("✓ Boolean tiene %d algoritmos: %v", len(algoritmos), algoritmos)
}

// TestTipoDatos_AlgoritmosCompresion_Integer verifica algoritmos para Integer
func TestTipoDatos_AlgoritmosCompresion_Integer(t *testing.T) {
	algoritmos := Integer.AlgoritmosCompresion()

	if len(algoritmos) != 4 {
		t.Errorf("Integer debería tener 4 algoritmos, tiene %d", len(algoritmos))
	}

	// Verificar algoritmos esperados
	esperados := map[TipoCompresion]bool{
		SinCompresion: false,
		DeltaDelta:    false,
		RLE:           false,
		Bits:          false,
	}

	for _, alg := range algoritmos {
		if _, existe := esperados[alg]; existe {
			esperados[alg] = true
		}
	}

	for alg, encontrado := range esperados {
		if !encontrado {
			t.Errorf("Integer debería soportar %s", alg)
		}
	}

	t.Logf("✓ Integer tiene %d algoritmos: %v", len(algoritmos), algoritmos)
}

// TestTipoDatos_AlgoritmosCompresion_Real verifica algoritmos para Real
func TestTipoDatos_AlgoritmosCompresion_Real(t *testing.T) {
	algoritmos := Real.AlgoritmosCompresion()

	if len(algoritmos) != 4 {
		t.Errorf("Real debería tener 4 algoritmos, tiene %d", len(algoritmos))
	}

	// Verificar algoritmos esperados
	esperados := map[TipoCompresion]bool{
		SinCompresion: false,
		DeltaDelta:    false,
		Xor:           false,
		RLE:           false,
	}

	for _, alg := range algoritmos {
		if _, existe := esperados[alg]; existe {
			esperados[alg] = true
		}
	}

	for alg, encontrado := range esperados {
		if !encontrado {
			t.Errorf("Real debería soportar %s", alg)
		}
	}

	t.Logf("✓ Real tiene %d algoritmos: %v", len(algoritmos), algoritmos)
}

// TestTipoDatos_AlgoritmosCompresion_Text verifica algoritmos para Text
func TestTipoDatos_AlgoritmosCompresion_Text(t *testing.T) {
	algoritmos := Text.AlgoritmosCompresion()

	if len(algoritmos) != 3 {
		t.Errorf("Text debería tener 3 algoritmos, tiene %d", len(algoritmos))
	}

	// Verificar algoritmos esperados
	esperados := map[TipoCompresion]bool{
		SinCompresion: false,
		RLE:           false,
		Diccionario:   false,
	}

	for _, alg := range algoritmos {
		if _, existe := esperados[alg]; existe {
			esperados[alg] = true
		}
	}

	for alg, encontrado := range esperados {
		if !encontrado {
			t.Errorf("Text debería soportar %s", alg)
		}
	}

	t.Logf("✓ Text tiene %d algoritmos: %v", len(algoritmos), algoritmos)
}

// TestTipoDatos_AlgoritmosCompresion_Desconocido verifica algoritmos para Desconocido
func TestTipoDatos_AlgoritmosCompresion_Desconocido(t *testing.T) {
	algoritmos := Desconocido.AlgoritmosCompresion()

	if len(algoritmos) != 1 {
		t.Errorf("Desconocido debería tener 1 algoritmo (SinCompresion), tiene %d", len(algoritmos))
	}

	if algoritmos[0] != SinCompresion {
		t.Errorf("Desconocido debería tener SinCompresion, tiene %s", algoritmos[0])
	}

	t.Log("✓ Desconocido solo soporta SinCompresion")
}

// ==================== Tests de TipoDatos.ValidarCompresion() ====================

// TestTipoDatos_ValidarCompresion_BooleanValido verifica compresión válida para Boolean
func TestTipoDatos_ValidarCompresion_BooleanValido(t *testing.T) {
	// SinCompresion debería ser válido
	err := Boolean.ValidarCompresion(SinCompresion)
	if err != nil {
		t.Errorf("SinCompresion debería ser válido para Boolean: %v", err)
	}

	// RLE debería ser válido
	err = Boolean.ValidarCompresion(RLE)
	if err != nil {
		t.Errorf("RLE debería ser válido para Boolean: %v", err)
	}

	t.Log("✓ Boolean acepta SinCompresion y RLE")
}

// TestTipoDatos_ValidarCompresion_BooleanInvalido verifica compresión inválida para Boolean
func TestTipoDatos_ValidarCompresion_BooleanInvalido(t *testing.T) {
	// DeltaDelta no debería ser válido para Boolean
	err := Boolean.ValidarCompresion(DeltaDelta)
	if err == nil {
		t.Error("DeltaDelta no debería ser válido para Boolean")
	} else {
		t.Logf("✓ Error esperado: %v", err)
	}

	// Xor no debería ser válido para Boolean
	err = Boolean.ValidarCompresion(Xor)
	if err == nil {
		t.Error("Xor no debería ser válido para Boolean")
	}

	// Diccionario no debería ser válido para Boolean
	err = Boolean.ValidarCompresion(Diccionario)
	if err == nil {
		t.Error("Diccionario no debería ser válido para Boolean")
	}

	t.Log("✓ Boolean rechaza algoritmos inválidos")
}

// TestTipoDatos_ValidarCompresion_IntegerValido verifica compresión válida para Integer
func TestTipoDatos_ValidarCompresion_IntegerValido(t *testing.T) {
	algoritmosValidos := []TipoCompresion{SinCompresion, DeltaDelta, RLE, Bits}

	for _, alg := range algoritmosValidos {
		err := Integer.ValidarCompresion(alg)
		if err != nil {
			t.Errorf("%s debería ser válido para Integer: %v", alg, err)
		}
	}

	t.Log("✓ Integer acepta SinCompresion, DeltaDelta, RLE y Bits")
}

// TestTipoDatos_ValidarCompresion_IntegerInvalido verifica compresión inválida para Integer
func TestTipoDatos_ValidarCompresion_IntegerInvalido(t *testing.T) {
	// Xor no debería ser válido para Integer
	err := Integer.ValidarCompresion(Xor)
	if err == nil {
		t.Error("Xor no debería ser válido para Integer")
	}

	// Diccionario no debería ser válido para Integer
	err = Integer.ValidarCompresion(Diccionario)
	if err == nil {
		t.Error("Diccionario no debería ser válido para Integer")
	}

	t.Log("✓ Integer rechaza Xor y Diccionario")
}

// TestTipoDatos_ValidarCompresion_RealValido verifica compresión válida para Real
func TestTipoDatos_ValidarCompresion_RealValido(t *testing.T) {
	algoritmosValidos := []TipoCompresion{SinCompresion, DeltaDelta, Xor, RLE}

	for _, alg := range algoritmosValidos {
		err := Real.ValidarCompresion(alg)
		if err != nil {
			t.Errorf("%s debería ser válido para Real: %v", alg, err)
		}
	}

	t.Log("✓ Real acepta SinCompresion, DeltaDelta, Xor y RLE")
}

// TestTipoDatos_ValidarCompresion_RealInvalido verifica compresión inválida para Real
func TestTipoDatos_ValidarCompresion_RealInvalido(t *testing.T) {
	// Bits no debería ser válido para Real
	err := Real.ValidarCompresion(Bits)
	if err == nil {
		t.Error("Bits no debería ser válido para Real")
	}

	// Diccionario no debería ser válido para Real
	err = Real.ValidarCompresion(Diccionario)
	if err == nil {
		t.Error("Diccionario no debería ser válido para Real")
	}

	t.Log("✓ Real rechaza Bits y Diccionario")
}

// TestTipoDatos_ValidarCompresion_TextValido verifica compresión válida para Text
func TestTipoDatos_ValidarCompresion_TextValido(t *testing.T) {
	algoritmosValidos := []TipoCompresion{SinCompresion, RLE, Diccionario}

	for _, alg := range algoritmosValidos {
		err := Text.ValidarCompresion(alg)
		if err != nil {
			t.Errorf("%s debería ser válido para Text: %v", alg, err)
		}
	}

	t.Log("✓ Text acepta SinCompresion, RLE y Diccionario")
}

// TestTipoDatos_ValidarCompresion_TextInvalido verifica compresión inválida para Text
func TestTipoDatos_ValidarCompresion_TextInvalido(t *testing.T) {
	algoritmosInvalidos := []TipoCompresion{DeltaDelta, Xor, Bits}

	for _, alg := range algoritmosInvalidos {
		err := Text.ValidarCompresion(alg)
		if err == nil {
			t.Errorf("%s no debería ser válido para Text", alg)
		}
	}

	t.Log("✓ Text rechaza DeltaDelta, Xor y Bits")
}

// TestTipoDatos_ValidarCompresion_DesconocidoSoloNinguno verifica que Desconocido solo acepta SinCompresion
func TestTipoDatos_ValidarCompresion_DesconocidoSoloNinguno(t *testing.T) {
	// SinCompresion debería ser válido
	err := Desconocido.ValidarCompresion(SinCompresion)
	if err != nil {
		t.Errorf("SinCompresion debería ser válido para Desconocido: %v", err)
	}

	// Cualquier otro algoritmo debería fallar
	err = Desconocido.ValidarCompresion(RLE)
	if err == nil {
		t.Error("RLE no debería ser válido para Desconocido")
	}

	t.Log("✓ Desconocido solo acepta SinCompresion")
}

// TestTipoDatos_ValidarCompresion_MensajeError verifica el formato del mensaje de error
func TestTipoDatos_ValidarCompresion_MensajeError(t *testing.T) {
	err := Boolean.ValidarCompresion(Xor)
	if err == nil {
		t.Fatal("Se esperaba error")
	}

	mensajeError := err.Error()
	// Verificar que el mensaje contiene información útil
	if len(mensajeError) < 20 {
		t.Errorf("Mensaje de error muy corto: %s", mensajeError)
	}

	t.Logf("✓ Mensaje de error descriptivo: %s", mensajeError)
}

// ==================== Tests de GobEncode/GobDecode para TipoDatos ====================

// TestTipoDatos_GobEncode verifica la serialización de TipoDatos
func TestTipoDatos_GobEncode(t *testing.T) {
	casosDePrueba := []TipoDatos{Boolean, Integer, Real, Text, Desconocido}

	for _, td := range casosDePrueba {
		t.Run(td.String(), func(t *testing.T) {
			data, err := td.GobEncode()
			if err != nil {
				t.Fatalf("Error en GobEncode: %v", err)
			}

			if td != Desconocido && len(data) == 0 {
				t.Error("GobEncode retornó datos vacíos para tipo no-desconocido")
			}
		})
	}

	t.Log("✓ GobEncode funciona para todos los tipos")
}

// TestTipoDatos_GobDecode verifica la deserialización de TipoDatos
func TestTipoDatos_GobDecode(t *testing.T) {
	original := Integer

	// Codificar
	data, err := original.GobEncode()
	if err != nil {
		t.Fatalf("Error en GobEncode: %v", err)
	}

	// Decodificar
	var decodificado TipoDatos
	err = decodificado.GobDecode(data)
	if err != nil {
		t.Fatalf("Error en GobDecode: %v", err)
	}

	if decodificado != original {
		t.Errorf("Tipo decodificado incorrecto: esperado %s, obtenido %s",
			original.String(), decodificado.String())
	}

	t.Logf("✓ GobEncode/GobDecode roundtrip correcto: %s", decodificado.String())
}

// TestTipoDatos_GobRoundtrip_TodosTipos verifica roundtrip para todos los tipos
func TestTipoDatos_GobRoundtrip_TodosTipos(t *testing.T) {
	tipos := []TipoDatos{Boolean, Integer, Real, Text, Desconocido}

	for _, original := range tipos {
		t.Run(original.String(), func(t *testing.T) {
			data, err := original.GobEncode()
			if err != nil {
				t.Fatalf("Error en GobEncode: %v", err)
			}

			var decodificado TipoDatos
			err = decodificado.GobDecode(data)
			if err != nil {
				t.Fatalf("Error en GobDecode: %v", err)
			}

			if decodificado != original {
				t.Errorf("Roundtrip falló: esperado %v, obtenido %v", original, decodificado)
			}
		})
	}
}

// ==================== Tests de MarshalJSON/UnmarshalJSON para TipoDatos ====================

// TestTipoDatos_MarshalJSON verifica la serialización JSON de TipoDatos
func TestTipoDatos_MarshalJSON(t *testing.T) {
	casosDePrueba := []struct {
		tipo     TipoDatos
		esperado string
	}{
		{Boolean, `"Boolean"`},
		{Integer, `"Integer"`},
		{Real, `"Real"`},
		{Text, `"Text"`},
		{Desconocido, `""`},
	}

	for _, tc := range casosDePrueba {
		t.Run(tc.tipo.String(), func(t *testing.T) {
			data, err := json.Marshal(tc.tipo)
			if err != nil {
				t.Fatalf("Error en MarshalJSON: %v", err)
			}

			if string(data) != tc.esperado {
				t.Errorf("MarshalJSON incorrecto: esperado %s, obtenido %s", tc.esperado, string(data))
			}
		})
	}

	t.Log("✓ MarshalJSON funciona para todos los tipos")
}

// TestTipoDatos_UnmarshalJSON verifica la deserialización JSON de TipoDatos
func TestTipoDatos_UnmarshalJSON(t *testing.T) {
	casosDePrueba := []struct {
		json     string
		esperado TipoDatos
	}{
		{`"Boolean"`, Boolean},
		{`"Integer"`, Integer},
		{`"Real"`, Real},
		{`"Text"`, Text},
		{`""`, Desconocido},
	}

	for _, tc := range casosDePrueba {
		t.Run(tc.esperado.String(), func(t *testing.T) {
			var resultado TipoDatos
			err := json.Unmarshal([]byte(tc.json), &resultado)
			if err != nil {
				t.Fatalf("Error en UnmarshalJSON: %v", err)
			}

			if resultado != tc.esperado {
				t.Errorf("UnmarshalJSON incorrecto: esperado %v, obtenido %v", tc.esperado, resultado)
			}
		})
	}

	t.Log("✓ UnmarshalJSON funciona para todos los tipos")
}

// TestTipoDatos_JSONRoundtrip verifica roundtrip JSON para todos los tipos
func TestTipoDatos_JSONRoundtrip(t *testing.T) {
	tipos := []TipoDatos{Boolean, Integer, Real, Text, Desconocido}

	for _, original := range tipos {
		t.Run(original.String(), func(t *testing.T) {
			data, err := json.Marshal(original)
			if err != nil {
				t.Fatalf("Error en MarshalJSON: %v", err)
			}

			var decodificado TipoDatos
			err = json.Unmarshal(data, &decodificado)
			if err != nil {
				t.Fatalf("Error en UnmarshalJSON: %v", err)
			}

			if decodificado != original {
				t.Errorf("Roundtrip JSON falló: esperado %v, obtenido %v", original, decodificado)
			}
		})
	}

	t.Log("✓ JSON roundtrip funciona para todos los tipos")
}

// TestTipoDatos_JSONEnEstructura verifica serialización JSON cuando TipoDatos está en una estructura
func TestTipoDatos_JSONEnEstructura(t *testing.T) {
	type Ejemplo struct {
		Nombre string    `json:"nombre"`
		Tipo   TipoDatos `json:"tipo"`
	}

	original := Ejemplo{
		Nombre: "temperatura",
		Tipo:   Real,
	}

	// Serializar
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Error serializando estructura: %v", err)
	}

	// Verificar que el JSON contiene el tipo correctamente
	esperado := `{"nombre":"temperatura","tipo":"Real"}`
	if string(data) != esperado {
		t.Errorf("JSON incorrecto: esperado %s, obtenido %s", esperado, string(data))
	}

	// Deserializar
	var recuperado Ejemplo
	err = json.Unmarshal(data, &recuperado)
	if err != nil {
		t.Fatalf("Error deserializando estructura: %v", err)
	}

	if recuperado.Tipo != original.Tipo {
		t.Errorf("TipoDatos no se recuperó correctamente: esperado %v, obtenido %v",
			original.Tipo, recuperado.Tipo)
	}

	t.Log("✓ TipoDatos se serializa/deserializa correctamente dentro de estructuras")
}
