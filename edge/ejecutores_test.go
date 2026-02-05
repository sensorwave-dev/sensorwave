package edge

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/sensorwave-dev/sensorwave/middleware"
)

// --- Tests para ExtraerVariables ---

func TestExtraerVariables_Simple(t *testing.T) {
	variables := ExtraerVariables("actuadores/{nodo_id}/ventilador")

	if len(variables) != 1 {
		t.Fatalf("esperaba 1 variable, obtuvo %d", len(variables))
	}
	if variables[0] != "nodo_id" {
		t.Errorf("esperaba 'nodo_id', obtuvo '%s'", variables[0])
	}
}

func TestExtraerVariables_Multiple(t *testing.T) {
	variables := ExtraerVariables("actuadores/{nodo_id}/{tipo_actuador}/comando")

	if len(variables) != 2 {
		t.Fatalf("esperaba 2 variables, obtuvo %d", len(variables))
	}

	esperadas := map[string]bool{"nodo_id": true, "tipo_actuador": true}
	for _, v := range variables {
		if !esperadas[v] {
			t.Errorf("variable inesperada: '%s'", v)
		}
	}
}

func TestExtraerVariables_SinVariables(t *testing.T) {
	variables := ExtraerVariables("actuadores/nodo_01/ventilador")

	if len(variables) != 0 {
		t.Errorf("esperaba 0 variables, obtuvo %d", len(variables))
	}
}

func TestExtraerVariables_Duplicadas(t *testing.T) {
	variables := ExtraerVariables("{nodo}/{tipo}/{nodo}")

	if len(variables) != 2 {
		t.Errorf("esperaba 2 variables únicas, obtuvo %d", len(variables))
	}
}

func TestExtraerVariables_ConGuionBajo(t *testing.T) {
	variables := ExtraerVariables("{serie_0}/{serie_1}")

	if len(variables) != 2 {
		t.Fatalf("esperaba 2 variables, obtuvo %d", len(variables))
	}
	if variables[0] != "serie_0" || variables[1] != "serie_1" {
		t.Errorf("variables incorrectas: %v", variables)
	}
}

// --- Tests para ValidarPlantilla ---

func TestValidarPlantilla_Valida(t *testing.T) {
	casos := []string{
		"actuadores/{nodo_id}/ventilador",
		"{a}/{b}/{c}",
		"sin_variables",
		"",
		"{serie_0}",
		"{_interno}",
	}

	for _, caso := range casos {
		err := ValidarPlantilla(caso)
		if err != nil {
			t.Errorf("plantilla '%s' debería ser válida, error: %v", caso, err)
		}
	}
}

func TestValidarPlantilla_LlaveSinCerrar(t *testing.T) {
	err := ValidarPlantilla("actuadores/{nodo_id/ventilador")

	if err == nil {
		t.Error("esperaba error por llave sin cerrar")
	}
}

func TestValidarPlantilla_LlaveSinAbrir(t *testing.T) {
	err := ValidarPlantilla("actuadores/nodo_id}/ventilador")

	if err == nil {
		t.Error("esperaba error por llave sin abrir")
	}
}

func TestValidarPlantilla_LlavesAnidadas(t *testing.T) {
	err := ValidarPlantilla("actuadores/{{nodo_id}}/ventilador")

	if err == nil {
		t.Error("esperaba error por llaves anidadas")
	}
}

func TestValidarPlantilla_NombreVacio(t *testing.T) {
	err := ValidarPlantilla("actuadores/{}/ventilador")

	if err == nil {
		t.Error("esperaba error por nombre de variable vacío")
	}
}

func TestValidarPlantilla_NombreInvalido(t *testing.T) {
	casos := []string{
		"actuadores/{123abc}/ventilador",  // comienza con número
		"actuadores/{nodo-id}/ventilador", // contiene guión
		"actuadores/{nodo id}/ventilador", // contiene espacio
	}

	for _, caso := range casos {
		err := ValidarPlantilla(caso)
		if err == nil {
			t.Errorf("plantilla '%s' debería ser inválida", caso)
		}
	}
}

// --- Tests para ValidarVariablesRequeridas ---

func TestValidarVariablesRequeridas_TodasPresentes(t *testing.T) {
	plantilla := "actuadores/{nodo_id}/{tipo}"
	params := map[string]string{
		"nodo_id": "nodo_01",
		"tipo":    "ventilador",
	}

	err := ValidarVariablesRequeridas(plantilla, params)
	if err != nil {
		t.Errorf("error inesperado: %v", err)
	}
}

func TestValidarVariablesRequeridas_Faltantes(t *testing.T) {
	plantilla := "actuadores/{nodo_id}/{tipo}"
	params := map[string]string{
		"nodo_id": "nodo_01",
		// falta "tipo"
	}

	err := ValidarVariablesRequeridas(plantilla, params)
	if err == nil {
		t.Error("esperaba error por variable faltante")
	}
}

func TestValidarVariablesRequeridas_VariablesContexto(t *testing.T) {
	// Variables de contexto no necesitan estar en params
	plantilla := "actuadores/{serie_0}/{serie_1}"
	params := map[string]string{} // vacío

	err := ValidarVariablesRequeridas(plantilla, params)
	if err != nil {
		t.Errorf("variables de contexto no deberían requerir params: %v", err)
	}
}

func TestValidarVariablesRequeridas_Mixto(t *testing.T) {
	plantilla := "actuadores/{serie_0}/{comando}"
	params := map[string]string{
		"comando": "encender",
	}

	err := ValidarVariablesRequeridas(plantilla, params)
	if err != nil {
		t.Errorf("error inesperado: %v", err)
	}
}

func TestValidarVariablesRequeridas_ReglaId(t *testing.T) {
	plantilla := "alertas/{regla_id}/{regla_nombre}"
	params := map[string]string{}

	err := ValidarVariablesRequeridas(plantilla, params)
	if err != nil {
		t.Errorf("regla_id y regla_nombre son de contexto: %v", err)
	}
}

// --- Tests para ResolverPlantilla ---

func TestResolverPlantilla_SoloParams(t *testing.T) {
	plantilla := "actuadores/{nodo_id}/{tipo}"
	params := map[string]string{
		"nodo_id": "nodo_01",
		"tipo":    "ventilador",
	}

	resultado := ResolverPlantilla(plantilla, params, nil, nil)
	esperado := "actuadores/nodo_01/ventilador"

	if resultado != esperado {
		t.Errorf("esperaba '%s', obtuvo '%s'", esperado, resultado)
	}
}

func TestResolverPlantilla_ConContextoSerie(t *testing.T) {
	plantilla := "actuadores/{serie_0}/{serie_1}"
	params := map[string]string{}
	contexto := map[string]interface{}{
		"_serie":   "nodo_03/temperatura",
		"_serie_0": "nodo_03",
		"_serie_1": "temperatura",
	}

	resultado := ResolverPlantilla(plantilla, params, nil, contexto)
	esperado := "actuadores/nodo_03/temperatura"

	if resultado != esperado {
		t.Errorf("esperaba '%s', obtuvo '%s'", esperado, resultado)
	}
}

func TestResolverPlantilla_ConRegla(t *testing.T) {
	plantilla := "alertas/{regla_id}"
	params := map[string]string{}
	regla := &Regla{
		ID:     "control_temp",
		Nombre: "Control de Temperatura",
	}

	resultado := ResolverPlantilla(plantilla, params, regla, nil)
	esperado := "alertas/control_temp"

	if resultado != esperado {
		t.Errorf("esperaba '%s', obtuvo '%s'", esperado, resultado)
	}
}

func TestResolverPlantilla_Mixto(t *testing.T) {
	plantilla := "actuadores/{serie_0}/{tipo_actuador}"
	params := map[string]string{
		"tipo_actuador": "ventilador",
	}
	contexto := map[string]interface{}{
		"_serie_0": "nodo_03",
	}
	regla := &Regla{ID: "regla_1"}

	resultado := ResolverPlantilla(plantilla, params, regla, contexto)
	esperado := "actuadores/nodo_03/ventilador"

	if resultado != esperado {
		t.Errorf("esperaba '%s', obtuvo '%s'", esperado, resultado)
	}
}

func TestResolverPlantilla_VariableNoResuelta(t *testing.T) {
	plantilla := "actuadores/{nodo_id}/{inexistente}"
	params := map[string]string{
		"nodo_id": "nodo_01",
	}

	resultado := ResolverPlantilla(plantilla, params, nil, nil)
	esperado := "actuadores/nodo_01/" // inexistente se reemplaza con vacío

	if resultado != esperado {
		t.Errorf("esperaba '%s', obtuvo '%s'", esperado, resultado)
	}
}

func TestResolverPlantilla_SeriePrincipal(t *testing.T) {
	plantilla := "series/{serie}"
	contexto := map[string]interface{}{
		"_serie": "nodo_01/sensor/temp",
	}

	resultado := ResolverPlantilla(plantilla, nil, nil, contexto)
	esperado := "series/nodo_01/sensor/temp"

	if resultado != esperado {
		t.Errorf("esperaba '%s', obtuvo '%s'", esperado, resultado)
	}
}

// --- Mock de cliente middleware para tests ---

type mockCliente struct {
	mu        sync.Mutex
	mensajes  []mockMensaje
	conectado bool
}

type mockMensaje struct {
	topico  string
	payload []byte
}

func nuevoMockCliente() *mockCliente {
	return &mockCliente{
		mensajes:  make([]mockMensaje, 0),
		conectado: true,
	}
}

func (m *mockCliente) Desconectar() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conectado = false
}

func (m *mockCliente) Publicar(topico string, mensaje interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var data []byte
	switch v := mensaje.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		data, _ = json.Marshal(v)
	}

	m.mensajes = append(m.mensajes, mockMensaje{topico: topico, payload: data})
}

func (m *mockCliente) Suscribir(topico string, manejador middleware.CallbackFunc) {
	// No implementado para tests
}

func (m *mockCliente) Desuscribir(topico string) {
	// No implementado para tests
}

func (m *mockCliente) obtenerMensajes() []mockMensaje {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mensajes
}

// --- Tests para CrearEjecutorPublicar ---

func TestCrearEjecutorPublicar_Simple(t *testing.T) {
	cliente := nuevoMockCliente()
	ejecutor := CrearEjecutorPublicar(cliente)

	accion := Accion{
		Tipo:    "publicar_mqtt",
		Destino: "actuadores/nodo_01/ventilador",
		Params: map[string]string{
			"comando": "encender",
		},
	}
	regla := &Regla{ID: "test_regla", Nombre: "Test"}
	valores := map[string]interface{}{
		"nodo_01/temp": 30.5,
	}

	err := ejecutor(accion, regla, valores)
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	mensajes := cliente.obtenerMensajes()
	if len(mensajes) != 1 {
		t.Fatalf("esperaba 1 mensaje, obtuvo %d", len(mensajes))
	}

	if mensajes[0].topico != "actuadores/nodo_01/ventilador" {
		t.Errorf("tópico incorrecto: %s", mensajes[0].topico)
	}

	// Verificar payload
	var payload PayloadActuador
	if err := json.Unmarshal(mensajes[0].payload, &payload); err != nil {
		t.Fatalf("error deserializando payload: %v", err)
	}

	if payload.Comando != "encender" {
		t.Errorf("comando incorrecto: %s", payload.Comando)
	}
	if payload.ReglaID != "test_regla" {
		t.Errorf("regla_id incorrecto: %s", payload.ReglaID)
	}
}

func TestCrearEjecutorPublicar_ConPlantilla(t *testing.T) {
	cliente := nuevoMockCliente()
	ejecutor := CrearEjecutorPublicar(cliente)

	accion := Accion{
		Tipo:    "publicar_mqtt",
		Destino: "actuadores/{nodo_id}/{tipo}",
		Params: map[string]string{
			"nodo_id": "nodo_03",
			"tipo":    "ventilador",
			"comando": "encender",
		},
	}
	regla := &Regla{ID: "test_regla"}
	valores := map[string]interface{}{}

	err := ejecutor(accion, regla, valores)
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	mensajes := cliente.obtenerMensajes()
	if mensajes[0].topico != "actuadores/nodo_03/ventilador" {
		t.Errorf("tópico incorrecto: %s", mensajes[0].topico)
	}
}

func TestCrearEjecutorPublicar_ConContextoSerie(t *testing.T) {
	cliente := nuevoMockCliente()
	ejecutor := CrearEjecutorPublicar(cliente)

	accion := Accion{
		Tipo:    "publicar_mqtt",
		Destino: "actuadores/{serie_0}/ventilador",
		Params: map[string]string{
			"comando": "encender",
		},
	}
	regla := &Regla{ID: "test_regla"}
	valores := map[string]interface{}{
		"_serie":              "nodo_05/temperatura",
		"_serie_0":            "nodo_05",
		"_serie_1":            "temperatura",
		"nodo_05/temperatura": 32.5,
	}

	err := ejecutor(accion, regla, valores)
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	mensajes := cliente.obtenerMensajes()
	if mensajes[0].topico != "actuadores/nodo_05/ventilador" {
		t.Errorf("tópico incorrecto: esperaba 'actuadores/nodo_05/ventilador', obtuvo '%s'", mensajes[0].topico)
	}
}

func TestCrearEjecutorPublicar_ClienteNil(t *testing.T) {
	ejecutor := CrearEjecutorPublicar(nil)

	accion := Accion{Destino: "test"}
	regla := &Regla{ID: "test"}

	err := ejecutor(accion, regla, nil)
	if err == nil {
		t.Error("esperaba error por cliente nil")
	}
}

func TestCrearEjecutorPublicar_TopicoVacio(t *testing.T) {
	cliente := nuevoMockCliente()
	ejecutor := CrearEjecutorPublicar(cliente)

	accion := Accion{
		Tipo:    "publicar_mqtt",
		Destino: "{inexistente}", // Se resuelve a vacío
		Params:  map[string]string{},
	}
	regla := &Regla{ID: "test"}

	err := ejecutor(accion, regla, nil)
	if err == nil {
		t.Error("esperaba error por tópico vacío")
	}
}

func TestCrearEjecutorPublicar_ContextoFiltrado(t *testing.T) {
	cliente := nuevoMockCliente()
	ejecutor := CrearEjecutorPublicar(cliente)

	accion := Accion{
		Tipo:    "publicar_mqtt",
		Destino: "test/topico",
		Params: map[string]string{
			"comando": "test",
		},
	}
	regla := &Regla{ID: "test"}
	valores := map[string]interface{}{
		"_serie":         "interno", // Debería filtrarse
		"_serie_0":       "interno", // Debería filtrarse
		"sensor/temp":    30.5,      // Debería incluirse
		"sensor/humedad": 65.0,      // Debería incluirse
	}

	ejecutor(accion, regla, valores)

	mensajes := cliente.obtenerMensajes()
	var payload PayloadActuador
	json.Unmarshal(mensajes[0].payload, &payload)

	// Verificar que metadatos internos no están en contexto
	if _, existe := payload.Contexto["_serie"]; existe {
		t.Error("_serie no debería estar en el contexto del payload")
	}
	if _, existe := payload.Contexto["_serie_0"]; existe {
		t.Error("_serie_0 no debería estar en el contexto del payload")
	}

	// Verificar que datos públicos sí están
	if _, existe := payload.Contexto["sensor/temp"]; !existe {
		t.Error("sensor/temp debería estar en el contexto")
	}
}

func TestCrearEjecutorPublicar_ParametrosFiltrados(t *testing.T) {
	cliente := nuevoMockCliente()
	ejecutor := CrearEjecutorPublicar(cliente)

	accion := Accion{
		Tipo:    "publicar_mqtt",
		Destino: "test/topico",
		Params: map[string]string{
			"comando":   "encender", // No debería estar en Parametros (es campo principal)
			"velocidad": "alta",     // Debería estar en Parametros
			"duracion":  "300",      // Debería estar en Parametros
		},
	}
	regla := &Regla{ID: "test"}

	ejecutor(accion, regla, nil)

	mensajes := cliente.obtenerMensajes()
	var payload PayloadActuador
	json.Unmarshal(mensajes[0].payload, &payload)

	// Verificar que comando está como campo principal
	if payload.Comando != "encender" {
		t.Errorf("comando incorrecto: %s", payload.Comando)
	}

	// Verificar que comando no está duplicado en Parametros
	if _, existe := payload.Parametros["comando"]; existe {
		t.Error("comando no debería estar en Parametros")
	}

	// Verificar otros parámetros
	if payload.Parametros["velocidad"] != "alta" {
		t.Error("velocidad debería estar en Parametros")
	}
}
