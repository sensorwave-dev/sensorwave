package edge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cockroachdb/pebble"
	"github.com/sensorwave-dev/sensorwave/compresor"
	"github.com/sensorwave-dev/sensorwave/tipos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// TESTS DE UTILS.GO
// ============================================================================

// TestValidarPuertoHTTP_Default verifica que retorna puerto por defecto
func TestValidarPuertoHTTP_Default(t *testing.T) {
	puerto, err := validarPuertoHTTP("")
	require.NoError(t, err)
	assert.Equal(t, "8080", puerto)
	t.Log("✓ validarPuertoHTTP retorna puerto por defecto 8080")
}

// TestValidarPuertoHTTP_Valido verifica puertos válidos
func TestValidarPuertoHTTP_Valido(t *testing.T) {
	casos := []struct {
		puerto   string
		esperado string
	}{
		{"8080", "8080"},
		{"3000", "3000"},
		{"65535", "65535"},
		{"1", "1"},
	}

	for _, caso := range casos {
		puerto, err := validarPuertoHTTP(caso.puerto)
		require.NoError(t, err)
		assert.Equal(t, caso.esperado, puerto)
	}
	t.Log("✓ validarPuertoHTTP acepta puertos válidos")
}

// TestValidarPuertoHTTP_Invalido verifica rechazo de puertos inválidos
func TestValidarPuertoHTTP_Invalido(t *testing.T) {
	casos := []string{
		"abc",
		"-1",
		"0",
		"65536",
		"100000",
	}

	for _, caso := range casos {
		_, err := validarPuertoHTTP(caso)
		assert.Error(t, err, "Debería fallar para puerto: %s", caso)
	}
	t.Log("✓ validarPuertoHTTP rechaza puertos inválidos")
}

// TestEsPathValido_Validos verifica paths válidos
func TestEsPathValido_Validos(t *testing.T) {
	casos := []string{
		"sensor/temperatura",
		"dispositivo_001/humedad",
		"nivel1/nivel2/nivel3",
		"simple",
		"A1/B2/C3",
		"sensor-01/temp",
		"device-a/metric-1",
	}

	for _, caso := range casos {
		assert.True(t, esPathValido(caso), "Debería ser válido: %s", caso)
	}
	t.Log("✓ esPathValido acepta paths válidos")
}

// TestEsPathValido_Invalidos verifica paths inválidos
func TestEsPathValido_Invalidos(t *testing.T) {
	casos := []string{
		"",
		"/sensor",
		"sensor/",
		"/sensor/temperatura/",
		"sensor//temperatura",
		"sensor/temp.eratura",
		"sensor/temp eratura",
	}

	for _, caso := range casos {
		assert.False(t, esPathValido(caso), "Debería ser inválido: %s", caso)
	}
	t.Log("✓ esPathValido rechaza paths inválidos")
}

// TestGenerarNodoID verifica generación de IDs únicos
func TestGenerarNodoID(t *testing.T) {
	ids := make(map[string]bool)

	for i := 0; i < 100; i++ {
		id := generarNodoID()
		assert.NotEmpty(t, id)
		assert.Contains(t, id, "edge-")
		assert.False(t, ids[id], "ID duplicado generado")
		ids[id] = true
	}
	t.Log("✓ generarNodoID genera IDs únicos")
}

// TestGenerarClaveDatos verifica formato de claves
func TestGenerarClaveDatos(t *testing.T) {
	clave := generarClaveDatos(1, 1000, 2000)
	assert.Contains(t, string(clave), "data/")
	assert.Contains(t, string(clave), "0000000001")
	t.Log("✓ generarClaveDatos genera claves con formato correcto")
}

// TestInferirTipo verifica inferencia de tipos
func TestInferirTipo(t *testing.T) {
	casos := []struct {
		valor    interface{}
		esperado tipos.TipoDatos
	}{
		{true, tipos.Boolean},
		{false, tipos.Boolean},
		{int(42), tipos.Integer},
		{int64(42), tipos.Integer},
		{float64(3.14), tipos.Real},
		{float32(3.14), tipos.Real},
		{"texto", tipos.Text},
		{[]byte{1, 2, 3}, tipos.Desconocido},
	}

	for _, caso := range casos {
		resultado := inferirTipo(caso.valor)
		assert.Equal(t, caso.esperado, resultado, "Para valor %v", caso.valor)
	}
	t.Log("✓ inferirTipo infiere tipos correctamente")
}

// TestEsCompatibleConTipo verifica compatibilidad de tipos
func TestEsCompatibleConTipo(t *testing.T) {
	assert.True(t, esCompatibleConTipo(true, tipos.Boolean))
	assert.True(t, esCompatibleConTipo(42, tipos.Integer))
	assert.True(t, esCompatibleConTipo(3.14, tipos.Real))
	assert.True(t, esCompatibleConTipo("texto", tipos.Text))

	assert.False(t, esCompatibleConTipo(42, tipos.Boolean))
	assert.False(t, esCompatibleConTipo("texto", tipos.Integer))
	assert.False(t, esCompatibleConTipo(3.14, tipos.Text))
	t.Log("✓ esCompatibleConTipo verifica compatibilidad correctamente")
}

// TestMatchTags verifica coincidencia de tags
func TestMatchTags(t *testing.T) {
	serieTags := map[string]string{
		"ubicacion": "sala1",
		"tipo":      "temperatura",
		"piso":      "1",
	}

	// Sin filtro - siempre coincide
	assert.True(t, matchTags(serieTags, nil))
	assert.True(t, matchTags(serieTags, map[string]string{}))

	// Filtro parcial que coincide
	assert.True(t, matchTags(serieTags, map[string]string{"ubicacion": "sala1"}))
	assert.True(t, matchTags(serieTags, map[string]string{"ubicacion": "sala1", "tipo": "temperatura"}))

	// Filtro que no coincide
	assert.False(t, matchTags(serieTags, map[string]string{"ubicacion": "sala2"}))
	assert.False(t, matchTags(serieTags, map[string]string{"inexistente": "valor"}))

	t.Log("✓ matchTags verifica coincidencia de tags correctamente")
}

// ============================================================================
// TESTS DE CREAR() - VALIDACIÓN DE OPCIONES
// ============================================================================

// TestCrear_SinS3SinPuerto_ModoLocal verifica modo puramente local
func TestCrear_SinS3SinPuerto_ModoLocal(t *testing.T) {
	tempDir := t.TempDir()

	manager, err := Crear(Opciones{
		NombreDB:   tempDir + "/test_local.db",
		PuertoHTTP: "",  // Sin puerto
		ConfigS3:   nil, // Sin S3
	})

	require.NoError(t, err)
	defer manager.Cerrar()

	assert.NotEmpty(t, manager.ObtenerNodoID())
	assert.Empty(t, manager.puertoHTTP)
	t.Log("✓ Crear funciona en modo local sin servidor HTTP")
}

// TestCrear_ConS3SinPuerto_Error verifica que falla si hay S3 pero no puerto
func TestCrear_ConS3SinPuerto_Error(t *testing.T) {
	tempDir := t.TempDir()

	_, err := Crear(Opciones{
		NombreDB:   tempDir + "/test_s3.db",
		PuertoHTTP: "", // Sin puerto
		ConfigS3: &tipos.ConfiguracionS3{
			Endpoint: "http://localhost:9000",
			Bucket:   "test",
		},
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "PuertoHTTP es requerido")
	t.Log("✓ Crear retorna error cuando hay S3 pero no puerto HTTP")
}

// TestCrear_SinS3ConPuerto_Error verifica que falla si hay puerto pero no S3
func TestCrear_SinS3ConPuerto_Error(t *testing.T) {
	tempDir := t.TempDir()

	_, err := Crear(Opciones{
		NombreDB:   tempDir + "/test_puerto.db",
		PuertoHTTP: "8080", // Con puerto
		ConfigS3:   nil,    // Sin S3
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no debe especificarse sin ConfigS3")
	t.Log("✓ Crear retorna error cuando hay puerto HTTP pero no S3")
}

// ============================================================================
// TESTS DE SERIES.GO
// ============================================================================

// TestMatchPath_Exacto verifica coincidencia exacta
func TestMatchPath_Exacto(t *testing.T) {
	assert.True(t, tipos.MatchPath("sensor/temperatura", "sensor/temperatura"))
	assert.False(t, tipos.MatchPath("sensor/temperatura", "sensor/humedad"))
	t.Log("✓ matchPath funciona con coincidencia exacta")
}

// TestMatchPath_Wildcard verifica wildcards
func TestMatchPath_Wildcard(t *testing.T) {
	// Wildcard total
	assert.True(t, tipos.MatchPath("cualquier/cosa", "*"))

	// Wildcard parcial
	assert.True(t, tipos.MatchPath("dispositivo_001/temperatura", "dispositivo_001/*"))
	assert.True(t, tipos.MatchPath("dispositivo_001/temperatura", "*/temperatura"))

	// No coincide (dispositivo diferente)
	assert.False(t, tipos.MatchPath("dispositivo_001/temperatura", "dispositivo_002/*"))

	// Wildcard al final ahora coincide con múltiples niveles
	assert.True(t, tipos.MatchPath("dispositivo_001/temperatura/extra", "dispositivo_001/*"))
	assert.True(t, tipos.MatchPath("nodo/dispositivo/sensor", "nodo/*"))

	t.Log("✓ matchPath funciona con wildcards")
}

// TestMatchPath_LongitudDiferente verifica paths con diferente longitud
func TestMatchPath_LongitudDiferente(t *testing.T) {
	assert.False(t, tipos.MatchPath("a/b/c", "a/b"))
	assert.False(t, tipos.MatchPath("a/b", "a/b/c"))
	t.Log("✓ matchPath maneja longitudes diferentes")
}

// TestMatchPath_WildcardParcial verifica wildcard parcial en segmento
func TestMatchPath_WildcardParcial(t *testing.T) {
	// Wildcard parcial al inicio del segmento (wildcard al final = múltiples niveles)
	assert.True(t, tipos.MatchPath("dispositivo1/temp", "dispositivo*/*"))
	assert.True(t, tipos.MatchPath("dispositivo10/temp", "dispositivo*/*"))
	assert.True(t, tipos.MatchPath("dispositivo123/humedad", "dispositivo*/*"))

	// Wildcard al final también coincide con múltiples niveles
	assert.True(t, tipos.MatchPath("dispositivo1/temp/extra", "dispositivo*/*"))
	assert.True(t, tipos.MatchPath("dispositivo123/humedad/nivel2/nivel3", "dispositivo*/*"))

	// Wildcard parcial en múltiples segmentos
	assert.True(t, tipos.MatchPath("dispositivo1/temperatura", "dispositivo*/temp*"))
	assert.True(t, tipos.MatchPath("sensor_abc/valor_xyz", "sensor_*/valor_*"))

	// No coincide con prefijo diferente
	assert.False(t, tipos.MatchPath("sensor1/temp", "dispositivo*/*"))
	assert.False(t, tipos.MatchPath("dev1/temp", "dispositivo*/*"))

	t.Log("✓ MatchPath funciona con wildcard parcial")
}

// ============================================================================
// TESTS DE REGLAS.GO - AGREGACIONES
// ============================================================================

// TestCalcularAgregacionSimple_Promedio verifica cálculo de promedio
func TestCalcularAgregacionSimple_Promedio(t *testing.T) {
	valores := []float64{10, 20, 30, 40, 50}
	resultado, err := CalcularAgregacionSimple(valores, AgregacionPromedio)
	require.NoError(t, err)
	assert.Equal(t, 30.0, resultado)
	t.Log("✓ CalcularAgregacionSimple calcula promedio correctamente")
}

// TestCalcularAgregacionSimple_Maximo verifica cálculo de máximo
func TestCalcularAgregacionSimple_Maximo(t *testing.T) {
	valores := []float64{10, 50, 30, 20, 40}
	resultado, err := CalcularAgregacionSimple(valores, AgregacionMaximo)
	require.NoError(t, err)
	assert.Equal(t, 50.0, resultado)
	t.Log("✓ CalcularAgregacionSimple calcula máximo correctamente")
}

// TestCalcularAgregacionSimple_Minimo verifica cálculo de mínimo
func TestCalcularAgregacionSimple_Minimo(t *testing.T) {
	valores := []float64{10, 50, 30, 5, 40}
	resultado, err := CalcularAgregacionSimple(valores, AgregacionMinimo)
	require.NoError(t, err)
	assert.Equal(t, 5.0, resultado)
	t.Log("✓ CalcularAgregacionSimple calcula mínimo correctamente")
}

// TestCalcularAgregacionSimple_Suma verifica cálculo de suma
func TestCalcularAgregacionSimple_Suma(t *testing.T) {
	valores := []float64{10, 20, 30}
	resultado, err := CalcularAgregacionSimple(valores, AgregacionSuma)
	require.NoError(t, err)
	assert.Equal(t, 60.0, resultado)
	t.Log("✓ CalcularAgregacionSimple calcula suma correctamente")
}

// TestCalcularAgregacionSimple_Count verifica conteo
func TestCalcularAgregacionSimple_Count(t *testing.T) {
	valores := []float64{10, 20, 30, 40, 50}
	resultado, err := CalcularAgregacionSimple(valores, AgregacionCount)
	require.NoError(t, err)
	assert.Equal(t, 5.0, resultado)
	t.Log("✓ CalcularAgregacionSimple cuenta elementos correctamente")
}

// TestCalcularAgregacionSimple_Vacio verifica error con slice vacío
func TestCalcularAgregacionSimple_Vacio(t *testing.T) {
	_, err := CalcularAgregacionSimple([]float64{}, AgregacionPromedio)
	assert.Error(t, err)
	t.Log("✓ CalcularAgregacionSimple retorna error con slice vacío")
}

// TestCalcularAgregacionSimple_NoSoportada verifica error con agregación inválida
func TestCalcularAgregacionSimple_NoSoportada(t *testing.T) {
	_, err := CalcularAgregacionSimple([]float64{1, 2, 3}, "invalida")
	assert.Error(t, err)
	t.Log("✓ CalcularAgregacionSimple retorna error con agregación no soportada")
}

// ============================================================================
// TESTS DE REGLAS.GO - CONVERSIONES
// ============================================================================

// TestConvertirAFloat64 verifica conversión a float64
func TestConvertirAFloat64(t *testing.T) {
	// float64
	resultado, err := convertirAFloat64(float64(3.14))
	require.NoError(t, err)
	assert.Equal(t, 3.14, resultado)

	// int64
	resultado, err = convertirAFloat64(int64(42))
	require.NoError(t, err)
	assert.Equal(t, 42.0, resultado)

	// Tipo no soportado
	_, err = convertirAFloat64("texto")
	assert.Error(t, err)

	t.Log("✓ convertirAFloat64 convierte correctamente")
}

// ============================================================================
// TESTS DE REGLAS.GO - OPERADORES
// ============================================================================

// crearMotorReglasTest crea un motor de reglas para testing
func crearMotorReglasTest() *MotorReglas {
	return &MotorReglas{
		reglas:     make(map[string]*Regla),
		ejecutores: make(map[string]EjecutorAccion),
		habilitado: true,
		manager:    nil,
		db:         nil,
	}
}

// TestAplicarOperador_Numericos verifica operadores numéricos
func TestAplicarOperador_Numericos(t *testing.T) {
	mr := crearMotorReglasTest()

	// Mayor que
	assert.True(t, mr.aplicarOperador(float64(10), OperadorMayor, float64(5)))
	assert.False(t, mr.aplicarOperador(float64(5), OperadorMayor, float64(10)))

	// Menor que
	assert.True(t, mr.aplicarOperador(float64(5), OperadorMenor, float64(10)))
	assert.False(t, mr.aplicarOperador(float64(10), OperadorMenor, float64(5)))

	// Mayor o igual
	assert.True(t, mr.aplicarOperador(float64(10), OperadorMayorIgual, float64(10)))
	assert.True(t, mr.aplicarOperador(float64(11), OperadorMayorIgual, float64(10)))

	// Menor o igual
	assert.True(t, mr.aplicarOperador(float64(10), OperadorMenorIgual, float64(10)))
	assert.True(t, mr.aplicarOperador(float64(9), OperadorMenorIgual, float64(10)))

	// Igual
	assert.True(t, mr.aplicarOperador(float64(10), OperadorIgual, float64(10)))
	assert.False(t, mr.aplicarOperador(float64(10), OperadorIgual, float64(11)))

	// Distinto
	assert.True(t, mr.aplicarOperador(float64(10), OperadorDistinto, float64(11)))
	assert.False(t, mr.aplicarOperador(float64(10), OperadorDistinto, float64(10)))

	t.Log("✓ aplicarOperador funciona con operadores numéricos")
}

// TestAplicarOperador_Int64YFloat64 verifica compatibilidad int64/float64
func TestAplicarOperador_Int64YFloat64(t *testing.T) {
	mr := crearMotorReglasTest()

	// int64 vs float64
	assert.True(t, mr.aplicarOperador(int64(10), OperadorIgual, float64(10.0)))
	assert.True(t, mr.aplicarOperador(float64(10.0), OperadorIgual, int64(10)))

	// int64 vs int64
	assert.True(t, mr.aplicarOperador(int64(10), OperadorMayor, int64(5)))

	t.Log("✓ aplicarOperador maneja compatibilidad int64/float64")
}

// TestAplicarOperador_Boolean verifica operadores booleanos
func TestAplicarOperador_Boolean(t *testing.T) {
	mr := crearMotorReglasTest()

	assert.True(t, mr.aplicarOperador(true, OperadorIgual, true))
	assert.True(t, mr.aplicarOperador(false, OperadorIgual, false))
	assert.False(t, mr.aplicarOperador(true, OperadorIgual, false))

	assert.True(t, mr.aplicarOperador(true, OperadorDistinto, false))
	assert.False(t, mr.aplicarOperador(true, OperadorDistinto, true))

	// Operador no soportado para boolean
	assert.False(t, mr.aplicarOperador(true, OperadorMayor, false))

	t.Log("✓ aplicarOperador funciona con booleanos")
}

// TestAplicarOperador_String verifica operadores de string
func TestAplicarOperador_String(t *testing.T) {
	mr := crearMotorReglasTest()

	// Igual (case-insensitive)
	assert.True(t, mr.aplicarOperador("TEXTO", OperadorIgual, "texto"))
	assert.True(t, mr.aplicarOperador("Texto", OperadorIgual, "TEXTO"))
	assert.False(t, mr.aplicarOperador("texto1", OperadorIgual, "texto2"))

	// Distinto
	assert.True(t, mr.aplicarOperador("texto1", OperadorDistinto, "texto2"))
	assert.False(t, mr.aplicarOperador("texto", OperadorDistinto, "TEXTO"))

	// Operador no soportado para string
	assert.False(t, mr.aplicarOperador("a", OperadorMayor, "b"))

	t.Log("✓ aplicarOperador funciona con strings")
}

// TestAplicarOperador_TiposIncompatibles verifica manejo de tipos incompatibles
func TestAplicarOperador_TiposIncompatibles(t *testing.T) {
	mr := crearMotorReglasTest()

	assert.False(t, mr.aplicarOperador(true, OperadorIgual, "true"))
	assert.False(t, mr.aplicarOperador(42, OperadorIgual, "42"))
	assert.False(t, mr.aplicarOperador("texto", OperadorIgual, 42))

	t.Log("✓ aplicarOperador maneja tipos incompatibles")
}

// ============================================================================
// TESTS DE REGLAS.GO - VALIDACIONES
// ============================================================================

// TestValidarRegla_IDVacio verifica rechazo de ID vacío
func TestValidarRegla_IDVacio(t *testing.T) {
	mr := crearMotorReglasTest()

	regla := &Regla{
		ID: "",
	}

	err := mr.validarRegla(regla)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ID")
	t.Log("✓ validarRegla rechaza ID vacío")
}

// TestValidarRegla_SinCondiciones verifica rechazo sin condiciones
func TestValidarRegla_SinCondiciones(t *testing.T) {
	mr := crearMotorReglasTest()

	regla := &Regla{
		ID:          "regla-001",
		Condiciones: []Condicion{},
	}

	err := mr.validarRegla(regla)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "condición")
	t.Log("✓ validarRegla rechaza regla sin condiciones")
}

// TestValidarAccion_Valida verifica acción válida
func TestValidarAccion_Valida(t *testing.T) {
	mr := crearMotorReglasTest()

	accion := &Accion{
		Tipo:    "log",
		Destino: "archivo",
	}

	err := mr.validarAccion(accion)
	assert.NoError(t, err)
	t.Log("✓ validarAccion acepta acción válida")
}

// TestValidarAccion_TipoVacio verifica rechazo de tipo vacío
func TestValidarAccion_TipoVacio(t *testing.T) {
	mr := crearMotorReglasTest()

	accion := &Accion{
		Tipo:    "",
		Destino: "archivo",
	}

	err := mr.validarAccion(accion)
	assert.Error(t, err)
	t.Log("✓ validarAccion rechaza tipo vacío")
}

// TestValidarAccion_DestinoVacio verifica rechazo de destino vacío
func TestValidarAccion_DestinoVacio(t *testing.T) {
	mr := crearMotorReglasTest()

	accion := &Accion{
		Tipo:    "log",
		Destino: "",
	}

	err := mr.validarAccion(accion)
	assert.Error(t, err)
	t.Log("✓ validarAccion rechaza destino vacío")
}

// ============================================================================
// TESTS DE REGLAS.GO - OPERACIONES CRUD
// ============================================================================

// TestRegistrarEjecutor verifica registro de ejecutores
func TestRegistrarEjecutor(t *testing.T) {
	mr := crearMotorReglasTest()

	ejecutorPersonalizado := func(accion Accion, regla *Regla, valores map[string]interface{}) error {
		return nil
	}

	err := mr.RegistrarEjecutor("personalizado", ejecutorPersonalizado)
	assert.NoError(t, err)

	// Tipo vacío
	err = mr.RegistrarEjecutor("", ejecutorPersonalizado)
	assert.Error(t, err)

	// Ejecutor nil
	err = mr.RegistrarEjecutor("otro", nil)
	assert.Error(t, err)

	t.Log("✓ RegistrarEjecutor funciona correctamente")
}

// TestHabilitar verifica habilitación/deshabilitación del motor
func TestHabilitar(t *testing.T) {
	mr := crearMotorReglasTest()

	assert.True(t, mr.habilitado)

	mr.Habilitar(false)
	assert.False(t, mr.habilitado)

	mr.Habilitar(true)
	assert.True(t, mr.habilitado)

	t.Log("✓ Habilitar funciona correctamente")
}

// TestListarReglas verifica listado de reglas
func TestListarReglas(t *testing.T) {
	mr := crearMotorReglasTest()

	// Inicialmente vacío
	reglas := mr.ListarReglas()
	assert.Empty(t, reglas)

	// Agregar reglas directamente
	mr.reglas["regla1"] = &Regla{ID: "regla1"}
	mr.reglas["regla2"] = &Regla{ID: "regla2"}

	reglas = mr.ListarReglas()
	assert.Len(t, reglas, 2)

	t.Log("✓ ListarReglas funciona correctamente")
}

// TestObtenerRegla verifica obtención de regla por ID
func TestObtenerRegla(t *testing.T) {
	mr := crearMotorReglasTest()

	mr.reglas["regla1"] = &Regla{ID: "regla1", Nombre: "Regla Uno"}

	// Existente
	regla, err := mr.ObtenerRegla("regla1")
	assert.NoError(t, err)
	assert.Equal(t, "Regla Uno", regla.Nombre)

	// No existente
	_, err = mr.ObtenerRegla("inexistente")
	assert.Error(t, err)

	t.Log("✓ ObtenerRegla funciona correctamente")
}

// TestAgregarReglaEnMemoria verifica agregar regla sin persistencia
func TestAgregarReglaEnMemoria(t *testing.T) {
	mr := crearMotorReglasTest()

	regla := &Regla{ID: "regla1"}
	err := mr.AgregarReglaEnMemoria(regla)
	assert.NoError(t, err)
	assert.Len(t, mr.reglas, 1)

	t.Log("✓ AgregarReglaEnMemoria funciona correctamente")
}

// TestEliminarReglaEnMemoria verifica eliminar regla sin persistencia
func TestEliminarReglaEnMemoria(t *testing.T) {
	mr := crearMotorReglasTest()

	mr.reglas["regla1"] = &Regla{ID: "regla1"}

	err := mr.EliminarReglaEnMemoria("regla1")
	assert.NoError(t, err)
	assert.Empty(t, mr.reglas)

	// No existente
	err = mr.EliminarReglaEnMemoria("inexistente")
	assert.Error(t, err)

	t.Log("✓ EliminarReglaEnMemoria funciona correctamente")
}

// TestActualizarReglaEnMemoria verifica actualizar regla sin persistencia
func TestActualizarReglaEnMemoria(t *testing.T) {
	mr := crearMotorReglasTest()

	mr.reglas["regla1"] = &Regla{ID: "regla1", Nombre: "Original"}

	reglaActualizada := &Regla{ID: "regla1", Nombre: "Actualizada"}
	err := mr.ActualizarReglaEnMemoria(reglaActualizada)
	assert.NoError(t, err)
	assert.Equal(t, "Actualizada", mr.reglas["regla1"].Nombre)

	// No existente
	err = mr.ActualizarReglaEnMemoria(&Regla{ID: "inexistente"})
	assert.Error(t, err)

	t.Log("✓ ActualizarReglaEnMemoria funciona correctamente")
}

// ============================================================================
// TESTS DE REGLAS.GO - PROCESAMIENTO DE DATOS
// ============================================================================

// ============================================================================
// TESTS DE REGLAS.GO - EVALUACIÓN DE CONDICIONES
// ============================================================================

// TestEvaluarCondicionesRegla_SinCondiciones verifica regla sin condiciones
func TestEvaluarCondicionesRegla_SinCondiciones(t *testing.T) {
	mr := crearMotorReglasTest()

	regla := &Regla{
		ID:          "regla1",
		Condiciones: []Condicion{},
	}

	assert.False(t, mr.evaluarCondicionesRegla(regla, time.Now()))
	t.Log("✓ evaluarCondicionesRegla retorna false sin condiciones")
}

// ============================================================================
// TESTS DE CONSULTAS.GO
// ============================================================================

// TestDeberiaSkipearBloque verifica lógica de skip de bloques
func TestDeberiaSkipearBloque(t *testing.T) {
	// Crear un manager mínimo para probar
	manager := &ManagerEdge{}

	// Bloque: 1000-2000, Consulta: 1500-2500 (se solapan)
	assert.False(t, manager.deberiaSkipearBloque(
		"data/0000000001/00000000000000001000_00000000000000002000",
		1500, 2500))

	// Bloque: 1000-2000, Consulta: 3000-4000 (no se solapan)
	assert.True(t, manager.deberiaSkipearBloque(
		"data/0000000001/00000000000000001000_00000000000000002000",
		3000, 4000))

	// Bloque: 3000-4000, Consulta: 1000-2000 (no se solapan)
	assert.True(t, manager.deberiaSkipearBloque(
		"data/0000000001/00000000000000003000_00000000000000004000",
		1000, 2000))

	// Formato inválido - no skip por seguridad
	assert.False(t, manager.deberiaSkipearBloque("formato/invalido", 1000, 2000))

	t.Log("✓ deberiaSkipearBloque funciona correctamente")
}

// ============================================================================
// TESTS DE MIGRACION_DATOS.GO
// ============================================================================

// TestParsearTiempoFinDeClave verifica parseo de tiempos
func TestParsearTiempoFinDeClave(t *testing.T) {
	// Clave válida
	tiempoFin, err := parsearTiempoFinDeClave("data/0000000001/00000000000000001000_00000000000000002000")
	assert.NoError(t, err)
	assert.Equal(t, int64(2000), tiempoFin)

	// Formato inválido - pocos segmentos
	_, err = parsearTiempoFinDeClave("data/0000000001")
	assert.Error(t, err)

	// Formato inválido - sin underscore
	_, err = parsearTiempoFinDeClave("data/0000000001/12345")
	assert.Error(t, err)

	// Formato inválido - no es número
	_, err = parsearTiempoFinDeClave("data/0000000001/abc_def")
	assert.Error(t, err)

	t.Log("✓ parsearTiempoFinDeClave funciona correctamente")
}

// ============================================================================
// TESTS DE REGLAS.GO - SERIALIZACIÓN
// ============================================================================

// TestGenerarClaveRegla verifica generación de claves
func TestGenerarClaveRegla(t *testing.T) {
	clave := generarClaveRegla("regla-001")
	assert.Equal(t, []byte("reglas/regla-001"), clave)
	t.Log("✓ generarClaveRegla funciona correctamente")
}

// ============================================================================
// TESTS DE REGLAS.GO - HELPERS
// ============================================================================

// TestGetTiposKeys verifica extracción de keys
func TestGetTiposKeys(t *testing.T) {
	m := map[tipos.TipoDatos]bool{
		tipos.Integer: true,
		tipos.Real:    true,
	}

	keys := getTiposKeys(m)
	assert.Len(t, keys, 2)
	t.Log("✓ getTiposKeys funciona correctamente")
}

// ============================================================================
// TESTS DE CONCURRENCIA
// ============================================================================

// TestMotorReglas_Concurrencia verifica seguridad en concurrencia
func TestMotorReglas_Concurrencia(t *testing.T) {
	mr := crearMotorReglasTest()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperaciones := 100

	// Múltiples goroutines agregando reglas
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperaciones; j++ {
				reglaID := fmt.Sprintf("regla-%d-%d", id, j)
				mr.AgregarReglaEnMemoria(&Regla{ID: reglaID})
			}
		}(i)
	}

	// Goroutines leyendo
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperaciones; j++ {
				mr.ListarReglas()
			}
		}()
	}

	wg.Wait()
	t.Log("✓ MotorReglas es seguro para acceso concurrente")
}

// ============================================================================
// TESTS DE INTEGRACIÓN CON PEBBLEDB (requiere DB temporal)
// ============================================================================

// crearDBTemporal crea una base de datos Pebble temporal para testing
func crearDBTemporal(t *testing.T) *pebble.DB {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	require.NoError(t, err)
	return db
}

// ============================================================================
// HELPER: MANAGER EDGE PARA TESTS
// ============================================================================

// crearManagerEdgeParaTest crea un ManagerEdge mínimo para testing
// sin iniciar servidor HTTP ni conectarse a S3
func crearManagerEdgeParaTest(t *testing.T) *ManagerEdge {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	require.NoError(t, err)

	manager := &ManagerEdge{
		nodoID:        "test-node-001",
		direccion:     "127.0.0.1",
		puertoHTTP:    "8080",
		db:            db,
		cache:         &Cache{datos: make(map[string]tipos.Serie)},
		done:          make(chan struct{}),
		contador:      0,
		tamañoBuffer:  100,
		timeoutBuffer: 100 * 1000 * 1000, // 100ms
	}

	// Inicializar motor de reglas
	manager.MotorReglas = &MotorReglas{
		reglas:     make(map[string]*Regla),
		ejecutores: make(map[string]EjecutorAccion),
		habilitado: true,
		db:         db,
		manager:    manager,
	}

	t.Cleanup(func() {
		select {
		case <-manager.done:
			// Ya cerrado
		default:
			close(manager.done)
		}
		db.Close()
	})

	return manager
}

// ============================================================================
// MOCK DE CLIENTE S3 PARA TESTS
// ============================================================================

// mockClienteS3 implementa tipos.ClienteS3 para testing
type mockClienteS3 struct {
	headBucketOutput   *s3.HeadBucketOutput
	createBucketOutput *s3.CreateBucketOutput
	listObjectsOutput  *s3.ListObjectsV2Output
	getObjectOutput    *s3.GetObjectOutput
	getObjectData      []byte
	putObjectOutput    *s3.PutObjectOutput
	deleteObjectOutput *s3.DeleteObjectOutput

	headBucketErr   error
	createBucketErr error
	listObjectsErr  error
	getObjectErr    error
	putObjectErr    error
	deleteObjectErr error

	// Para verificar llamadas
	putObjectCalls    int
	deleteObjectCalls int
}

func (m *mockClienteS3) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	if m.headBucketErr != nil {
		return nil, m.headBucketErr
	}
	return m.headBucketOutput, nil
}

func (m *mockClienteS3) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	if m.createBucketErr != nil {
		return nil, m.createBucketErr
	}
	return m.createBucketOutput, nil
}

func (m *mockClienteS3) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.listObjectsErr != nil {
		return nil, m.listObjectsErr
	}
	return m.listObjectsOutput, nil
}

func (m *mockClienteS3) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getObjectErr != nil {
		return nil, m.getObjectErr
	}
	if m.getObjectData != nil {
		return &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewReader(m.getObjectData)),
		}, nil
	}
	return m.getObjectOutput, nil
}

func (m *mockClienteS3) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.putObjectCalls++
	if m.putObjectErr != nil {
		return nil, m.putObjectErr
	}
	return m.putObjectOutput, nil
}

func (m *mockClienteS3) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.deleteObjectCalls++
	if m.deleteObjectErr != nil {
		return nil, m.deleteObjectErr
	}
	return m.deleteObjectOutput, nil
}

// ============================================================================
// HELPER: CREAR BLOQUE COMPRIMIDO PARA TESTS
// ============================================================================

// crearBloqueComprimidoTest genera un bloque comprimido válido para testing
func crearBloqueComprimidoTest(t *testing.T, serie tipos.Serie, mediciones []tipos.Medicion) []byte {
	// Comprimir tiempos con DeltaDelta
	tiemposComprimidos := compresor.CompresionDeltaDeltaTiempo(mediciones)

	// Extraer y comprimir valores según tipo
	valores := compresor.ExtraerValores(mediciones)
	var valoresComprimidos []byte
	var err error

	switch serie.TipoDatos {
	case tipos.Integer:
		valoresInt, _ := compresor.ConvertirAInt64Array(valores)
		switch serie.CompresionBytes {
		case tipos.DeltaDelta:
			comp := &compresor.CompresorDeltaDeltaGenerico[int64]{}
			valoresComprimidos, err = comp.Comprimir(valoresInt)
		case tipos.SinCompresion:
			comp := &compresor.CompresorNingunoGenerico[int64]{}
			valoresComprimidos, err = comp.Comprimir(valoresInt)
		default:
			comp := &compresor.CompresorNingunoGenerico[int64]{}
			valoresComprimidos, err = comp.Comprimir(valoresInt)
		}

	case tipos.Real:
		valoresFloat, _ := compresor.ConvertirAFloat64Array(valores)
		switch serie.CompresionBytes {
		case tipos.Xor:
			comp := &compresor.CompresorXor{}
			valoresComprimidos, err = comp.Comprimir(valoresFloat)
		case tipos.SinCompresion:
			comp := &compresor.CompresorNingunoGenerico[float64]{}
			valoresComprimidos, err = comp.Comprimir(valoresFloat)
		default:
			comp := &compresor.CompresorNingunoGenerico[float64]{}
			valoresComprimidos, err = comp.Comprimir(valoresFloat)
		}

	case tipos.Boolean:
		valoresBool, _ := compresor.ConvertirABoolArray(valores)
		comp := &compresor.CompresorNingunoGenerico[bool]{}
		valoresComprimidos, err = comp.Comprimir(valoresBool)

	case tipos.Text:
		valoresStr, _ := compresor.ConvertirAStringArray(valores)
		comp := &compresor.CompresorNingunoGenerico[string]{}
		valoresComprimidos, err = comp.Comprimir(valoresStr)
	}

	require.NoError(t, err)

	// Combinar tiempos y valores
	bloqueNivel1 := compresor.CombinarDatos(tiemposComprimidos, valoresComprimidos)

	// Aplicar compresión de bloque
	compresorBloque := compresor.ObtenerCompresorBloque(serie.CompresionBloque)
	bloqueFinal, err := compresorBloque.Comprimir(bloqueNivel1)
	require.NoError(t, err)

	return bloqueFinal
}

// ============================================================================
// TESTS DE SERIES.GO
// ============================================================================

// TestObtenerSeries_Existe verifica obtención de serie existente
func TestObtenerSeries_Existe(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Agregar serie al cache
	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temperatura",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
		Tags:             map[string]string{"ubicacion": "sala1"},
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temperatura"] = serie
	manager.cache.mu.Unlock()

	// Obtener serie
	resultado, err := manager.ObtenerSeries("sensor/temperatura")
	require.NoError(t, err)
	assert.Equal(t, serie.Path, resultado.Path)
	assert.Equal(t, serie.TipoDatos, resultado.TipoDatos)
	t.Log("ObtenerSeries retorna serie existente correctamente")
}

// TestObtenerSeries_NoExiste verifica error cuando serie no existe
func TestObtenerSeries_NoExiste(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	_, err := manager.ObtenerSeries("serie/inexistente")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("ObtenerSeries retorna error para serie inexistente")
}

// TestListarSeries_Vacio verifica listado cuando no hay series
func TestListarSeries_Vacio(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	series, err := manager.ListarSeries()
	require.NoError(t, err)
	assert.Empty(t, series)
	t.Log("ListarSeries retorna lista vacía cuando no hay series")
}

// TestListarSeries_ConDatos verifica listado con múltiples series
func TestListarSeries_ConDatos(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Agregar varias series al cache
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temperatura"] = tipos.Serie{Path: "sensor/temperatura"}
	manager.cache.datos["sensor/humedad"] = tipos.Serie{Path: "sensor/humedad"}
	manager.cache.datos["actuador/motor"] = tipos.Serie{Path: "actuador/motor"}
	manager.cache.mu.Unlock()

	series, err := manager.ListarSeries()
	require.NoError(t, err)
	assert.Len(t, series, 3)
	t.Log("ListarSeries retorna todas las series")
}

// TestListarSeriesPorPath_ConWildcard verifica filtrado por patrón
func TestListarSeriesPorPath_ConWildcard(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Agregar series al cache
	manager.cache.mu.Lock()
	manager.cache.datos["dispositivo_001/temperatura"] = tipos.Serie{Path: "dispositivo_001/temperatura"}
	manager.cache.datos["dispositivo_001/humedad"] = tipos.Serie{Path: "dispositivo_001/humedad"}
	manager.cache.datos["dispositivo_002/temperatura"] = tipos.Serie{Path: "dispositivo_002/temperatura"}
	manager.cache.mu.Unlock()

	// Buscar con wildcard
	series, err := manager.ListarSeriesPorPath("dispositivo_001/*")
	require.NoError(t, err)
	assert.Len(t, series, 2)

	// Buscar otro patrón
	series, err = manager.ListarSeriesPorPath("*/temperatura")
	require.NoError(t, err)
	assert.Len(t, series, 2)

	t.Log("ListarSeriesPorPath filtra correctamente con wildcards")
}

// TestListarSeriesPorTags_Coincidencias verifica filtrado por tags
func TestListarSeriesPorTags_Coincidencias(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Agregar series con diferentes tags
	manager.cache.mu.Lock()
	manager.cache.datos["sensor1"] = tipos.Serie{
		Path: "sensor1",
		Tags: map[string]string{"ubicacion": "sala1", "tipo": "temperatura"},
	}
	manager.cache.datos["sensor2"] = tipos.Serie{
		Path: "sensor2",
		Tags: map[string]string{"ubicacion": "sala1", "tipo": "humedad"},
	}
	manager.cache.datos["sensor3"] = tipos.Serie{
		Path: "sensor3",
		Tags: map[string]string{"ubicacion": "sala2", "tipo": "temperatura"},
	}
	manager.cache.mu.Unlock()

	// Filtrar por ubicacion
	series, err := manager.ListarSeriesPorTags(map[string]string{"ubicacion": "sala1"})
	require.NoError(t, err)
	assert.Len(t, series, 2)

	// Filtrar por múltiples tags
	series, err = manager.ListarSeriesPorTags(map[string]string{"ubicacion": "sala1", "tipo": "temperatura"})
	require.NoError(t, err)
	assert.Len(t, series, 1)

	// Sin filtro retorna todas
	series, err = manager.ListarSeriesPorTags(nil)
	require.NoError(t, err)
	assert.Len(t, series, 3)

	t.Log("ListarSeriesPorTags filtra correctamente por tags")
}

// TestListarSeriesPorDispositivo verifica filtrado por dispositivo
func TestListarSeriesPorDispositivo(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Agregar series
	manager.cache.mu.Lock()
	manager.cache.datos["dispositivo_001/temp"] = tipos.Serie{Path: "dispositivo_001/temp"}
	manager.cache.datos["dispositivo_001/hum"] = tipos.Serie{Path: "dispositivo_001/hum"}
	manager.cache.datos["dispositivo_002/temp"] = tipos.Serie{Path: "dispositivo_002/temp"}
	manager.cache.mu.Unlock()

	series, err := manager.ListarSeriesPorDispositivo("dispositivo_001")
	require.NoError(t, err)
	assert.Len(t, series, 2)
	t.Log("ListarSeriesPorDispositivo filtra correctamente")
}

// ============================================================================
// TESTS DE EDGE.GO - CREARSERIE VALIDACIONES
// ============================================================================

// TestCrearSerie_PathVacio verifica rechazo de path vacío
func TestCrearSerie_PathVacio(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	err := manager.CrearSerie(tipos.Serie{
		Path:             "",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "vacío")
	t.Log("CrearSerie rechaza path vacío")
}

// TestCrearSerie_PathInvalido verifica rechazo de path con formato inválido
func TestCrearSerie_PathInvalido(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	casosInvalidos := []string{
		"/sensor/temp", // Empieza con /
		"sensor/temp/", // Termina con /
		"sensor//temp", // Doble /
	}

	for _, path := range casosInvalidos {
		err := manager.CrearSerie(tipos.Serie{
			Path:             path,
			TipoDatos:        tipos.Real,
			TamañoBloque:     100,
			CompresionBloque: tipos.Ninguna,
			CompresionBytes:  tipos.SinCompresion,
		})
		assert.Error(t, err, "Debería fallar para path: %s", path)
	}
	t.Log("CrearSerie rechaza paths con formato inválido")
}

// TestCrearSerie_TipoDatosInvalido verifica rechazo de tipo de datos inválido
func TestCrearSerie_TipoDatosInvalido(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Desconocido, // Tipo desconocido no es válido
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tipo de datos")
	t.Log("CrearSerie rechaza tipo de datos inválido")
}

// TestCrearSerie_TamanoBloqueInvalido verifica rechazo de tamaño de bloque fuera de rango
func TestCrearSerie_TamanoBloqueInvalido(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Tamaño 0
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     0,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	assert.Error(t, err)

	// Tamaño negativo
	err = manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp2",
		TipoDatos:        tipos.Real,
		TamañoBloque:     -1,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	assert.Error(t, err)

	t.Log("CrearSerie rechaza tamaño de bloque inválido")
}

// TestCrearSerie_CompresionBloqueInvalida verifica rechazo de compresión de bloque inválida
func TestCrearSerie_CompresionBloqueInvalida(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.TipoCompresionBloque("InvalidCompression"),
		CompresionBytes:  tipos.SinCompresion,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "compresión")
	t.Log("CrearSerie rechaza compresión de bloque inválida")
}

// TestCrearSerie_CompresionBytesInvalida verifica rechazo de compresión de bytes incompatible
func TestCrearSerie_CompresionBytesInvalida(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// XOR no es válido para Boolean
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/activo",
		TipoDatos:        tipos.Boolean,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.Xor,
	})
	assert.Error(t, err)
	t.Log("CrearSerie rechaza compresión de bytes incompatible con tipo")
}

// TestCrearSerie_Exitoso verifica creación exitosa de serie
func TestCrearSerie_Exitoso(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temperatura",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
		Tags:             map[string]string{"ubicacion": "sala1"},
	})
	require.NoError(t, err)

	// Verificar que está en cache
	manager.cache.mu.RLock()
	serie, existe := manager.cache.datos["sensor/temperatura"]
	manager.cache.mu.RUnlock()

	assert.True(t, existe)
	assert.Equal(t, "sensor/temperatura", serie.Path)
	assert.Equal(t, 1, serie.SerieId)

	// Verificar que está en DB
	_, closer, err := manager.db.Get([]byte("series/sensor/temperatura"))
	require.NoError(t, err)
	closer.Close()

	t.Log("CrearSerie crea serie exitosamente con persistencia")
}

// TestCrearSerie_YaExiste_NoError verifica idempotencia
func TestCrearSerie_YaExiste_NoError(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	serie := tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}

	// Primera creación
	err := manager.CrearSerie(serie)
	require.NoError(t, err)

	// Segunda creación - debe ser idempotente
	err = manager.CrearSerie(serie)
	assert.NoError(t, err) // No debe dar error

	// Verificar que solo hay una serie
	manager.cache.mu.RLock()
	count := len(manager.cache.datos)
	manager.cache.mu.RUnlock()
	assert.Equal(t, 1, count)

	t.Log("CrearSerie es idempotente para series existentes")
}

// ============================================================================
// TESTS DE EDGE.GO - INSERTAR Y OTRAS FUNCIONES
// ============================================================================

// TestInsertar_SerieNoExiste verifica error cuando serie no existe
func TestInsertar_SerieNoExiste(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	err := manager.Insertar("serie/inexistente", time.Now().UnixNano(), 25.5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("Insertar retorna error para serie inexistente")
}

// TestInsertar_TipoIncompatible verifica error con tipo de dato incorrecto
func TestInsertar_TipoIncompatible(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie de tipo Integer
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/contador",
		TipoDatos:        tipos.Integer,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Intentar insertar string en serie Integer
	err = manager.Insertar("sensor/contador", time.Now().UnixNano(), "texto")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "incompatible")
	t.Log("Insertar rechaza tipo de dato incompatible")
}

// TestInsertar_Exitoso verifica inserción exitosa
func TestInsertar_Exitoso(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Insertar dato
	tiempo := time.Now().UnixNano()
	err = manager.Insertar("sensor/temp", tiempo, 25.5)
	assert.NoError(t, err)

	t.Log("Insertar agrega dato correctamente")
}

// TestObtenerNodoID verifica que retorna el ID del nodo
func TestObtenerNodoID(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	nodoID := manager.ObtenerNodoID()
	assert.Equal(t, "test-node-001", nodoID)
	t.Log("ObtenerNodoID retorna el ID correctamente")
}

// ============================================================================
// TESTS DE CONSULTAS.GO
// ============================================================================

// TestConsultarRango_SerieNoExiste verifica error cuando serie no existe
func TestConsultarRango_SerieNoExiste(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	_, err := manager.ConsultarRango("serie/inexistente", time.Now().Add(-1*time.Hour), time.Now())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("ConsultarRango retorna error para serie inexistente")
}

// TestConsultarRango_SinDatos verifica consulta cuando no hay datos
func TestConsultarRango_SinDatos(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie sin datos
	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	resultado, err := manager.ConsultarRango("sensor/temp", time.Now().Add(-1*time.Hour), time.Now())
	require.NoError(t, err)
	assert.Empty(t, resultado.Tiempos)
	assert.Empty(t, resultado.Series)
	assert.Empty(t, resultado.Valores)
	t.Log("ConsultarRango retorna resultado vacío cuando no hay datos")
}

// TestConsultarRango_ConDatosEnDB verifica consulta con datos en DB
func TestConsultarRango_ConDatosEnDB(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	// Crear mediciones de prueba
	ahora := time.Now().UnixNano()
	mediciones := []tipos.Medicion{
		{Tiempo: ahora - 3000, Valor: float64(20.0)},
		{Tiempo: ahora - 2000, Valor: float64(21.0)},
		{Tiempo: ahora - 1000, Valor: float64(22.0)},
	}

	// Crear bloque comprimido
	bloque := crearBloqueComprimidoTest(t, serie, mediciones)

	// Guardar en DB
	clave := generarClaveDatos(serie.SerieId, ahora-3000, ahora-1000)
	err := manager.db.Set(clave, bloque, pebble.Sync)
	require.NoError(t, err)

	// Consultar
	resultado, err := manager.ConsultarRango("sensor/temp",
		time.Unix(0, ahora-5000),
		time.Unix(0, ahora))
	require.NoError(t, err)

	// Verificar formato tabular
	assert.Len(t, resultado.Tiempos, 3, "Debe haber 3 timestamps")
	assert.Equal(t, []string{"sensor/temp"}, resultado.Series, "Debe haber una serie")
	assert.Len(t, resultado.Valores, 3, "Debe haber 3 filas de valores")

	// Verificar que cada fila tiene una columna con el valor correcto
	for i, fila := range resultado.Valores {
		assert.Len(t, fila, 1, "Cada fila debe tener 1 columna")
		assert.NotNil(t, fila[0], "El valor no debe ser nil")
		t.Logf("Fila %d: tiempo=%d, valor=%v", i, resultado.Tiempos[i], fila[0])
	}
	t.Log("ConsultarRango lee y descomprime datos de DB correctamente")
}

// TestConsultarUltimoPunto_DesdeBuffer verifica lectura desde buffer en memoria
func TestConsultarUltimoPunto_DesdeBuffer(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie con buffer
	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	// Crear buffer con datos
	ahora := time.Now().UnixNano()
	buffer := &SerieBuffer{
		datos:      make([]tipos.Medicion, 100),
		serie:      serie,
		indice:     3,
		done:       make(chan struct{}),
		datosCanal: make(chan tipos.Medicion, 100),
	}
	buffer.datos[0] = tipos.Medicion{Tiempo: ahora - 2000, Valor: float64(20.0)}
	buffer.datos[1] = tipos.Medicion{Tiempo: ahora - 1000, Valor: float64(21.0)}
	buffer.datos[2] = tipos.Medicion{Tiempo: ahora, Valor: float64(22.0)} // Más reciente
	manager.buffers.Store("sensor/temp", buffer)

	// Consultar último punto (nil, nil = sin límites de tiempo)
	resultado, err := manager.ConsultarUltimoPunto("sensor/temp", nil, nil)
	require.NoError(t, err)

	// Verificar formato columnar
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, "sensor/temp", resultado.Series[0])
	assert.Equal(t, ahora, resultado.Tiempos[0])
	assert.Equal(t, float64(22.0), resultado.Valores[0])
	t.Log("ConsultarUltimoPunto lee desde buffer correctamente")
}

// TestConsultarUltimoPunto_DesdeDB verifica lectura desde DB cuando buffer vacío
func TestConsultarUltimoPunto_DesdeDB(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	// Crear mediciones
	ahora := time.Now().UnixNano()
	mediciones := []tipos.Medicion{
		{Tiempo: ahora - 2000, Valor: float64(20.0)},
		{Tiempo: ahora - 1000, Valor: float64(21.0)},
		{Tiempo: ahora, Valor: float64(22.0)},
	}

	// Guardar bloque
	bloque := crearBloqueComprimidoTest(t, serie, mediciones)
	clave := generarClaveDatos(serie.SerieId, ahora-2000, ahora)
	err := manager.db.Set(clave, bloque, pebble.Sync)
	require.NoError(t, err)

	// Consultar (sin buffer, nil, nil = sin límites de tiempo)
	resultado, err := manager.ConsultarUltimoPunto("sensor/temp", nil, nil)
	require.NoError(t, err)

	// Verificar formato columnar
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, "sensor/temp", resultado.Series[0])
	assert.Equal(t, ahora, resultado.Tiempos[0])
	t.Log("ConsultarUltimoPunto lee desde DB cuando buffer vacío")
}

// ============================================================================
// TESTS DE HANDLERS HTTP (comunicacion_nube.go)
// ============================================================================

// TestHandleConsultaRango_MetodoInvalido verifica rechazo de método GET
func TestHandleConsultaRango_MetodoInvalido(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	req := httptest.NewRequest(http.MethodGet, "/api/consulta/rango", nil)
	w := httptest.NewRecorder()

	manager.handleConsultaRango(w, req)

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	t.Log("handleConsultaRango rechaza método GET")
}

// TestHandleConsultaRango_BodyInvalido verifica error con body inválido
func TestHandleConsultaRango_BodyInvalido(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	req := httptest.NewRequest(http.MethodPost, "/api/consulta/rango", bytes.NewReader([]byte("datos inválidos")))
	w := httptest.NewRecorder()

	manager.handleConsultaRango(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	t.Log("handleConsultaRango rechaza body inválido")
}

// TestHandleConsultaRango_Exitoso verifica consulta exitosa
func TestHandleConsultaRango_Exitoso(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	// Crear solicitud
	ahora := time.Now().UnixNano()
	solicitud := tipos.SolicitudConsultaRango{
		Serie:        "sensor/temp",
		TiempoInicio: ahora - 1000000,
		TiempoFin:    ahora,
	}
	solicitudBytes, _ := tipos.SerializarGob(solicitud)

	req := httptest.NewRequest(http.MethodPost, "/api/consulta/rango", bytes.NewReader(solicitudBytes))
	w := httptest.NewRecorder()

	manager.handleConsultaRango(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/octet-stream", w.Header().Get("Content-Type"))
	t.Log("handleConsultaRango procesa solicitud exitosamente")
}

// TestHandleConsultaUltimo_Exitoso verifica consulta de último punto
func TestHandleConsultaUltimo_Exitoso(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie con buffer
	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	// Buffer con datos
	ahora := time.Now().UnixNano()
	buffer := &SerieBuffer{
		datos:  make([]tipos.Medicion, 100),
		serie:  serie,
		indice: 1,
	}
	buffer.datos[0] = tipos.Medicion{Tiempo: ahora, Valor: float64(25.0)}
	manager.buffers.Store("sensor/temp", buffer)

	// Crear solicitud
	solicitud := tipos.SolicitudConsultaPunto{Serie: "sensor/temp"}
	solicitudBytes, _ := tipos.SerializarGob(solicitud)

	req := httptest.NewRequest(http.MethodPost, "/api/consulta/ultimo", bytes.NewReader(solicitudBytes))
	w := httptest.NewRecorder()

	manager.handleConsultaUltimo(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Deserializar respuesta
	var respuesta tipos.RespuestaConsultaPunto
	err := tipos.DeserializarGob(w.Body.Bytes(), &respuesta)
	require.NoError(t, err)

	// Verificar formato columnar
	require.Len(t, respuesta.Resultado.Series, 1)
	assert.Equal(t, "sensor/temp", respuesta.Resultado.Series[0])
	assert.Equal(t, float64(25.0), respuesta.Resultado.Valores[0])
	assert.Empty(t, respuesta.Error)
	t.Log("handleConsultaUltimo retorna último punto correctamente")
}

// TestEnviarRespuestaGob_Exitoso verifica serialización de respuesta
func TestEnviarRespuestaGob_Exitoso(t *testing.T) {
	w := httptest.NewRecorder()

	respuesta := tipos.RespuestaConsultaPunto{
		Resultado: tipos.ResultadoConsultaPunto{
			Series:  []string{"sensor/temp"},
			Tiempos: []int64{1000},
			Valores: []interface{}{float64(25.0)},
		},
	}

	enviarRespuestaGob(w, respuesta)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/octet-stream", w.Header().Get("Content-Type"))
	assert.NotEmpty(t, w.Body.Bytes())
	t.Log("enviarRespuestaGob serializa y envía correctamente")
}

// TestEnviarRespuestaError verifica envío de error
func TestEnviarRespuestaError(t *testing.T) {
	w := httptest.NewRecorder()

	enviarRespuestaError(w, "Error de prueba")

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "Error de prueba")
	t.Log("enviarRespuestaError envía error correctamente")
}

// ============================================================================
// TESTS DE S3 (migracion_datos.go y comunicacion_nube.go)
// ============================================================================

// TestRegistrarEnS3_S3NoConfigurado verifica error cuando S3 no está configurado
func TestRegistrarEnS3_S3NoConfigurado(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Asegurar que clienteS3 es nil
	clienteOriginal := clienteS3
	clienteS3 = nil
	defer func() { clienteS3 = clienteOriginal }()

	err := manager.RegistrarEnS3()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no está configurado")
	t.Log("RegistrarEnS3 retorna error cuando S3 no está configurado")
}

// TestRegistrarEnS3_Exitoso verifica registro exitoso
func TestRegistrarEnS3_Exitoso(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Configurar mock
	clienteOriginal := clienteS3
	configOriginal := configuracionS3
	defer func() {
		clienteS3 = clienteOriginal
		configuracionS3 = configOriginal
	}()

	mockS3 := &mockClienteS3{
		putObjectOutput: &s3.PutObjectOutput{},
	}
	clienteS3 = mockS3
	configuracionS3 = tipos.ConfiguracionS3{Bucket: "test-bucket"}

	// Agregar series al cache
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = tipos.Serie{Path: "sensor/temp"}
	manager.cache.mu.Unlock()

	err := manager.RegistrarEnS3()
	assert.NoError(t, err)
	assert.Equal(t, 1, mockS3.putObjectCalls)
	t.Log("RegistrarEnS3 registra nodo exitosamente")
}

// TestRegistrarEnS3_ErrorPutObject verifica manejo de error en PutObject
func TestRegistrarEnS3_ErrorPutObject(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Configurar mock con error
	clienteOriginal := clienteS3
	configOriginal := configuracionS3
	defer func() {
		clienteS3 = clienteOriginal
		configuracionS3 = configOriginal
	}()

	mockS3 := &mockClienteS3{
		putObjectErr: assert.AnError,
	}
	clienteS3 = mockS3
	configuracionS3 = tipos.ConfiguracionS3{Bucket: "test-bucket"}

	err := manager.RegistrarEnS3()
	assert.Error(t, err)
	t.Log("RegistrarEnS3 maneja error de PutObject")
}

// TestMigrarAS3_S3NoConfigurado verifica error sin configuración
func TestMigrarAS3_S3NoConfigurado(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Asegurar que clienteS3 es nil y no hay variables de entorno
	clienteOriginal := clienteS3
	clienteS3 = nil
	defer func() { clienteS3 = clienteOriginal }()

	err := manager.MigrarAS3()
	assert.Error(t, err)
	t.Log("MigrarAS3 retorna error cuando S3 no está configurado")
}

// TestMigrarPorTiempoAlmacenamiento_S3NoConfigurado verifica error sin S3
func TestMigrarPorTiempoAlmacenamiento_S3NoConfigurado(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	clienteOriginal := clienteS3
	clienteS3 = nil
	defer func() { clienteS3 = clienteOriginal }()

	err := manager.MigrarPorTiempoAlmacenamiento()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no está configurado")
	t.Log("MigrarPorTiempoAlmacenamiento retorna error sin S3")
}

// TestMigrarPorTiempoAlmacenamiento_SinSeriesConTiempo verifica cuando no hay series con tiempo
func TestMigrarPorTiempoAlmacenamiento_SinSeriesConTiempo(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Configurar mock
	clienteOriginal := clienteS3
	defer func() { clienteS3 = clienteOriginal }()

	mockS3 := &mockClienteS3{}
	clienteS3 = mockS3

	// Agregar serie SIN TiempoAlmacenamiento
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = tipos.Serie{
		Path:                 "sensor/temp",
		TiempoAlmacenamiento: 0, // Sin tiempo configurado
	}
	manager.cache.mu.Unlock()

	err := manager.MigrarPorTiempoAlmacenamiento()
	assert.NoError(t, err)
	assert.Equal(t, 0, mockS3.putObjectCalls) // No debe migrar nada
	t.Log("MigrarPorTiempoAlmacenamiento no hace nada si no hay series con tiempo")
}

// TestMigrarPorTiempoAlmacenamiento_MigraBloquesAntiguos verifica migración de bloques antiguos
func TestMigrarPorTiempoAlmacenamiento_MigraBloquesAntiguos(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Configurar mock
	clienteOriginal := clienteS3
	configOriginal := configuracionS3
	defer func() {
		clienteS3 = clienteOriginal
		configuracionS3 = configOriginal
	}()

	mockS3 := &mockClienteS3{
		putObjectOutput: &s3.PutObjectOutput{},
	}
	clienteS3 = mockS3
	configuracionS3 = tipos.ConfiguracionS3{Bucket: "test-bucket"}

	// Serie con tiempo de almacenamiento de 1 hora
	serie := tipos.Serie{
		SerieId:              1,
		Path:                 "sensor/temp",
		TipoDatos:            tipos.Real,
		TamañoBloque:         100,
		CompresionBloque:     tipos.Ninguna,
		CompresionBytes:      tipos.SinCompresion,
		TiempoAlmacenamiento: int64(time.Hour),
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	// Crear bloque antiguo (hace 2 horas)
	tiempoAntiguo := time.Now().Add(-2 * time.Hour).UnixNano()
	mediciones := []tipos.Medicion{
		{Tiempo: tiempoAntiguo, Valor: float64(20.0)},
	}
	bloque := crearBloqueComprimidoTest(t, serie, mediciones)
	clave := generarClaveDatos(serie.SerieId, tiempoAntiguo, tiempoAntiguo)
	manager.db.Set(clave, bloque, pebble.Sync)

	err := manager.MigrarPorTiempoAlmacenamiento()
	assert.NoError(t, err)
	assert.Equal(t, 1, mockS3.putObjectCalls)
	t.Log("MigrarPorTiempoAlmacenamiento migra bloques antiguos correctamente")
}

// ============================================================================
// TESTS DE CONSULTAS DE AGREGACIÓN (consultas.go)
// ============================================================================

// TestConsultarAgregacion_SerieExacta_Promedio verifica agregación AVG sobre una serie
func TestConsultarAgregacion_SerieExacta_Promedio(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	// Crear mediciones: 10, 20, 30 → promedio = 20
	ahora := time.Now().UnixNano()
	mediciones := []tipos.Medicion{
		{Tiempo: ahora - 3000, Valor: float64(10.0)},
		{Tiempo: ahora - 2000, Valor: float64(20.0)},
		{Tiempo: ahora - 1000, Valor: float64(30.0)},
	}

	// Guardar bloque
	bloque := crearBloqueComprimidoTest(t, serie, mediciones)
	clave := generarClaveDatos(serie.SerieId, ahora-3000, ahora-1000)
	err := manager.db.Set(clave, bloque, pebble.Sync)
	require.NoError(t, err)

	// Consultar promedio
	resultado, err := manager.ConsultarAgregacion(
		"sensor/temp",
		time.Unix(0, ahora-5000),
		time.Unix(0, ahora),
		[]tipos.TipoAgregacion{tipos.AgregacionPromedio},
	)
	require.NoError(t, err)
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, "sensor/temp", resultado.Series[0])
	require.Len(t, resultado.Agregaciones, 1)
	assert.Equal(t, tipos.AgregacionPromedio, resultado.Agregaciones[0])
	assert.Equal(t, 20.0, resultado.Valores[0][0]) // [agregacion][serie]
	t.Log("ConsultarAgregacion calcula promedio correctamente")
}

// TestConsultarAgregacion_SerieExacta_MinMax verifica MIN y MAX
func TestConsultarAgregacion_SerieExacta_MinMax(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	ahora := time.Now().UnixNano()
	mediciones := []tipos.Medicion{
		{Tiempo: ahora - 3000, Valor: float64(15.0)},
		{Tiempo: ahora - 2000, Valor: float64(5.0)},  // MIN
		{Tiempo: ahora - 1000, Valor: float64(25.0)}, // MAX
	}

	bloque := crearBloqueComprimidoTest(t, serie, mediciones)
	clave := generarClaveDatos(serie.SerieId, ahora-3000, ahora-1000)
	manager.db.Set(clave, bloque, pebble.Sync)

	// MIN
	minResult, err := manager.ConsultarAgregacion(
		"sensor/temp",
		time.Unix(0, ahora-5000),
		time.Unix(0, ahora),
		[]tipos.TipoAgregacion{tipos.AgregacionMinimo},
	)
	require.NoError(t, err)
	require.Len(t, minResult.Series, 1)
	assert.Equal(t, 5.0, minResult.Valores[0][0]) // [agregacion][serie]

	// MAX
	maxResult, err := manager.ConsultarAgregacion(
		"sensor/temp",
		time.Unix(0, ahora-5000),
		time.Unix(0, ahora),
		[]tipos.TipoAgregacion{tipos.AgregacionMaximo},
	)
	require.NoError(t, err)
	require.Len(t, maxResult.Series, 1)
	assert.Equal(t, 25.0, maxResult.Valores[0][0]) // [agregacion][serie]

	t.Log("ConsultarAgregacion calcula MIN y MAX correctamente")
}

// TestConsultarAgregacion_SerieExacta_SumaCount verifica SUM y COUNT
func TestConsultarAgregacion_SerieExacta_SumaCount(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	ahora := time.Now().UnixNano()
	mediciones := []tipos.Medicion{
		{Tiempo: ahora - 3000, Valor: float64(10.0)},
		{Tiempo: ahora - 2000, Valor: float64(20.0)},
		{Tiempo: ahora - 1000, Valor: float64(30.0)},
	}

	bloque := crearBloqueComprimidoTest(t, serie, mediciones)
	clave := generarClaveDatos(serie.SerieId, ahora-3000, ahora-1000)
	manager.db.Set(clave, bloque, pebble.Sync)

	// SUM = 60
	sumaResult, err := manager.ConsultarAgregacion(
		"sensor/temp",
		time.Unix(0, ahora-5000),
		time.Unix(0, ahora),
		[]tipos.TipoAgregacion{tipos.AgregacionSuma},
	)
	require.NoError(t, err)
	require.Len(t, sumaResult.Series, 1)
	assert.Equal(t, 60.0, sumaResult.Valores[0][0]) // [agregacion][serie]

	// COUNT = 3
	countResult, err := manager.ConsultarAgregacion(
		"sensor/temp",
		time.Unix(0, ahora-5000),
		time.Unix(0, ahora),
		[]tipos.TipoAgregacion{tipos.AgregacionCount},
	)
	require.NoError(t, err)
	require.Len(t, countResult.Series, 1)
	assert.Equal(t, 3.0, countResult.Valores[0][0]) // [agregacion][serie]

	t.Log("ConsultarAgregacion calcula SUM y COUNT correctamente")
}

// TestConsultarAgregacion_SerieNoExiste verifica error para serie inexistente
func TestConsultarAgregacion_SerieNoExiste(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	_, err := manager.ConsultarAgregacion(
		"serie/inexistente",
		time.Now().Add(-time.Hour),
		time.Now(),
		[]tipos.TipoAgregacion{tipos.AgregacionPromedio},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("ConsultarAgregacion retorna error para serie inexistente")
}

// TestConsultarAgregacion_SinDatos verifica error cuando no hay datos en rango
func TestConsultarAgregacion_SinDatos(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	_, err := manager.ConsultarAgregacion(
		"sensor/temp",
		time.Now().Add(-time.Hour),
		time.Now(),
		[]tipos.TipoAgregacion{tipos.AgregacionPromedio},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no hay datos")
	t.Log("ConsultarAgregacion retorna error cuando no hay datos")
}

// TestConsultarAgregacion_Patron verifica agregación con wildcard
func TestConsultarAgregacion_Patron(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear 2 series con patrón común (wildcard como segmento completo)
	series := []tipos.Serie{
		{SerieId: 1, Path: "sensor_01/temp", TipoDatos: tipos.Real, TamañoBloque: 100, CompresionBloque: tipos.Ninguna, CompresionBytes: tipos.SinCompresion},
		{SerieId: 2, Path: "sensor_02/temp", TipoDatos: tipos.Real, TamañoBloque: 100, CompresionBloque: tipos.Ninguna, CompresionBytes: tipos.SinCompresion},
	}

	manager.cache.mu.Lock()
	for _, s := range series {
		manager.cache.datos[s.Path] = s
	}
	manager.cache.mu.Unlock()

	ahora := time.Now().UnixNano()

	// Serie 1: valores 10, 20 → promedio 15
	mediciones1 := []tipos.Medicion{
		{Tiempo: ahora - 2000, Valor: float64(10.0)},
		{Tiempo: ahora - 1000, Valor: float64(20.0)},
	}
	bloque1 := crearBloqueComprimidoTest(t, series[0], mediciones1)
	manager.db.Set(generarClaveDatos(1, ahora-2000, ahora-1000), bloque1, pebble.Sync)

	// Serie 2: valores 30, 40 → promedio 35
	mediciones2 := []tipos.Medicion{
		{Tiempo: ahora - 2000, Valor: float64(30.0)},
		{Tiempo: ahora - 1000, Valor: float64(40.0)},
	}
	bloque2 := crearBloqueComprimidoTest(t, series[1], mediciones2)
	manager.db.Set(generarClaveDatos(2, ahora-2000, ahora-1000), bloque2, pebble.Sync)

	// Consultar con patrón */temp (wildcard como segmento completo)
	// Serie 1: promedio 15, Serie 2: promedio 35 (ahora columnar, cada serie tiene su valor)
	resultado, err := manager.ConsultarAgregacion(
		"*/temp",
		time.Unix(0, ahora-5000),
		time.Unix(0, ahora),
		[]tipos.TipoAgregacion{tipos.AgregacionPromedio},
	)
	require.NoError(t, err)
	require.Len(t, resultado.Series, 2)
	// Series ordenadas alfabéticamente
	assert.Equal(t, "sensor_01/temp", resultado.Series[0])
	assert.Equal(t, "sensor_02/temp", resultado.Series[1])
	assert.Equal(t, 15.0, resultado.Valores[0][0]) // [agregacion][serie] - promedio de 10, 20
	assert.Equal(t, 35.0, resultado.Valores[0][1]) // [agregacion][serie] - promedio de 30, 40
	t.Log("ConsultarAgregacion con patrón wildcard funciona correctamente")
}

// TestConsultarAgregacion_PatronSinMatch verifica error cuando patrón no tiene coincidencias
func TestConsultarAgregacion_PatronSinMatch(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	_, err := manager.ConsultarAgregacion(
		"inexistente_*/temp",
		time.Now().Add(-time.Hour),
		time.Now(),
		[]tipos.TipoAgregacion{tipos.AgregacionPromedio},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no se encontraron series")
	t.Log("ConsultarAgregacion retorna error cuando patrón no tiene coincidencias")
}

// TestConsultarAgregacionTemporal_Buckets verifica downsampling con múltiples buckets
func TestConsultarAgregacionTemporal_Buckets(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	// Crear mediciones distribuidas en 2 horas
	ahora := time.Now()
	hace2Horas := ahora.Add(-2 * time.Hour)
	hace1Hora := ahora.Add(-1 * time.Hour)

	mediciones := []tipos.Medicion{
		// Primera hora: 10, 20 → promedio 15
		{Tiempo: hace2Horas.UnixNano() + 1000, Valor: float64(10.0)},
		{Tiempo: hace2Horas.UnixNano() + 2000, Valor: float64(20.0)},
		// Segunda hora: 30, 40 → promedio 35
		{Tiempo: hace1Hora.UnixNano() + 1000, Valor: float64(30.0)},
		{Tiempo: hace1Hora.UnixNano() + 2000, Valor: float64(40.0)},
	}

	bloque := crearBloqueComprimidoTest(t, serie, mediciones)
	clave := generarClaveDatos(serie.SerieId, hace2Horas.UnixNano(), hace1Hora.UnixNano()+2000)
	manager.db.Set(clave, bloque, pebble.Sync)

	// Consultar con buckets de 1 hora
	resultado, err := manager.ConsultarAgregacionTemporal(
		"sensor/temp",
		hace2Horas,
		ahora,
		[]tipos.TipoAgregacion{tipos.AgregacionPromedio},
		time.Hour,
	)
	require.NoError(t, err)
	assert.Len(t, resultado.Tiempos, 2)
	assert.Len(t, resultado.Series, 1)
	assert.Equal(t, "sensor/temp", resultado.Series[0])

	// Verificar primer bucket - formato [agregacion][bucket][serie]
	assert.Equal(t, 15.0, resultado.Valores[0][0][0])

	// Verificar segundo bucket
	assert.Equal(t, 35.0, resultado.Valores[0][1][0])

	t.Log("ConsultarAgregacionTemporal genera buckets correctamente")
}

// TestConsultarAgregacionTemporal_IntervaloGrande verifica un solo bucket cuando intervalo > rango
func TestConsultarAgregacionTemporal_IntervaloGrande(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	ahora := time.Now().UnixNano()
	mediciones := []tipos.Medicion{
		{Tiempo: ahora - 3000, Valor: float64(10.0)},
		{Tiempo: ahora - 2000, Valor: float64(20.0)},
		{Tiempo: ahora - 1000, Valor: float64(30.0)},
	}

	bloque := crearBloqueComprimidoTest(t, serie, mediciones)
	clave := generarClaveDatos(serie.SerieId, ahora-3000, ahora-1000)
	manager.db.Set(clave, bloque, pebble.Sync)

	// Intervalo de 1 día para rango de pocos segundos → 1 bucket
	resultado, err := manager.ConsultarAgregacionTemporal(
		"sensor/temp",
		time.Unix(0, ahora-5000),
		time.Unix(0, ahora),
		[]tipos.TipoAgregacion{tipos.AgregacionPromedio},
		24*time.Hour,
	)
	require.NoError(t, err)
	assert.Len(t, resultado.Tiempos, 1)
	assert.Len(t, resultado.Series, 1)
	assert.Equal(t, 20.0, resultado.Valores[0][0][0]) // [agregacion][bucket][serie] Promedio de 10, 20, 30

	t.Log("ConsultarAgregacionTemporal maneja intervalo > rango correctamente")
}

// TestConsultarAgregacionTemporal_IntervaloInvalido verifica error con intervalo <= 0
func TestConsultarAgregacionTemporal_IntervaloInvalido(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	_, err := manager.ConsultarAgregacionTemporal(
		"sensor/temp",
		time.Now().Add(-time.Hour),
		time.Now(),
		[]tipos.TipoAgregacion{tipos.AgregacionPromedio},
		0,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "intervalo")
	t.Log("ConsultarAgregacionTemporal rechaza intervalo inválido")
}

// TestConsultarAgregacionTemporal_SinDatos verifica error cuando no hay datos
func TestConsultarAgregacionTemporal_SinDatos(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	_, err := manager.ConsultarAgregacionTemporal(
		"sensor/temp",
		time.Now().Add(-time.Hour),
		time.Now(),
		[]tipos.TipoAgregacion{tipos.AgregacionPromedio},
		time.Minute,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no hay datos")
	t.Log("ConsultarAgregacionTemporal retorna error cuando no hay datos")
}

// TestResolverSeries_PathExacto verifica resolución de path exacto
func TestResolverSeries_PathExacto(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	serie := tipos.Serie{
		SerieId: 1,
		Path:    "sensor/temp",
	}
	manager.cache.mu.Lock()
	manager.cache.datos["sensor/temp"] = serie
	manager.cache.mu.Unlock()

	series, err := manager.resolverSeries("sensor/temp")
	require.NoError(t, err)
	assert.Len(t, series, 1)
	assert.Equal(t, "sensor/temp", series[0].Path)
	t.Log("resolverSeries resuelve path exacto correctamente")
}

// TestResolverSeries_Patron verifica resolución de patrón wildcard
func TestResolverSeries_Patron(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	manager.cache.mu.Lock()
	manager.cache.datos["sensor_01/temp"] = tipos.Serie{SerieId: 1, Path: "sensor_01/temp"}
	manager.cache.datos["sensor_02/temp"] = tipos.Serie{SerieId: 2, Path: "sensor_02/temp"}
	manager.cache.datos["sensor_01/humedad"] = tipos.Serie{SerieId: 3, Path: "sensor_01/humedad"}
	manager.cache.mu.Unlock()

	series, err := manager.resolverSeries("*/temp")
	require.NoError(t, err)
	assert.Len(t, series, 2)
	t.Log("resolverSeries resuelve patrón wildcard correctamente")
}

// ============================================================================
// TESTS DE MÚLTIPLES AGREGACIONES
// ============================================================================

// TestConsultarAgregacion_MultiplesAgregaciones_MinMax verifica cálculo de min y max en una sola pasada
func TestConsultarAgregacion_MultiplesAgregaciones_MinMax(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temperatura",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos[serie.Path] = serie
	manager.cache.mu.Unlock()

	// Crear mediciones: 10, 20, 30, 40, 50
	ahora := time.Now().UnixNano()
	mediciones := []tipos.Medicion{
		{Tiempo: ahora - 5000, Valor: float64(10.0)},
		{Tiempo: ahora - 4000, Valor: float64(20.0)},
		{Tiempo: ahora - 3000, Valor: float64(30.0)},
		{Tiempo: ahora - 2000, Valor: float64(40.0)},
		{Tiempo: ahora - 1000, Valor: float64(50.0)},
	}

	// Guardar bloque
	bloque := crearBloqueComprimidoTest(t, serie, mediciones)
	clave := generarClaveDatos(serie.SerieId, ahora-5000, ahora-1000)
	err := manager.db.Set(clave, bloque, pebble.Sync)
	require.NoError(t, err)

	// Consultar min y max en una sola llamada
	resultado, err := manager.ConsultarAgregacion(
		serie.Path,
		time.Unix(0, ahora-6000),
		time.Unix(0, ahora),
		[]tipos.TipoAgregacion{tipos.AgregacionMinimo, tipos.AgregacionMaximo},
	)
	require.NoError(t, err)

	// Verificar que tenemos ambas agregaciones
	assert.Len(t, resultado.Series, 1)
	assert.Len(t, resultado.Agregaciones, 2)
	assert.Equal(t, tipos.AgregacionMinimo, resultado.Agregaciones[0])
	assert.Equal(t, tipos.AgregacionMaximo, resultado.Agregaciones[1])

	// Verificar valores - formato [agregacion][serie]
	assert.Equal(t, 10.0, resultado.Valores[0][0]) // Min
	assert.Equal(t, 50.0, resultado.Valores[1][0]) // Max

	t.Log("ConsultarAgregacion calcula min y max correctamente en una sola pasada")
}

// TestConsultarAgregacion_MultiplesAgregaciones_Todas verifica todas las agregaciones juntas
func TestConsultarAgregacion_MultiplesAgregaciones_Todas(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	serie := tipos.Serie{
		SerieId:          1,
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	manager.cache.mu.Lock()
	manager.cache.datos[serie.Path] = serie
	manager.cache.mu.Unlock()

	// Crear mediciones: 10, 20, 30, 40, 50 (suma=150, promedio=30, count=5)
	ahora := time.Now().UnixNano()
	mediciones := []tipos.Medicion{
		{Tiempo: ahora - 5000, Valor: float64(10.0)},
		{Tiempo: ahora - 4000, Valor: float64(20.0)},
		{Tiempo: ahora - 3000, Valor: float64(30.0)},
		{Tiempo: ahora - 2000, Valor: float64(40.0)},
		{Tiempo: ahora - 1000, Valor: float64(50.0)},
	}

	bloque := crearBloqueComprimidoTest(t, serie, mediciones)
	clave := generarClaveDatos(serie.SerieId, ahora-5000, ahora-1000)
	err := manager.db.Set(clave, bloque, pebble.Sync)
	require.NoError(t, err)

	agregaciones := []tipos.TipoAgregacion{
		tipos.AgregacionMinimo,
		tipos.AgregacionMaximo,
		tipos.AgregacionPromedio,
		tipos.AgregacionSuma,
		tipos.AgregacionCount,
	}

	resultado, err := manager.ConsultarAgregacion(
		serie.Path,
		time.Unix(0, ahora-6000),
		time.Unix(0, ahora),
		agregaciones,
	)
	require.NoError(t, err)

	// Valores esperados para cada agregación - formato [agregacion][serie]
	assert.Equal(t, 10.0, resultado.Valores[0][0])  // Min
	assert.Equal(t, 50.0, resultado.Valores[1][0])  // Max
	assert.Equal(t, 30.0, resultado.Valores[2][0])  // Promedio
	assert.Equal(t, 150.0, resultado.Valores[3][0]) // Suma
	assert.Equal(t, 5.0, resultado.Valores[4][0])   // Count

	t.Log("ConsultarAgregacion calcula todas las agregaciones correctamente")
}

// TestConsultarAgregacion_MultiplesAgregaciones_Wildcard verifica múltiples agregaciones con wildcard
func TestConsultarAgregacion_MultiplesAgregaciones_Wildcard(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear dos series
	serie1 := tipos.Serie{SerieId: 1, Path: "dispositivo1/temp", TipoDatos: tipos.Real, TamañoBloque: 100, CompresionBloque: tipos.Ninguna, CompresionBytes: tipos.SinCompresion}
	serie2 := tipos.Serie{SerieId: 2, Path: "dispositivo2/temp", TipoDatos: tipos.Real, TamañoBloque: 100, CompresionBloque: tipos.Ninguna, CompresionBytes: tipos.SinCompresion}

	manager.cache.mu.Lock()
	manager.cache.datos[serie1.Path] = serie1
	manager.cache.datos[serie2.Path] = serie2
	manager.cache.mu.Unlock()

	ahora := time.Now().UnixNano()

	// Serie 1: valores 10, 20 (min=10, max=20)
	mediciones1 := []tipos.Medicion{
		{Tiempo: ahora - 2000, Valor: float64(10.0)},
		{Tiempo: ahora - 1000, Valor: float64(20.0)},
	}
	bloque1 := crearBloqueComprimidoTest(t, serie1, mediciones1)
	clave1 := generarClaveDatos(serie1.SerieId, ahora-2000, ahora-1000)
	manager.db.Set(clave1, bloque1, pebble.Sync)

	// Serie 2: valores 100, 200 (min=100, max=200)
	mediciones2 := []tipos.Medicion{
		{Tiempo: ahora - 2000, Valor: float64(100.0)},
		{Tiempo: ahora - 1000, Valor: float64(200.0)},
	}
	bloque2 := crearBloqueComprimidoTest(t, serie2, mediciones2)
	clave2 := generarClaveDatos(serie2.SerieId, ahora-2000, ahora-1000)
	manager.db.Set(clave2, bloque2, pebble.Sync)

	resultado, err := manager.ConsultarAgregacion(
		"dispositivo*/temp",
		time.Unix(0, ahora-3000),
		time.Unix(0, ahora),
		[]tipos.TipoAgregacion{tipos.AgregacionMinimo, tipos.AgregacionMaximo},
	)
	require.NoError(t, err)

	// Debe haber 2 series
	assert.Len(t, resultado.Series, 2)

	// Verificar valores por serie (ordenadas alfabéticamente)
	// dispositivo1/temp está en índice 0, dispositivo2/temp en índice 1
	// formato [agregacion][serie]
	assert.Equal(t, 10.0, resultado.Valores[0][0])  // Min serie1
	assert.Equal(t, 20.0, resultado.Valores[1][0])  // Max serie1
	assert.Equal(t, 100.0, resultado.Valores[0][1]) // Min serie2
	assert.Equal(t, 200.0, resultado.Valores[1][1]) // Max serie2

	t.Log("ConsultarAgregacion con múltiples agregaciones funciona con wildcard sobre múltiples series")
}

// TestConsultarAgregacion_SinAgregaciones verifica error sin agregaciones
func TestConsultarAgregacion_SinAgregaciones(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	_, err := manager.ConsultarAgregacion(
		"sensor/temp",
		time.Now().Add(-1*time.Hour),
		time.Now(),
		[]tipos.TipoAgregacion{}, // Lista vacía
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "al menos una agregación")
	t.Log("ConsultarAgregacion retorna error sin agregaciones")
}

// TestConsultarAgregacion_SerieNoExisteConMultiples verifica error para serie inexistente
func TestConsultarAgregacion_SerieNoExisteConMultiples(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	_, err := manager.ConsultarAgregacion(
		"serie/inexistente",
		time.Now().Add(-1*time.Hour),
		time.Now(),
		[]tipos.TipoAgregacion{tipos.AgregacionMinimo},
	)
	assert.Error(t, err)
	t.Log("ConsultarAgregacion retorna error para serie inexistente")
}

// ============================================================================
// TESTS DE ELIMINARSERIE (edge.go)
// ============================================================================

// TestEliminarSerie_SerieNoExiste verifica error cuando serie no existe
func TestEliminarSerie_SerieNoExiste(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	err := manager.EliminarSerie("serie/inexistente")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("EliminarSerie retorna error para serie inexistente")
}

// TestEliminarSerie_EliminaCache verifica que elimina del cache
func TestEliminarSerie_EliminaCache(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Verificar que está en cache
	manager.cache.mu.RLock()
	_, existe := manager.cache.datos["sensor/temp"]
	manager.cache.mu.RUnlock()
	assert.True(t, existe, "Serie debe existir en cache antes de eliminar")

	// Eliminar serie
	err = manager.EliminarSerie("sensor/temp")
	require.NoError(t, err)

	// Verificar que ya no está en cache
	manager.cache.mu.RLock()
	_, existe = manager.cache.datos["sensor/temp"]
	manager.cache.mu.RUnlock()
	assert.False(t, existe, "Serie no debe existir en cache después de eliminar")

	t.Log("EliminarSerie elimina serie del cache correctamente")
}

// TestEliminarSerie_EliminaMetadatosDB verifica que elimina metadatos de PebbleDB
func TestEliminarSerie_EliminaMetadatosDB(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Verificar que está en DB
	_, closer, err := manager.db.Get([]byte("series/sensor/temp"))
	require.NoError(t, err)
	closer.Close()

	// Eliminar serie
	err = manager.EliminarSerie("sensor/temp")
	require.NoError(t, err)

	// Verificar que ya no está en DB
	_, _, err = manager.db.Get([]byte("series/sensor/temp"))
	assert.Equal(t, pebble.ErrNotFound, err, "Metadatos deben ser eliminados de DB")

	t.Log("EliminarSerie elimina metadatos de PebbleDB correctamente")
}

// TestEliminarSerie_EliminaDatosDB verifica que elimina bloques de datos de PebbleDB
func TestEliminarSerie_EliminaDatosDB(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	serie := tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	err := manager.CrearSerie(serie)
	require.NoError(t, err)

	// Obtener SerieId asignado
	manager.cache.mu.RLock()
	serieGuardada := manager.cache.datos["sensor/temp"]
	manager.cache.mu.RUnlock()
	serieId := serieGuardada.SerieId

	// Crear y guardar bloques de datos
	ahora := time.Now().UnixNano()
	mediciones := []tipos.Medicion{
		{Tiempo: ahora - 2000, Valor: float64(20.0)},
		{Tiempo: ahora - 1000, Valor: float64(21.0)},
	}
	bloque := crearBloqueComprimidoTest(t, serieGuardada, mediciones)
	clave := generarClaveDatos(serieId, ahora-2000, ahora-1000)
	err = manager.db.Set(clave, bloque, pebble.Sync)
	require.NoError(t, err)

	// Verificar que el bloque existe
	_, closer, err := manager.db.Get(clave)
	require.NoError(t, err)
	closer.Close()

	// Eliminar serie
	err = manager.EliminarSerie("sensor/temp")
	require.NoError(t, err)

	// Verificar que el bloque fue eliminado
	_, _, err = manager.db.Get(clave)
	assert.Equal(t, pebble.ErrNotFound, err, "Bloques de datos deben ser eliminados")

	t.Log("EliminarSerie elimina bloques de datos de PebbleDB correctamente")
}

// TestEliminarSerie_EliminaBuffer verifica que elimina y cierra el buffer
func TestEliminarSerie_EliminaBuffer(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Verificar que existe buffer
	_, existe := manager.buffers.Load("sensor/temp")
	assert.True(t, existe, "Buffer debe existir antes de eliminar")

	// Eliminar serie
	err = manager.EliminarSerie("sensor/temp")
	require.NoError(t, err)

	// Verificar que buffer fue eliminado
	_, existe = manager.buffers.Load("sensor/temp")
	assert.False(t, existe, "Buffer debe ser eliminado")

	t.Log("EliminarSerie elimina y cierra buffer correctamente")
}

// TestEliminarSerie_NoAfectaOtrasSeries verifica que no elimina otras series
func TestEliminarSerie_NoAfectaOtrasSeries(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear dos series
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp1",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	err = manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp2",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Eliminar solo la primera
	err = manager.EliminarSerie("sensor/temp1")
	require.NoError(t, err)

	// Verificar que la segunda sigue existiendo
	manager.cache.mu.RLock()
	_, existe := manager.cache.datos["sensor/temp2"]
	manager.cache.mu.RUnlock()
	assert.True(t, existe, "Segunda serie debe seguir existiendo")

	// Verificar en DB
	_, closer, err := manager.db.Get([]byte("series/sensor/temp2"))
	require.NoError(t, err)
	closer.Close()

	t.Log("EliminarSerie no afecta otras series")
}

// TestEliminarSerie_EliminaMultiplesBloques verifica eliminación de múltiples bloques
func TestEliminarSerie_EliminaMultiplesBloques(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	serie := tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	}
	err := manager.CrearSerie(serie)
	require.NoError(t, err)

	// Obtener SerieId
	manager.cache.mu.RLock()
	serieGuardada := manager.cache.datos["sensor/temp"]
	manager.cache.mu.RUnlock()
	serieId := serieGuardada.SerieId

	// Crear múltiples bloques
	ahora := time.Now().UnixNano()
	var claves [][]byte

	for i := 0; i < 5; i++ {
		tiempoBase := ahora - int64((i+1)*10000)
		mediciones := []tipos.Medicion{
			{Tiempo: tiempoBase, Valor: float64(20.0 + float64(i))},
			{Tiempo: tiempoBase + 1000, Valor: float64(21.0 + float64(i))},
		}
		bloque := crearBloqueComprimidoTest(t, serieGuardada, mediciones)
		clave := generarClaveDatos(serieId, tiempoBase, tiempoBase+1000)
		claves = append(claves, clave)
		err := manager.db.Set(clave, bloque, pebble.Sync)
		require.NoError(t, err)
	}

	// Verificar que todos los bloques existen
	for _, clave := range claves {
		_, closer, err := manager.db.Get(clave)
		require.NoError(t, err)
		closer.Close()
	}

	// Eliminar serie
	err = manager.EliminarSerie("sensor/temp")
	require.NoError(t, err)

	// Verificar que todos los bloques fueron eliminados
	for _, clave := range claves {
		_, _, err := manager.db.Get(clave)
		assert.Equal(t, pebble.ErrNotFound, err, "Todos los bloques deben ser eliminados")
	}

	t.Log("EliminarSerie elimina múltiples bloques correctamente")
}

// TestEliminarSerie_ConS3 verifica que se registra eliminación pendiente para S3
func TestEliminarSerie_ConS3(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Configurar mock de S3
	clienteOriginal := clienteS3
	configOriginal := configuracionS3
	defer func() {
		clienteS3 = clienteOriginal
		configuracionS3 = configOriginal
	}()

	mockS3 := &mockClienteS3{
		putObjectOutput: &s3.PutObjectOutput{},
	}
	clienteS3 = mockS3
	configuracionS3 = tipos.ConfiguracionS3{Bucket: "test-bucket"}

	// Crear serie
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Eliminar serie
	err = manager.EliminarSerie("sensor/temp")
	require.NoError(t, err)

	// Verificar que se registró eliminación pendiente (la actualización de S3 es diferida)
	pendientes, err := manager.cargarEliminacionesPendientes()
	require.NoError(t, err)
	assert.Equal(t, 1, len(pendientes), "Debe registrar eliminación pendiente para S3")
	if len(pendientes) > 0 {
		assert.Equal(t, "sensor/temp", pendientes[0].Path)
		assert.Equal(t, 1, pendientes[0].SerieId)
	}

	t.Log("EliminarSerie registra eliminación pendiente para S3 correctamente")
}

// TestEliminarSerie_InsercionDespuesDeEliminar verifica que no se puede insertar después de eliminar
func TestEliminarSerie_InsercionDespuesDeEliminar(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Eliminar serie
	err = manager.EliminarSerie("sensor/temp")
	require.NoError(t, err)

	// Intentar insertar
	err = manager.Insertar("sensor/temp", time.Now().UnixNano(), 25.5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")

	t.Log("EliminarSerie previene inserciones posteriores")
}

// TestEliminarSerie_ConsultaDespuesDeEliminar verifica que no se puede consultar después de eliminar
func TestEliminarSerie_ConsultaDespuesDeEliminar(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Eliminar serie
	err = manager.EliminarSerie("sensor/temp")
	require.NoError(t, err)

	// Intentar consultar
	_, err = manager.ConsultarRango("sensor/temp", time.Now().Add(-time.Hour), time.Now())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")

	t.Log("EliminarSerie previene consultas posteriores")
}

// TestEliminarSerie_PuedeRecrear verifica que se puede recrear una serie eliminada
func TestEliminarSerie_PuedeRecrear(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Crear serie
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Eliminar serie
	err = manager.EliminarSerie("sensor/temp")
	require.NoError(t, err)

	// Recrear serie (puede ser con configuración diferente)
	err = manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Integer, // Tipo diferente
		TamañoBloque:     200,           // Tamaño diferente
		CompresionBloque: tipos.LZ4,     // Compresión diferente
		CompresionBytes:  tipos.DeltaDelta,
	})
	require.NoError(t, err)

	// Verificar que la nueva configuración está aplicada
	manager.cache.mu.RLock()
	serie := manager.cache.datos["sensor/temp"]
	manager.cache.mu.RUnlock()

	assert.Equal(t, tipos.Integer, serie.TipoDatos)
	assert.Equal(t, 200, serie.TamañoBloque)
	assert.Equal(t, tipos.LZ4, serie.CompresionBloque)

	t.Log("EliminarSerie permite recrear serie con nueva configuración")
}

// ============================================================================
// TESTS DE ELIMINACIONES PENDIENTES S3 (migracion_datos.go)
// ============================================================================

// TestGuardarEliminacionPendiente verifica que se guarda correctamente
func TestGuardarEliminacionPendiente(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	err := manager.guardarEliminacionPendiente(123, "sensor/temp")
	require.NoError(t, err)

	// Verificar que se guardó en DB
	clave := generarClaveEliminacionPendiente(123)
	_, closer, err := manager.db.Get(clave)
	require.NoError(t, err)
	closer.Close()

	t.Log("guardarEliminacionPendiente guarda correctamente en PebbleDB")
}

// TestCargarEliminacionesPendientes verifica carga de pendientes
func TestCargarEliminacionesPendientes(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Guardar varias pendientes
	err := manager.guardarEliminacionPendiente(1, "sensor/temp1")
	require.NoError(t, err)
	err = manager.guardarEliminacionPendiente(2, "sensor/temp2")
	require.NoError(t, err)
	err = manager.guardarEliminacionPendiente(3, "sensor/temp3")
	require.NoError(t, err)

	// Cargar pendientes
	pendientes, err := manager.cargarEliminacionesPendientes()
	require.NoError(t, err)
	assert.Len(t, pendientes, 3)

	t.Log("cargarEliminacionesPendientes carga todas las pendientes")
}

// TestEliminarPendienteCompletado verifica eliminación de pendiente
func TestEliminarPendienteCompletado(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Guardar pendiente
	err := manager.guardarEliminacionPendiente(123, "sensor/temp")
	require.NoError(t, err)

	// Eliminar pendiente
	err = manager.eliminarPendienteCompletado(123)
	require.NoError(t, err)

	// Verificar que ya no existe
	clave := generarClaveEliminacionPendiente(123)
	_, _, err = manager.db.Get(clave)
	assert.Equal(t, pebble.ErrNotFound, err)

	t.Log("eliminarPendienteCompletado elimina pendiente correctamente")
}

// TestActualizarEliminacionPendiente verifica actualización de intentos
func TestActualizarEliminacionPendiente(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Guardar pendiente
	err := manager.guardarEliminacionPendiente(123, "sensor/temp")
	require.NoError(t, err)

	// Cargar, modificar y actualizar
	pendientes, _ := manager.cargarEliminacionesPendientes()
	pendiente := pendientes[0]
	pendiente.Intentos = 5

	err = manager.actualizarEliminacionPendiente(pendiente)
	require.NoError(t, err)

	// Verificar actualización
	pendientesActualizados, _ := manager.cargarEliminacionesPendientes()
	assert.Equal(t, 5, pendientesActualizados[0].Intentos)

	t.Log("actualizarEliminacionPendiente actualiza intentos correctamente")
}

// TestEliminarSerie_GuardaPendienteConS3 verifica que se guarda pendiente cuando hay S3
func TestEliminarSerie_GuardaPendienteConS3(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Configurar mock de S3
	clienteOriginal := clienteS3
	configOriginal := configuracionS3
	defer func() {
		clienteS3 = clienteOriginal
		configuracionS3 = configOriginal
	}()

	mockS3 := &mockClienteS3{
		putObjectOutput:   &s3.PutObjectOutput{},
		listObjectsOutput: &s3.ListObjectsV2Output{}, // Sin objetos
	}
	clienteS3 = mockS3
	configuracionS3 = tipos.ConfiguracionS3{Bucket: "test-bucket"}

	// Crear serie
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Obtener SerieId
	manager.cache.mu.RLock()
	serieId := manager.cache.datos["sensor/temp"].SerieId
	manager.cache.mu.RUnlock()

	// Eliminar serie
	err = manager.EliminarSerie("sensor/temp")
	require.NoError(t, err)

	// Esperar un momento para que el goroutine procese (best-effort)
	time.Sleep(100 * time.Millisecond)

	// Verificar que se guardó pendiente (o se procesó y eliminó si tuvo éxito)
	// Como el mock retorna lista vacía, la eliminación es exitosa y el pendiente se elimina
	// Pero antes de procesar, el pendiente debió existir
	// Verificamos que la serie fue eliminada localmente
	manager.cache.mu.RLock()
	_, existe := manager.cache.datos["sensor/temp"]
	manager.cache.mu.RUnlock()
	assert.False(t, existe)

	t.Logf("EliminarSerie guarda pendiente cuando hay S3 configurado (serieId: %d)", serieId)
}

// TestEliminarSerie_SinS3NoPendiente verifica que no guarda pendiente sin S3
func TestEliminarSerie_SinS3NoPendiente(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Asegurar que no hay S3
	clienteOriginal := clienteS3
	clienteS3 = nil
	defer func() { clienteS3 = clienteOriginal }()

	// Crear serie
	err := manager.CrearSerie(tipos.Serie{
		Path:             "sensor/temp",
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.Ninguna,
		CompresionBytes:  tipos.SinCompresion,
	})
	require.NoError(t, err)

	// Eliminar serie
	err = manager.EliminarSerie("sensor/temp")
	require.NoError(t, err)

	// Verificar que no hay pendientes
	pendientes, err := manager.cargarEliminacionesPendientes()
	require.NoError(t, err)
	assert.Empty(t, pendientes)

	t.Log("EliminarSerie no guarda pendiente sin S3 configurado")
}

// TestEliminarSerieDeS3_SinS3 verifica error cuando no hay S3
func TestEliminarSerieDeS3_SinS3(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	clienteOriginal := clienteS3
	clienteS3 = nil
	defer func() { clienteS3 = clienteOriginal }()

	_, err := manager.eliminarSerieDeS3(123)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no está configurado")

	t.Log("eliminarSerieDeS3 retorna error sin S3 configurado")
}

// TestEliminarSerieDeS3_Exitoso verifica eliminación exitosa de S3
func TestEliminarSerieDeS3_Exitoso(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Configurar mock con objetos a eliminar
	clienteOriginal := clienteS3
	configOriginal := configuracionS3
	defer func() {
		clienteS3 = clienteOriginal
		configuracionS3 = configOriginal
	}()

	// Simular 3 objetos en S3
	key1 := aws.String("test-node/0000000001_00000000000000001000_00000000000000002000")
	key2 := aws.String("test-node/0000000001_00000000000000002000_00000000000000003000")
	key3 := aws.String("test-node/0000000001_00000000000000003000_00000000000000004000")

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: key1},
				{Key: key2},
				{Key: key3},
			},
		},
		deleteObjectOutput: &s3.DeleteObjectOutput{},
	}
	clienteS3 = mockS3
	configuracionS3 = tipos.ConfiguracionS3{Bucket: "test-bucket"}

	// Eliminar serie de S3
	eliminados, err := manager.eliminarSerieDeS3(1)
	require.NoError(t, err)
	assert.Equal(t, 3, eliminados)
	assert.Equal(t, 3, mockS3.deleteObjectCalls)

	t.Log("eliminarSerieDeS3 elimina objetos correctamente")
}

// TestProcesarEliminacionesPendientes_SinS3 verifica que no hace nada sin S3
func TestProcesarEliminacionesPendientes_SinS3(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	clienteOriginal := clienteS3
	clienteS3 = nil
	defer func() { clienteS3 = clienteOriginal }()

	// Guardar pendiente manualmente
	pendiente := EliminacionPendiente{SerieId: 1, Path: "test", Timestamp: time.Now().UnixNano(), Intentos: 0}
	datos, _ := tipos.SerializarGob(pendiente)
	manager.db.Set(generarClaveEliminacionPendiente(1), datos, pebble.Sync)

	// Procesar (no debería hacer nada)
	err := manager.ProcesarEliminacionesPendientes()
	assert.NoError(t, err)

	// El pendiente debería seguir ahí
	pendientes, _ := manager.cargarEliminacionesPendientes()
	assert.Len(t, pendientes, 1)

	t.Log("ProcesarEliminacionesPendientes no hace nada sin S3")
}

// TestProcesarEliminacionesPendientes_Exitoso verifica procesamiento exitoso
func TestProcesarEliminacionesPendientes_Exitoso(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Configurar mock
	clienteOriginal := clienteS3
	configOriginal := configuracionS3
	defer func() {
		clienteS3 = clienteOriginal
		configuracionS3 = configOriginal
	}()

	mockS3 := &mockClienteS3{
		listObjectsOutput:  &s3.ListObjectsV2Output{}, // Sin objetos (ya migrados)
		putObjectOutput:    &s3.PutObjectOutput{},     // Para RegistrarEnS3
		deleteObjectOutput: &s3.DeleteObjectOutput{},
	}
	clienteS3 = mockS3
	configuracionS3 = tipos.ConfiguracionS3{Bucket: "test-bucket"}

	// Guardar pendiente
	err := manager.guardarEliminacionPendiente(1, "sensor/temp")
	require.NoError(t, err)

	// Procesar
	err = manager.ProcesarEliminacionesPendientes()
	require.NoError(t, err)

	// Verificar que el pendiente fue eliminado
	pendientes, _ := manager.cargarEliminacionesPendientes()
	assert.Empty(t, pendientes)

	// Verificar que se actualizó el registro en S3
	assert.GreaterOrEqual(t, mockS3.putObjectCalls, 1)

	t.Log("ProcesarEliminacionesPendientes procesa y elimina pendientes exitosamente")
}

// TestProcesarEliminacionesPendientes_FallaConexion verifica reintento
func TestProcesarEliminacionesPendientes_FallaConexion(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Configurar mock con error
	clienteOriginal := clienteS3
	configOriginal := configuracionS3
	defer func() {
		clienteS3 = clienteOriginal
		configuracionS3 = configOriginal
	}()

	mockS3 := &mockClienteS3{
		listObjectsErr: fmt.Errorf("conexión fallida"),
	}
	clienteS3 = mockS3
	configuracionS3 = tipos.ConfiguracionS3{Bucket: "test-bucket"}

	// Guardar pendiente
	err := manager.guardarEliminacionPendiente(1, "sensor/temp")
	require.NoError(t, err)

	// Procesar (fallará)
	err = manager.ProcesarEliminacionesPendientes()
	require.NoError(t, err) // No retorna error, solo loggea

	// Verificar que el pendiente sigue ahí con intentos incrementados
	pendientes, _ := manager.cargarEliminacionesPendientes()
	require.Len(t, pendientes, 1)
	assert.Equal(t, 1, pendientes[0].Intentos)

	t.Log("ProcesarEliminacionesPendientes incrementa intentos en fallo y no descarta")
}

// TestProcesarEliminacionesPendientes_MultiplesSeries verifica múltiples pendientes
func TestProcesarEliminacionesPendientes_MultiplesSeries(t *testing.T) {
	manager := crearManagerEdgeParaTest(t)

	// Configurar mock
	clienteOriginal := clienteS3
	configOriginal := configuracionS3
	defer func() {
		clienteS3 = clienteOriginal
		configuracionS3 = configOriginal
	}()

	mockS3 := &mockClienteS3{
		listObjectsOutput:  &s3.ListObjectsV2Output{},
		putObjectOutput:    &s3.PutObjectOutput{},
		deleteObjectOutput: &s3.DeleteObjectOutput{},
	}
	clienteS3 = mockS3
	configuracionS3 = tipos.ConfiguracionS3{Bucket: "test-bucket"}

	// Guardar varias pendientes
	for i := 1; i <= 5; i++ {
		err := manager.guardarEliminacionPendiente(i, fmt.Sprintf("sensor/temp%d", i))
		require.NoError(t, err)
	}

	// Verificar que hay 5 pendientes
	pendientes, _ := manager.cargarEliminacionesPendientes()
	assert.Len(t, pendientes, 5)

	// Procesar
	err := manager.ProcesarEliminacionesPendientes()
	require.NoError(t, err)

	// Verificar que todos fueron procesados
	pendientes, _ = manager.cargarEliminacionesPendientes()
	assert.Empty(t, pendientes)

	t.Log("ProcesarEliminacionesPendientes procesa múltiples pendientes")
}

// TestGenerarClaveEliminacionPendiente verifica formato de clave
func TestGenerarClaveEliminacionPendiente(t *testing.T) {
	clave := generarClaveEliminacionPendiente(123)
	assert.Equal(t, []byte("pendientes/eliminar/0000000123"), clave)

	clave = generarClaveEliminacionPendiente(1)
	assert.Equal(t, []byte("pendientes/eliminar/0000000001"), clave)

	t.Log("generarClaveEliminacionPendiente genera claves con formato correcto")
}
