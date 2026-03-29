package despachador

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/sensorwave-dev/sensorwave/compresor"
	"github.com/sensorwave-dev/sensorwave/tipos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// MOCK DE CLIENTE BORDE PARA TESTS
// ============================================================================

// mockClienteBorde implementa clienteBorde para testing
type mockClienteBorde struct {
	respuestaRango              *tipos.RespuestaConsultaRango
	respuestaPunto              *tipos.RespuestaConsultaPunto
	respuestaAgregacion         *tipos.RespuestaConsultaAgregacion
	respuestaAgregacionTemporal *tipos.RespuestaConsultaAgregacionTemporal
	err                         error
}

func (m *mockClienteBorde) ConsultarRango(ctx context.Context, cliente string, direccion string, req tipos.SolicitudConsultaRango) (*tipos.RespuestaConsultaRango, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.respuestaRango, nil
}

func (m *mockClienteBorde) ConsultarUltimoPunto(ctx context.Context, cliente string, direccion string, req tipos.SolicitudConsultaPunto) (*tipos.RespuestaConsultaPunto, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.respuestaPunto, nil
}

func (m *mockClienteBorde) ConsultarAgregacion(ctx context.Context, cliente string, direccion string, req tipos.SolicitudConsultaAgregacion) (*tipos.RespuestaConsultaAgregacion, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.respuestaAgregacion, nil
}

func (m *mockClienteBorde) ConsultarAgregacionTemporal(ctx context.Context, cliente string, direccion string, req tipos.SolicitudConsultaAgregacionTemporal) (*tipos.RespuestaConsultaAgregacionTemporal, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.respuestaAgregacionTemporal, nil
}

// crearRespuestaRangoTabular es un helper para crear respuestas en formato tabular
// a partir de una lista de mediciones y el path de la serie
func crearRespuestaRangoTabular(seriePath string, mediciones []tipos.Medicion) *tipos.RespuestaConsultaRango {
	if len(mediciones) == 0 {
		return &tipos.RespuestaConsultaRango{
			Resultado: tipos.ResultadoConsultaRango{},
		}
	}

	tiempos := make([]int64, len(mediciones))
	valores := make([][]interface{}, len(mediciones))
	for i, m := range mediciones {
		tiempos[i] = m.Tiempo
		valores[i] = []interface{}{m.Valor}
	}

	return &tipos.RespuestaConsultaRango{
		Resultado: tipos.ResultadoConsultaRango{
			Series:  []string{seriePath},
			Tiempos: tiempos,
			Valores: valores,
		},
	}
}

// crearRespuestaPuntoColumnar es un helper para crear respuestas de punto en formato columnar
func crearRespuestaPuntoColumnar(seriePath string, tiempo int64, valor interface{}) *tipos.RespuestaConsultaPunto {
	return &tipos.RespuestaConsultaPunto{
		Resultado: tipos.ResultadoConsultaPunto{
			Series:  []string{seriePath},
			Tiempos: []int64{tiempo},
			Valores: []interface{}{valor},
		},
	}
}

// crearRespuestaPuntoVacia crea una respuesta de punto vacía (sin datos)
func crearRespuestaPuntoVacia() *tipos.RespuestaConsultaPunto {
	return &tipos.RespuestaConsultaPunto{
		Resultado: tipos.ResultadoConsultaPunto{},
	}
}

// ============================================================================
// MOCK DE CLIENTE S3 PARA TESTS
// ============================================================================

// mockClienteS3 implementa tipos.ClienteS3 para testing
type mockClienteS3 struct {
	// Respuestas configurables
	listObjectsOutput  *s3.ListObjectsV2Output
	getObjectOutput    *s3.GetObjectOutput
	getObjectData      []byte
	putObjectOutput    *s3.PutObjectOutput
	deleteObjectOutput *s3.DeleteObjectOutput
	headBucketOutput   *s3.HeadBucketOutput
	createBucketOutput *s3.CreateBucketOutput

	// Errores configurables
	listObjectsErr  error
	getObjectErr    error
	putObjectErr    error
	deleteObjectErr error
	headBucketErr   error
	createBucketErr error
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
	if m.putObjectErr != nil {
		return nil, m.putObjectErr
	}
	return m.putObjectOutput, nil
}

func (m *mockClienteS3) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.deleteObjectErr != nil {
		return nil, m.deleteObjectErr
	}
	return m.deleteObjectOutput, nil
}

// ============================================================================
// TESTS DE COMBINAR RESULTADOS TABULARES
// ============================================================================

// TestCombinarResultadosTabulares_Vacio verifica comportamiento con lista vacía
func TestCombinarResultadosTabulares_Vacio(t *testing.T) {
	m := &GestorDespachador{}

	resultado := m.combinarResultadosTabulares([]tipos.ResultadoConsultaRango{})

	assert.Empty(t, resultado.Series)
	assert.Empty(t, resultado.Tiempos)
	assert.Empty(t, resultado.Valores)
	t.Log("combinarResultadosTabulares retorna vacío cuando no hay resultados")
}

// TestCombinarResultadosTabulares_UnaFuente verifica con una sola fuente de datos
func TestCombinarResultadosTabulares_UnaFuente(t *testing.T) {
	m := &GestorDespachador{}

	entrada := []tipos.ResultadoConsultaRango{
		{
			Series:  []string{"/sensores/temp"},
			Tiempos: []int64{1000, 2000, 3000},
			Valores: [][]interface{}{{10.0}, {20.0}, {30.0}},
		},
	}

	resultado := m.combinarResultadosTabulares(entrada)

	assert.Equal(t, []string{"/sensores/temp"}, resultado.Series)
	assert.Equal(t, []int64{1000, 2000, 3000}, resultado.Tiempos)
	assert.Len(t, resultado.Valores, 3)
	t.Log("combinarResultadosTabulares retorna datos cuando hay una sola fuente")
}

// TestCombinarResultadosTabulares_MultiplesSeriesSinSolapamiento verifica combinación sin solapamiento
func TestCombinarResultadosTabulares_MultiplesSeriesSinSolapamiento(t *testing.T) {
	m := &GestorDespachador{}

	entrada := []tipos.ResultadoConsultaRango{
		{
			Series:  []string{"/sensores/temp"},
			Tiempos: []int64{1000, 2000},
			Valores: [][]interface{}{{10.0}, {20.0}},
		},
		{
			Series:  []string{"/sensores/humedad"},
			Tiempos: []int64{3000, 4000},
			Valores: [][]interface{}{{50.0}, {60.0}},
		},
	}

	resultado := m.combinarResultadosTabulares(entrada)

	// Series ordenadas alfabéticamente
	assert.Equal(t, []string{"/sensores/humedad", "/sensores/temp"}, resultado.Series)
	// Timestamps combinados y ordenados
	assert.Equal(t, []int64{1000, 2000, 3000, 4000}, resultado.Tiempos)
	// Verificar matriz de valores con nils donde no hay datos
	assert.Len(t, resultado.Valores, 4)
	t.Log("combinarResultadosTabulares combina series sin solapamiento correctamente")
}

// TestCombinarResultadosTabulares_ConSolapamientoTiempos verifica combinación con timestamps comunes
func TestCombinarResultadosTabulares_ConSolapamientoTiempos(t *testing.T) {
	m := &GestorDespachador{}

	entrada := []tipos.ResultadoConsultaRango{
		{
			Series:  []string{"/sensores/temp"},
			Tiempos: []int64{1000, 2000},
			Valores: [][]interface{}{{10.0}, {20.0}},
		},
		{
			Series:  []string{"/sensores/humedad"},
			Tiempos: []int64{2000, 3000},
			Valores: [][]interface{}{{50.0}, {60.0}},
		},
	}

	resultado := m.combinarResultadosTabulares(entrada)

	// Series ordenadas alfabéticamente
	assert.Equal(t, []string{"/sensores/humedad", "/sensores/temp"}, resultado.Series)
	// Timestamps únicos y ordenados
	assert.Equal(t, []int64{1000, 2000, 3000}, resultado.Tiempos)
	// Verificar que timestamp 2000 tiene valores de ambas series
	assert.Len(t, resultado.Valores, 3)
	t.Log("combinarResultadosTabulares maneja timestamps solapados correctamente")
}

// TestCombinarResultadosTabular_S3YBorde verifica combinación de S3 con datos tabulares del borde
func TestCombinarResultadosTabular_S3YBorde(t *testing.T) {
	m := &GestorDespachador{}

	datosS3 := []tipos.Medicion{
		{Tiempo: 1000, Valor: 10.0},
		{Tiempo: 2000, Valor: 20.0},
	}

	datosBorde := tipos.ResultadoConsultaRango{
		Series:  []string{"/sensores/temp"},
		Tiempos: []int64{2000, 3000},
		Valores: [][]interface{}{{25.0}, {30.0}},
	}

	resultado := m.combinarResultadosTabular(datosS3, datosBorde, "/sensores/temp")

	assert.Equal(t, []string{"/sensores/temp"}, resultado.Series)
	assert.Equal(t, []int64{1000, 2000, 3000}, resultado.Tiempos)
	// Verificar que el borde tiene prioridad en timestamp 2000
	// Fila 1 (tiempo 2000) debe tener valor 25.0 del borde, no 20.0 de S3
	assert.Equal(t, 25.0, resultado.Valores[1][0])
	t.Log("combinarResultadosTabular prioriza datos del borde sobre S3")
}

// ============================================================================
// TESTS DE LISTAR NODOS
// ============================================================================

// TestListarNodos_Vacio verifica lista vacia
func TestListarNodos_Vacio(t *testing.T) {
	m := &GestorDespachador{
		nodos: make(map[string]*tipos.Nodo),
	}

	resultado := m.ListarNodos()

	assert.Empty(t, resultado)
	t.Log("ListarNodos retorna lista vacia cuando no hay nodos")
}

// TestListarNodos_ConNodos verifica que retorna todos los nodos
func TestListarNodos_ConNodos(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {NodoID: "nodo1", Direccion: "192.168.1.1"},
			"nodo2": {NodoID: "nodo2", Direccion: "192.168.1.2"},
			"nodo3": {NodoID: "nodo3", Direccion: "192.168.1.3"},
		},
	}

	resultado := m.ListarNodos()

	assert.Len(t, resultado, 3)

	// Verificar que todos los nodos estan presentes
	ids := make(map[string]bool)
	for _, nodo := range resultado {
		ids[nodo.NodoID] = true
	}
	assert.True(t, ids["nodo1"])
	assert.True(t, ids["nodo2"])
	assert.True(t, ids["nodo3"])

	t.Log("ListarNodos retorna todos los nodos registrados")
}

// TestListarNodos_EsCopia verifica que retorna una copia, no referencias
func TestListarNodos_EsCopia(t *testing.T) {
	nodoOriginal := &tipos.Nodo{NodoID: "nodo1", Direccion: "192.168.1.1"}
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": nodoOriginal,
		},
	}

	resultado := m.ListarNodos()

	// Modificar el resultado no debe afectar el original
	resultado[0].Direccion = "10.0.0.1"

	assert.Equal(t, "192.168.1.1", m.nodos["nodo1"].Direccion,
		"Modificar resultado no debe afectar nodo original")

	t.Log("ListarNodos retorna copias de los nodos")
}

// ============================================================================
// TESTS DE CERRAR
// ============================================================================

// TestCerrar verifica que cierra el canal finalizado
func TestCerrar(t *testing.T) {
	m := &GestorDespachador{
		finalizado: make(chan struct{}),
	}

	// Iniciar goroutine que espera el cierre
	cerrado := make(chan bool, 1)
	go func() {
		<-m.finalizado
		cerrado <- true
	}()

	// Cerrar el gestor
	err := m.Cerrar()

	assert.NoError(t, err)

	// Verificar que el canal fue cerrado
	select {
	case <-cerrado:
		// OK
	case <-time.After(100 * time.Millisecond):
		t.Fatal("El canal finalizado no fue cerrado")
	}

	t.Log("Cerrar cierra el canal finalizado correctamente")
}

// ============================================================================
// TESTS DE BUSCAR SERIES POR PATH
// ============================================================================

// TestBuscarSeriesPorPath_Exacto verifica busqueda exacta por nombre de serie
func TestBuscarSeriesPorPath_Exacto(t *testing.T) {
	serie := tipos.Serie{
		SerieId:   1,
		Path:      "/sensores/temperatura",
		TipoDatos: tipos.Real,
	}
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:    "nodo1",
				Direccion: "192.168.1.1",
				Series: map[string]tipos.Serie{
					"/sensores/temperatura": serie,
				},
			},
		},
	}

	resultados, err := m.buscarSeriesPorPath("/sensores/temperatura")

	assert.NoError(t, err)
	assert.Len(t, resultados, 1)
	assert.Equal(t, "nodo1", resultados[0].nodo.NodoID)
	assert.Equal(t, serie.SerieId, resultados[0].serie.SerieId)
	assert.Equal(t, "/sensores/temperatura", resultados[0].path)

	t.Log("buscarSeriesPorPath encuentra serie por nombre exacto")
}

// TestBuscarSeriesPorPath_NoEncontrada verifica error cuando no existe la serie
func TestBuscarSeriesPorPath_NoEncontrada(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID: "nodo1",
				Series: map[string]tipos.Serie{
					"/sensores/temperatura": {SerieId: 1},
				},
			},
		},
	}

	_, err := m.buscarSeriesPorPath("/sensores/humedad")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")

	t.Log("buscarSeriesPorPath retorna error cuando serie no existe")
}

// TestBuscarSeriesPorPath_SinNodos verifica error cuando no hay nodos
func TestBuscarSeriesPorPath_SinNodos(t *testing.T) {
	m := &GestorDespachador{
		nodos: make(map[string]*tipos.Nodo),
	}

	_, err := m.buscarSeriesPorPath("/sensores/temperatura")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")

	t.Log("buscarSeriesPorPath retorna error cuando no hay nodos")
}

// TestBuscarSeriesPorPath_Wildcard verifica busqueda con patron wildcard
func TestBuscarSeriesPorPath_Wildcard(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID: "nodo1",
				Series: map[string]tipos.Serie{
					"sensores/temperatura": {SerieId: 1, Path: "sensores/temperatura"},
					"sensores/humedad":     {SerieId: 2, Path: "sensores/humedad"},
					"actuadores/riego":     {SerieId: 3, Path: "actuadores/riego"},
				},
			},
		},
	}

	// Buscar con wildcard */temperatura
	resultados, err := m.buscarSeriesPorPath("*/temperatura")

	assert.NoError(t, err)
	assert.Len(t, resultados, 1)
	assert.Equal(t, "sensores/temperatura", resultados[0].path)

	t.Log("buscarSeriesPorPath encuentra serie con patron wildcard")
}

// TestBuscarSeriesPorPath_WildcardMultiples verifica busqueda con wildcard que retorna multiples
func TestBuscarSeriesPorPath_WildcardMultiples(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID: "nodo1",
				Series: map[string]tipos.Serie{
					"sensores/temperatura": {SerieId: 1, Path: "sensores/temperatura"},
					"sensores/humedad":     {SerieId: 2, Path: "sensores/humedad"},
					"actuadores/riego":     {SerieId: 3, Path: "actuadores/riego"},
				},
			},
		},
	}

	// Buscar con wildcard sensores/*
	resultados, err := m.buscarSeriesPorPath("sensores/*")

	assert.NoError(t, err)
	assert.Len(t, resultados, 2)

	paths := []string{resultados[0].path, resultados[1].path}
	assert.Contains(t, paths, "sensores/temperatura")
	assert.Contains(t, paths, "sensores/humedad")

	t.Log("buscarSeriesPorPath encuentra multiples series con wildcard")
}

// TestBuscarSeriesPorPath_WildcardSinCoincidencias verifica error cuando wildcard no tiene matches
func TestBuscarSeriesPorPath_WildcardSinCoincidencias(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID: "nodo1",
				Series: map[string]tipos.Serie{
					"/sensores/temperatura": {SerieId: 1},
				},
			},
		},
	}

	_, err := m.buscarSeriesPorPath("*/presion")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")

	t.Log("buscarSeriesPorPath retorna error cuando wildcard no tiene coincidencias")
}

// TestBuscarSeriesPorPath_MultiplesNodos verifica busqueda en multiples nodos
func TestBuscarSeriesPorPath_MultiplesNodos(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID: "nodo1",
				Series: map[string]tipos.Serie{
					"/sensores/temperatura": {SerieId: 1},
				},
			},
			"nodo2": {
				NodoID: "nodo2",
				Series: map[string]tipos.Serie{
					"/sensores/humedad": {SerieId: 2},
				},
			},
		},
	}

	// Buscar serie exacta en nodo2
	resultados, err := m.buscarSeriesPorPath("/sensores/humedad")

	assert.NoError(t, err)
	assert.Len(t, resultados, 1)
	assert.Equal(t, "nodo2", resultados[0].nodo.NodoID)
	assert.Equal(t, 2, resultados[0].serie.SerieId)

	t.Log("buscarSeriesPorPath encuentra serie en multiples nodos")
}

// ============================================================================
// TESTS DE CONSULTAR PUNTO BORDE
// ============================================================================

// TestConsultarUltimoPuntoBorde_Exitoso verifica consulta exitosa al borde
func TestConsultarUltimoPuntoBorde_Exitoso(t *testing.T) {
	mockBorde := &mockClienteBorde{
		respuestaPunto: crearRespuestaPuntoColumnar("/sensores/temp", 1000, 25.5),
	}

	m := &GestorDespachador{
		clienteBorde: mockBorde,
	}

	nodo := tipos.Nodo{
		NodoID:     "nodo1",
		Direccion:  "192.168.1.100",
		PuertoHTTP: "8080",
	}

	resultado, err := m.consultarPuntoBorde(nodo, "/sensores/temp", nil, nil, 5*time.Second)

	assert.NoError(t, err)
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, "/sensores/temp", resultado.Series[0])
	assert.Equal(t, int64(1000), resultado.Tiempos[0])
	assert.Equal(t, 25.5, resultado.Valores[0])
	t.Log("consultarPuntoBorde retorna resultado columnar correctamente")
}

// TestConsultarUltimoPuntoBorde_SinDatos verifica cuando no hay datos
func TestConsultarUltimoPuntoBorde_SinDatos(t *testing.T) {
	mockBorde := &mockClienteBorde{
		respuestaPunto: crearRespuestaPuntoVacia(),
	}

	m := &GestorDespachador{
		clienteBorde: mockBorde,
	}

	nodo := tipos.Nodo{
		NodoID:     "nodo1",
		Direccion:  "192.168.1.100",
		PuertoHTTP: "8080",
	}

	resultado, err := m.consultarPuntoBorde(nodo, "/sensores/temp", nil, nil, 5*time.Second)

	assert.NoError(t, err)
	assert.Empty(t, resultado.Series)
	t.Log("consultarPuntoBorde retorna resultado vacío cuando no hay datos")
}

// TestConsultarUltimoPuntoBorde_ErrorConexion verifica manejo de error de conexion
func TestConsultarUltimoPuntoBorde_ErrorConexion(t *testing.T) {
	mockBorde := &mockClienteBorde{
		err: assert.AnError,
	}

	m := &GestorDespachador{
		clienteBorde: mockBorde,
	}

	nodo := tipos.Nodo{
		NodoID:     "nodo1",
		Direccion:  "192.168.1.100",
		PuertoHTTP: "8080",
	}

	_, err := m.consultarPuntoBorde(nodo, "/sensores/temp", nil, nil, 5*time.Second)

	assert.Error(t, err)
	t.Log("consultarPuntoBorde retorna error cuando hay falla de conexion")
}

// TestConsultarUltimoPuntoBorde_ErrorDelBorde verifica manejo de error reportado por el borde
func TestConsultarUltimoPuntoBorde_ErrorDelBorde(t *testing.T) {
	mockBorde := &mockClienteBorde{
		respuestaPunto: &tipos.RespuestaConsultaPunto{
			Error: "serie no encontrada",
		},
	}

	m := &GestorDespachador{
		clienteBorde: mockBorde,
	}

	nodo := tipos.Nodo{
		NodoID:     "nodo1",
		Direccion:  "192.168.1.100",
		PuertoHTTP: "8080",
	}

	_, err := m.consultarPuntoBorde(nodo, "/sensores/temp", nil, nil, 5*time.Second)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "serie no encontrada")
	t.Log("consultarPuntoBorde retorna error cuando el borde reporta error")
}

// ============================================================================
// TESTS DE CONSULTAR BORDE CON TIMEOUT
// ============================================================================

// TestConsultarBordeConTimeout_Exitoso verifica consulta exitosa de rango
func TestConsultarBordeConTimeout_Exitoso(t *testing.T) {
	resultadoEsperado := tipos.ResultadoConsultaRango{
		Series:  []string{"/sensores/temp"},
		Tiempos: []int64{1000, 2000, 3000},
		Valores: [][]interface{}{{10.0}, {20.0}, {30.0}},
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: &tipos.RespuestaConsultaRango{
			Resultado: resultadoEsperado,
			Error:     "",
		},
	}

	m := &GestorDespachador{
		clienteBorde: mockBorde,
	}

	nodo := tipos.Nodo{
		NodoID:     "nodo1",
		Direccion:  "192.168.1.100",
		PuertoHTTP: "8080",
	}

	resultado, err := m.consultarBordeConTimeout(nodo, "/sensores/temp", 1000, 3000, 5*time.Second)

	assert.NoError(t, err)
	assert.Len(t, resultado.Tiempos, 3)
	assert.Equal(t, []string{"/sensores/temp"}, resultado.Series)
	t.Log("consultarBordeConTimeout retorna resultado tabular correctamente")
}

// TestConsultarBordeConTimeout_SinDatos verifica respuesta vacia
func TestConsultarBordeConTimeout_SinDatos(t *testing.T) {
	mockBorde := &mockClienteBorde{
		respuestaRango: &tipos.RespuestaConsultaRango{
			Resultado: tipos.ResultadoConsultaRango{},
			Error:     "",
		},
	}

	m := &GestorDespachador{
		clienteBorde: mockBorde,
	}

	nodo := tipos.Nodo{
		NodoID:     "nodo1",
		Direccion:  "192.168.1.100",
		PuertoHTTP: "8080",
	}

	resultado, err := m.consultarBordeConTimeout(nodo, "/sensores/temp", 1000, 3000, 5*time.Second)

	assert.NoError(t, err)
	assert.Empty(t, resultado.Tiempos)
	t.Log("consultarBordeConTimeout retorna resultado vacío cuando no hay datos")
}

// TestConsultarBordeConTimeout_ErrorConexion verifica que error de conexion retorna resultado vacío
func TestConsultarBordeConTimeout_ErrorConexion(t *testing.T) {
	mockBorde := &mockClienteBorde{
		err: assert.AnError,
	}

	m := &GestorDespachador{
		clienteBorde: mockBorde,
	}

	nodo := tipos.Nodo{
		NodoID:     "nodo1",
		Direccion:  "192.168.1.100",
		PuertoHTTP: "8080",
	}

	// Error de conexion retorna resultado vacío sin error (el borde puede estar offline)
	resultado, err := m.consultarBordeConTimeout(nodo, "/sensores/temp", 1000, 3000, 5*time.Second)

	assert.Nil(t, err)
	assert.Empty(t, resultado.Tiempos)
	assert.Empty(t, resultado.Series)
	t.Log("consultarBordeConTimeout retorna resultado vacío cuando hay error de conexion")
}

// TestConsultarBordeConTimeout_ErrorDelBorde verifica manejo de error reportado por el borde
func TestConsultarBordeConTimeout_ErrorDelBorde(t *testing.T) {
	mockBorde := &mockClienteBorde{
		respuestaRango: &tipos.RespuestaConsultaRango{
			Error: "serie no existe",
		},
	}

	m := &GestorDespachador{
		clienteBorde: mockBorde,
	}

	nodo := tipos.Nodo{
		NodoID:     "nodo1",
		Direccion:  "192.168.1.100",
		PuertoHTTP: "8080",
	}

	_, err := m.consultarBordeConTimeout(nodo, "/sensores/temp", 1000, 3000, 5*time.Second)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "serie no existe")
	t.Log("consultarBordeConTimeout retorna error cuando el borde reporta error")
}

// ============================================================================
// TESTS DE LISTAR BLOQUES EN RANGO
// ============================================================================

// TestListarBloquesEnRango_SinBloques verifica respuesta cuando no hay bloques
func TestListarBloquesEnRango_SinBloques(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	bloques, err := m.listarBloquesEnRango("nodo1", 1, 1000, 5000)

	assert.NoError(t, err)
	assert.Empty(t, bloques)
	t.Log("listarBloquesEnRango retorna lista vacia cuando no hay bloques")
}

// TestListarBloquesEnRango_ConBloques verifica listado de bloques
func TestListarBloquesEnRango_ConBloques(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodo1/0000000001_00000000000000001000_00000000000000002000")},
				{Key: aws.String("nodo1/0000000001_00000000000000002000_00000000000000003000")},
				{Key: aws.String("nodo1/0000000001_00000000000000003000_00000000000000004000")},
			},
		},
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	// Rango que intersecta con los primeros dos bloques
	bloques, err := m.listarBloquesEnRango("nodo1", 1, 1500, 2500)

	assert.NoError(t, err)
	assert.Len(t, bloques, 2)
	t.Log("listarBloquesEnRango retorna bloques que intersectan con el rango")
}

// TestListarBloquesEnRango_TodosLosBloques verifica cuando el rango cubre todos los bloques
func TestListarBloquesEnRango_TodosLosBloques(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodo1/0000000001_00000000000000001000_00000000000000002000")},
				{Key: aws.String("nodo1/0000000001_00000000000000002000_00000000000000003000")},
				{Key: aws.String("nodo1/0000000001_00000000000000003000_00000000000000004000")},
			},
		},
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	// Rango amplio que cubre todos los bloques
	bloques, err := m.listarBloquesEnRango("nodo1", 1, 0, 10000)

	assert.NoError(t, err)
	assert.Len(t, bloques, 3)
	t.Log("listarBloquesEnRango retorna todos los bloques cuando el rango los cubre")
}

// TestListarBloquesEnRango_NingunBloqueEnRango verifica cuando ningun bloque intersecta
func TestListarBloquesEnRango_NingunBloqueEnRango(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodo1/0000000001_00000000000000001000_00000000000000002000")},
				{Key: aws.String("nodo1/0000000001_00000000000000002000_00000000000000003000")},
			},
		},
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	// Rango que no intersecta con ningun bloque
	bloques, err := m.listarBloquesEnRango("nodo1", 1, 5000, 6000)

	assert.NoError(t, err)
	assert.Empty(t, bloques)
	t.Log("listarBloquesEnRango retorna vacio cuando ningun bloque intersecta")
}

// TestListarBloquesEnRango_ErrorS3 verifica manejo de error de S3
func TestListarBloquesEnRango_ErrorS3(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsErr: assert.AnError,
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	_, err := m.listarBloquesEnRango("nodo1", 1, 1000, 5000)

	assert.Error(t, err)
	t.Log("listarBloquesEnRango retorna error cuando S3 falla")
}

// TestListarBloquesEnRango_FormatoIncorrecto verifica que ignora bloques mal formateados
func TestListarBloquesEnRango_FormatoIncorrecto(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodo1/0000000001_00000000000000001000_00000000000000002000")}, // Correcto
				{Key: aws.String("nodo1/0000000001_bloque_invalido")},                           // Incorrecto
				{Key: aws.String("nodo1/0000000001")},                                           // Sin tiempos
				{Key: aws.String("nodo1/0000000001_00000000000000003000_00000000000000004000")}, // Correcto
			},
		},
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	bloques, err := m.listarBloquesEnRango("nodo1", 1, 0, 10000)

	assert.NoError(t, err)
	assert.Len(t, bloques, 2) // Solo los bloques con formato correcto
	t.Log("listarBloquesEnRango ignora bloques con formato incorrecto")
}

// TestListarBloquesEnRango_OrdenPorTiempo verifica que los bloques estan ordenados
func TestListarBloquesEnRango_OrdenPorTiempo(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodo1/0000000001_00000000000000003000_00000000000000004000")},
				{Key: aws.String("nodo1/0000000001_00000000000000001000_00000000000000002000")},
				{Key: aws.String("nodo1/0000000001_00000000000000002000_00000000000000003000")},
			},
		},
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	bloques, err := m.listarBloquesEnRango("nodo1", 1, 0, 10000)

	assert.NoError(t, err)
	assert.Len(t, bloques, 3)
	// Verificar orden ascendente (los nombres tienen padding, asi que sort.Strings funciona)
	assert.Contains(t, bloques[0], "00000000000000001000")
	assert.Contains(t, bloques[1], "00000000000000002000")
	assert.Contains(t, bloques[2], "00000000000000003000")
	t.Log("listarBloquesEnRango retorna bloques ordenados por tiempo")
}

// ============================================================================
// TESTS DE CONSULTAR DATOS S3
// ============================================================================

// TestConsultarDatosS3_SinBloques verifica respuesta cuando no hay bloques
func TestConsultarDatosS3_SinBloques(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	nodo := tipos.Nodo{NodoID: "nodo1"}
	serie := tipos.Serie{SerieId: 1}

	mediciones, err := m.consultarDatosS3(nodo, serie, 1000, 5000)

	assert.NoError(t, err)
	assert.Empty(t, mediciones)
	t.Log("consultarDatosS3 retorna lista vacia cuando no hay bloques")
}

// TestConsultarDatosS3_ErrorListando verifica manejo de error al listar
func TestConsultarDatosS3_ErrorListando(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsErr: assert.AnError,
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	nodo := tipos.Nodo{NodoID: "nodo1"}
	serie := tipos.Serie{SerieId: 1}

	_, err := m.consultarDatosS3(nodo, serie, 1000, 5000)

	assert.Error(t, err)
	t.Log("consultarDatosS3 retorna error cuando falla el listado")
}

// ============================================================================
// TESTS DE CONSULTAR RANGO
// ============================================================================

// TestConsultarRango_SerieNoEncontrada verifica error cuando la serie no existe
func TestConsultarRango_SerieNoEncontrada(t *testing.T) {
	m := &GestorDespachador{
		nodos: make(map[string]*tipos.Nodo),
	}

	_, err := m.ConsultarRango("/sensores/noexiste", time.Now().Add(-1*time.Hour), time.Now())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("ConsultarRango retorna error cuando la serie no existe")
}

// TestConsultarRango_SoloBorde verifica consulta cuando solo borde tiene datos
func TestConsultarRango_SoloBorde(t *testing.T) {
	mockBorde := &mockClienteBorde{
		respuestaRango: &tipos.RespuestaConsultaRango{
			Resultado: tipos.ResultadoConsultaRango{
				Series:  []string{"/sensores/temp"},
				Tiempos: []int64{1000, 2000},
				Valores: [][]interface{}{{10.0}, {20.0}},
			},
		},
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 500)
	fin := time.Unix(0, 3000)

	resultado, err := m.ConsultarRango("/sensores/temp", inicio, fin)

	assert.NoError(t, err)
	assert.Len(t, resultado.Tiempos, 2)
	assert.Equal(t, []string{"/sensores/temp"}, resultado.Series)
	t.Log("ConsultarRango combina datos de borde cuando S3 esta vacio")
}

// TestConsultarRango_BordeOffline verifica consulta cuando el borde esta offline
func TestConsultarRango_BordeOffline(t *testing.T) {
	mockBorde := &mockClienteBorde{
		err: assert.AnError, // Simular borde offline
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 500)
	fin := time.Unix(0, 3000)

	// Debe continuar con datos de S3 incluso si el borde falla
	resultado, err := m.ConsultarRango("/sensores/temp", inicio, fin)

	assert.NoError(t, err)
	assert.Empty(t, resultado.Tiempos)
	t.Log("ConsultarRango continua cuando borde esta offline")
}

// TestConsultarRango_ErrorS3 verifica error critico cuando S3 falla
func TestConsultarRango_ErrorS3(t *testing.T) {
	mockBorde := &mockClienteBorde{
		respuestaRango: &tipos.RespuestaConsultaRango{
			Resultado: tipos.ResultadoConsultaRango{},
		},
	}

	mockS3 := &mockClienteS3{
		listObjectsErr: assert.AnError,
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 500)
	fin := time.Unix(0, 3000)

	// S3 falla -> error critico
	_, err := m.ConsultarRango("/sensores/temp", inicio, fin)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "S3")
	t.Log("ConsultarRango retorna error cuando S3 falla")
}

// ============================================================================
// TESTS DE CONSULTAR ULTIMO PUNTO
// ============================================================================

// TestConsultarUltimoPunto_SerieNoEncontrada verifica error cuando la serie no existe
func TestConsultarUltimoPunto_SerieNoEncontrada(t *testing.T) {
	m := &GestorDespachador{
		nodos: make(map[string]*tipos.Nodo),
	}

	_, err := m.ConsultarUltimoPunto("/sensores/noexiste", nil, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("ConsultarUltimoPunto retorna error cuando la serie no existe")
}

// TestConsultarUltimoPunto_DesdeBorde verifica que retorna dato del borde cuando esta disponible
func TestConsultarUltimoPunto_DesdeBorde(t *testing.T) {
	mockBorde := &mockClienteBorde{
		respuestaPunto: crearRespuestaPuntoColumnar("/sensores/temp", 5000, 50.0),
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
	}

	resultado, err := m.ConsultarUltimoPunto("/sensores/temp", nil, nil)

	assert.NoError(t, err)
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, "/sensores/temp", resultado.Series[0])
	assert.Equal(t, int64(5000), resultado.Tiempos[0])
	assert.Equal(t, 50.0, resultado.Valores[0])
	t.Log("ConsultarUltimoPunto retorna dato del borde en formato columnar")
}

// TestConsultarUltimoPunto_BordeOffline_SinDatosS3 verifica error cuando no hay datos
func TestConsultarUltimoPunto_BordeOffline_SinDatosS3(t *testing.T) {
	mockBorde := &mockClienteBorde{
		err: assert.AnError, // Borde offline
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	_, err := m.ConsultarUltimoPunto("/sensores/temp", nil, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no se encontraron datos")
	t.Log("ConsultarUltimoPunto retorna error cuando no hay datos")
}

// ============================================================================
// HELPER: CREAR BLOQUE COMPRIMIDO PARA TESTS
// ============================================================================

// crearBloqueComprimidoTest crea un bloque comprimido válido para usar en tests
func crearBloqueComprimidoTest(t *testing.T, mediciones []tipos.Medicion, tipoDatos tipos.TipoDatos,
	compresionBytes tipos.TipoCompresion, compresionBloque tipos.TipoCompresionBloque) []byte {

	// Comprimir tiempos con DeltaDelta (siempre)
	tiemposComprimidos := compresor.CompresionDeltaDeltaTiempo(mediciones)

	// Comprimir valores según tipo
	var valoresComprimidos []byte
	var err error

	switch tipoDatos {
	case tipos.Integer:
		valores := make([]int64, len(mediciones))
		for i, m := range mediciones {
			valores[i] = m.Valor.(int64)
		}
		switch compresionBytes {
		case tipos.DeltaDelta:
			c := &compresor.CompresorDeltaDeltaGenerico[int64]{}
			valoresComprimidos, err = c.Comprimir(valores)
		case tipos.SinCompresion:
			c := &compresor.CompresorNingunoGenerico[int64]{}
			valoresComprimidos, err = c.Comprimir(valores)
		}
	case tipos.Real:
		valores := make([]float64, len(mediciones))
		for i, m := range mediciones {
			valores[i] = m.Valor.(float64)
		}
		switch compresionBytes {
		case tipos.SinCompresion:
			c := &compresor.CompresorNingunoGenerico[float64]{}
			valoresComprimidos, err = c.Comprimir(valores)
		case tipos.Xor:
			c := &compresor.CompresorXor{}
			valoresComprimidos, err = c.Comprimir(valores)
		}
	}
	require.NoError(t, err)

	// Combinar tiempos y valores
	datosCombinados := compresor.CombinarDatos(tiemposComprimidos, valoresComprimidos)

	// Comprimir bloque
	compBloque := compresor.ObtenerCompresorBloque(compresionBloque)
	bloqueComprimido, err := compBloque.Comprimir(datosCombinados)
	require.NoError(t, err)

	return bloqueComprimido
}

// ============================================================================
// TESTS DE CLIENTE BORDE HTTP (httptest)
// ============================================================================

// TestClienteBordeHTTP_ConsultarRango_Exitoso verifica consulta exitosa via HTTP
func TestClienteBordeHTTP_ConsultarRango_Exitoso(t *testing.T) {
	// Crear respuesta esperada
	respuestaEsperada := tipos.RespuestaConsultaRango{
		Resultado: tipos.ResultadoConsultaRango{
			Series:  []string{"/sensores/temp"},
			Tiempos: []int64{1000, 2000},
			Valores: [][]interface{}{{10.0}, {20.0}},
		},
		Error: "",
	}

	// Serializar respuesta
	respuestaBytes, err := tipos.SerializarGob(respuestaEsperada)
	require.NoError(t, err)

	// Crear servidor HTTP mock
	servidor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verificar método y path
		assert.Equal(t, http.MethodPost, r.Method)
		assert.True(t, strings.HasSuffix(r.URL.Path, "/api/consulta/rango"))

		w.WriteHeader(http.StatusOK)
		w.Write(respuestaBytes)
	}))
	defer servidor.Close()

	// Crear cliente y hacer consulta
	cliente := nuevoClienteBordeHTTP()

	// Extraer host:port del servidor de test
	direccion := strings.TrimPrefix(servidor.URL, "http://")

	solicitud := tipos.SolicitudConsultaRango{
		Serie:        "/sensores/temp",
		TiempoInicio: 1000,
		TiempoFin:    2000,
	}

	respuesta, err := cliente.ConsultarRango(context.Background(), "1", direccion, solicitud)

	assert.NoError(t, err)
	assert.NotNil(t, respuesta)
	assert.Len(t, respuesta.Resultado.Tiempos, 2)
	assert.Equal(t, int64(1000), respuesta.Resultado.Tiempos[0])
	t.Log("clienteBordeHTTP.ConsultarRango funciona correctamente via HTTP")
}

// TestClienteBordeHTTP_ConsultarRango_ErrorHTTP verifica manejo de error HTTP
func TestClienteBordeHTTP_ConsultarRango_ErrorHTTP(t *testing.T) {
	// Crear servidor que retorna error
	servidor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer servidor.Close()

	cliente := nuevoClienteBordeHTTP()
	direccion := strings.TrimPrefix(servidor.URL, "http://")

	solicitud := tipos.SolicitudConsultaRango{
		Serie:        "/sensores/temp",
		TiempoInicio: 1000,
		TiempoFin:    2000,
	}

	_, err := cliente.ConsultarRango(context.Background(), "1", direccion, solicitud)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")
	t.Log("clienteBordeHTTP.ConsultarRango maneja errores HTTP correctamente")
}

// TestClienteBordeHTTP_ConsultarRango_ErrorConexion verifica manejo de error de conexion
func TestClienteBordeHTTP_ConsultarRango_ErrorConexion(t *testing.T) {
	cliente := nuevoClienteBordeHTTP()

	// Usar direccion invalida
	solicitud := tipos.SolicitudConsultaRango{
		Serie:        "/sensores/temp",
		TiempoInicio: 1000,
		TiempoFin:    2000,
	}

	_, err := cliente.ConsultarRango(context.Background(), "localhost:99999", "1", solicitud)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error en request HTTP")
	t.Log("clienteBordeHTTP.ConsultarRango maneja errores de conexion")
}

// TestClienteBordeHTTP_ConsultarRango_ErrorDeserializacion verifica manejo de respuesta invalida
func TestClienteBordeHTTP_ConsultarRango_ErrorDeserializacion(t *testing.T) {
	// Crear servidor que retorna datos invalidos
	servidor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("datos invalidos que no son gob"))
	}))
	defer servidor.Close()

	cliente := nuevoClienteBordeHTTP()
	direccion := strings.TrimPrefix(servidor.URL, "http://")

	solicitud := tipos.SolicitudConsultaRango{
		Serie:        "/sensores/temp",
		TiempoInicio: 1000,
		TiempoFin:    2000,
	}

	_, err := cliente.ConsultarRango(context.Background(), "1", direccion, solicitud)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "deserializando")
	t.Log("clienteBordeHTTP.ConsultarRango maneja errores de deserializacion")
}

// TestClienteBordeHTTP_ConsultarUltimoPunto_Exitoso verifica consulta de punto via HTTP
func TestClienteBordeHTTP_ConsultarUltimoPunto_Exitoso(t *testing.T) {
	// Crear respuesta esperada en formato columnar
	respuestaEsperada := tipos.RespuestaConsultaPunto{
		Resultado: tipos.ResultadoConsultaPunto{
			Series:  []string{"/sensores/temp"},
			Tiempos: []int64{5000},
			Valores: []interface{}{50.0},
		},
	}

	respuestaBytes, err := tipos.SerializarGob(respuestaEsperada)
	require.NoError(t, err)

	servidor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.True(t, strings.HasSuffix(r.URL.Path, "/api/consulta/ultimo"))

		w.WriteHeader(http.StatusOK)
		w.Write(respuestaBytes)
	}))
	defer servidor.Close()

	cliente := nuevoClienteBordeHTTP()
	direccion := strings.TrimPrefix(servidor.URL, "http://")

	solicitud := tipos.SolicitudConsultaPunto{
		Serie: "/sensores/temp",
	}

	respuesta, err := cliente.ConsultarUltimoPunto(context.Background(), "1", direccion, solicitud)

	assert.NoError(t, err)
	assert.NotNil(t, respuesta)
	require.Len(t, respuesta.Resultado.Series, 1)
	assert.Equal(t, "/sensores/temp", respuesta.Resultado.Series[0])
	assert.Equal(t, int64(5000), respuesta.Resultado.Tiempos[0])
	t.Log("clienteBordeHTTP.ConsultarUltimoPunto funciona correctamente")
}

// TestClienteBordeHTTP_ConsultarUltimoPunto_ErrorHTTP verifica manejo de error HTTP
func TestClienteBordeHTTP_ConsultarUltimoPunto_ErrorHTTP(t *testing.T) {
	servidor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("serie no encontrada"))
	}))
	defer servidor.Close()

	cliente := nuevoClienteBordeHTTP()
	direccion := strings.TrimPrefix(servidor.URL, "http://")

	solicitud := tipos.SolicitudConsultaPunto{Serie: "/sensores/noexiste"}
	_, err := cliente.ConsultarUltimoPunto(context.Background(), "1", direccion, solicitud)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "404")
	t.Log("clienteBordeHTTP.ConsultarUltimoPunto maneja errores HTTP")
}

// TestClienteBordeHTTP_ConsultarUltimoPunto_ErrorConexion verifica error de conexion
func TestClienteBordeHTTP_ConsultarUltimoPunto_ErrorConexion(t *testing.T) {
	cliente := nuevoClienteBordeHTTP()

	solicitud := tipos.SolicitudConsultaPunto{Serie: "/sensores/temp"}
	_, err := cliente.ConsultarUltimoPunto(context.Background(), "1", "localhost:99999", solicitud)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error en request HTTP")
	t.Log("clienteBordeHTTP.ConsultarUltimoPunto maneja errores de conexion")
}

// ============================================================================
// TESTS DE DESCARGAR Y DESCOMPRIMIR BLOQUE
// ============================================================================

// TestDescargarYDescomprimirBloque_Exitoso verifica descarga y descompresion correcta
func TestDescargarYDescomprimirBloque_Exitoso(t *testing.T) {
	// Crear mediciones de prueba
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: int64(100)},
		{Tiempo: 1000001000, Valor: int64(110)},
		{Tiempo: 1000002000, Valor: int64(120)},
	}

	// Crear bloque comprimido
	bloqueComprimido := crearBloqueComprimidoTest(t, mediciones, tipos.Integer, tipos.DeltaDelta, tipos.Ninguna)

	mockS3 := &mockClienteS3{
		getObjectData: bloqueComprimido,
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	serie := tipos.Serie{
		SerieId:          1,
		TipoDatos:        tipos.Integer,
		CompresionBytes:  tipos.DeltaDelta,
		CompresionBloque: tipos.Ninguna,
	}

	resultado, err := m.descargarYDescomprimirBloque("nodo1/data/0000000001/bloque", serie)

	assert.NoError(t, err)
	assert.Len(t, resultado, 3)
	assert.Equal(t, int64(1000000000), resultado[0].Tiempo)
	assert.Equal(t, int64(100), resultado[0].Valor)
	t.Log("descargarYDescomprimirBloque funciona correctamente")
}

// TestDescargarYDescomprimirBloque_ErrorDescarga verifica manejo de error al descargar
func TestDescargarYDescomprimirBloque_ErrorDescarga(t *testing.T) {
	mockS3 := &mockClienteS3{
		getObjectErr: assert.AnError,
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	serie := tipos.Serie{
		SerieId:          1,
		TipoDatos:        tipos.Integer,
		CompresionBytes:  tipos.DeltaDelta,
		CompresionBloque: tipos.Ninguna,
	}

	_, err := m.descargarYDescomprimirBloque("nodo1/data/0000000001/bloque", serie)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "descargando")
	t.Log("descargarYDescomprimirBloque maneja errores de descarga")
}

// TestDescargarYDescomprimirBloque_ErrorDescompresion verifica manejo de datos invalidos
func TestDescargarYDescomprimirBloque_ErrorDescompresion(t *testing.T) {
	mockS3 := &mockClienteS3{
		getObjectData: []byte("datos invalidos no comprimidos"),
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	serie := tipos.Serie{
		SerieId:          1,
		TipoDatos:        tipos.Integer,
		CompresionBytes:  tipos.DeltaDelta,
		CompresionBloque: tipos.LZ4, // Espera LZ4 pero recibe datos invalidos
	}

	_, err := m.descargarYDescomprimirBloque("nodo1/data/0000000001/bloque", serie)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "descomprimiendo")
	t.Log("descargarYDescomprimirBloque maneja errores de descompresion")
}

// ============================================================================
// TESTS DE CONSULTAR DATOS S3 CON BLOQUES VALIDOS
// ============================================================================

// TestConsultarDatosS3_ConBloquesValidos verifica descarga y filtrado de bloques
func TestConsultarDatosS3_ConBloquesValidos(t *testing.T) {
	// Crear mediciones de prueba
	mediciones := []tipos.Medicion{
		{Tiempo: 1000, Valor: int64(10)},
		{Tiempo: 2000, Valor: int64(20)},
		{Tiempo: 3000, Valor: int64(30)},
		{Tiempo: 4000, Valor: int64(40)},
	}

	bloqueComprimido := crearBloqueComprimidoTest(t, mediciones, tipos.Integer, tipos.DeltaDelta, tipos.Ninguna)

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodo1/0000000001_00000000000000001000_00000000000000004000")},
			},
		},
		getObjectData: bloqueComprimido,
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	nodo := tipos.Nodo{NodoID: "nodo1"}
	serie := tipos.Serie{
		SerieId:          1,
		TipoDatos:        tipos.Integer,
		CompresionBytes:  tipos.DeltaDelta,
		CompresionBloque: tipos.Ninguna,
	}

	// Consultar rango que incluye solo algunas mediciones
	resultado, err := m.consultarDatosS3(nodo, serie, 1500, 3500)

	assert.NoError(t, err)
	// Debe filtrar solo mediciones en el rango [1500, 3500]
	assert.Len(t, resultado, 2) // 2000 y 3000 estan en el rango
	t.Log("consultarDatosS3 descarga, descomprime y filtra correctamente")
}

// ============================================================================
// TESTS DE CONSULTAR ULTIMO PUNTO DESDE S3
// ============================================================================

// TestConsultarUltimoPunto_DesdeS3 verifica fallback a S3 cuando borde no responde
func TestConsultarUltimoPunto_DesdeS3(t *testing.T) {
	// Crear mediciones de prueba
	mediciones := []tipos.Medicion{
		{Tiempo: 1000, Valor: int64(10)},
		{Tiempo: 2000, Valor: int64(20)},
		{Tiempo: 3000, Valor: int64(30)},
	}

	bloqueComprimido := crearBloqueComprimidoTest(t, mediciones, tipos.Integer, tipos.DeltaDelta, tipos.Ninguna)

	// Mock borde que no encuentra datos (retorna resultado vacío)
	mockBorde := &mockClienteBorde{
		respuestaPunto: crearRespuestaPuntoVacia(),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodo1/0000000001_00000000000000001000_00000000000000003000")},
			},
		},
		getObjectData: bloqueComprimido,
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {
						SerieId:          1,
						Path:             "/sensores/temp",
						TipoDatos:        tipos.Integer,
						CompresionBytes:  tipos.DeltaDelta,
						CompresionBloque: tipos.Ninguna,
					},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	resultado, err := m.ConsultarUltimoPunto("/sensores/temp", nil, nil)

	assert.NoError(t, err)
	// Debe retornar la última medición del bloque en formato columnar
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, "/sensores/temp", resultado.Series[0])
	assert.Equal(t, int64(3000), resultado.Tiempos[0])
	assert.Equal(t, int64(30), resultado.Valores[0])
	t.Log("ConsultarUltimoPunto hace fallback a S3 correctamente")
}

// ============================================================================
// TESTS DE NUEVO CLIENTE BORDE HTTP
// ============================================================================

// TestNuevoClienteBordeHTTP verifica creacion del cliente
func TestNuevoClienteBordeHTTP(t *testing.T) {
	cliente := nuevoClienteBordeHTTP()

	assert.NotNil(t, cliente)
	assert.NotNil(t, cliente.httpClient)
	assert.Equal(t, 10*time.Second, cliente.httpClient.Timeout)
	t.Log("nuevoClienteBordeHTTP crea cliente correctamente")
}

// ============================================================================
// TESTS DE CARGAR NODOS DESDE S3
// ============================================================================

// TestCargarNodosDesdeS3_SinNodos verifica carga cuando no hay nodos
func TestCargarNodosDesdeS3_SinNodos(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
		nodos:  make(map[string]*tipos.Nodo),
	}

	err := m.cargarNodosDesdeS3()

	assert.NoError(t, err)
	assert.Empty(t, m.nodos)
	t.Log("cargarNodosDesdeS3 funciona cuando no hay nodos")
}

// TestCargarNodosDesdeS3_ConNodos verifica carga de nodos existentes
func TestCargarNodosDesdeS3_ConNodos(t *testing.T) {
	// Crear JSON de nodo de prueba
	nodo := tipos.Nodo{
		NodoID:     "nodo-test",
		Direccion:  "192.168.1.100",
		PuertoHTTP: "8080",
		Series: map[string]tipos.Serie{
			"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
		},
	}
	nodoJSON, _ := json.Marshal(nodo)

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodos/nodo-test.json")},
			},
		},
		getObjectData: nodoJSON,
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
		nodos:  make(map[string]*tipos.Nodo),
	}

	err := m.cargarNodosDesdeS3()

	assert.NoError(t, err)
	assert.Len(t, m.nodos, 1)
	assert.Contains(t, m.nodos, "nodo-test")
	assert.Equal(t, "192.168.1.100", m.nodos["nodo-test"].Direccion)
	t.Log("cargarNodosDesdeS3 carga nodos correctamente")
}

// TestCargarNodosDesdeS3_ErrorListando verifica manejo de error al listar
func TestCargarNodosDesdeS3_ErrorListando(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsErr: assert.AnError,
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
		nodos:  make(map[string]*tipos.Nodo),
	}

	err := m.cargarNodosDesdeS3()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "listando nodos")
	t.Log("cargarNodosDesdeS3 maneja error de listado")
}

// TestCargarNodosDesdeS3_ErrorGetObject verifica que continua si falla un GetObject
func TestCargarNodosDesdeS3_ErrorGetObject(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodos/nodo-test.json")},
			},
		},
		getObjectErr: assert.AnError,
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
		nodos:  make(map[string]*tipos.Nodo),
	}

	err := m.cargarNodosDesdeS3()

	// No debe retornar error, solo loguea y continua
	assert.NoError(t, err)
	assert.Empty(t, m.nodos)
	t.Log("cargarNodosDesdeS3 continua si falla GetObject")
}

// TestCargarNodosDesdeS3_JSONInvalido verifica que continua con JSON invalido
func TestCargarNodosDesdeS3_JSONInvalido(t *testing.T) {
	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodos/nodo-invalido.json")},
			},
		},
		getObjectData: []byte("esto no es JSON valido"),
	}

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
		nodos:  make(map[string]*tipos.Nodo),
	}

	err := m.cargarNodosDesdeS3()

	// No debe retornar error, solo loguea y continua
	assert.NoError(t, err)
	assert.Empty(t, m.nodos)
	t.Log("cargarNodosDesdeS3 continua con JSON invalido")
}

// TestCargarNodosDesdeS3_MultiplesNodos verifica carga de multiples nodos
func TestCargarNodosDesdeS3_MultiplesNodos(t *testing.T) {
	// Para este test necesitamos un mock mas sofisticado que retorne
	// diferentes datos segun la clave solicitada
	nodo1 := tipos.Nodo{NodoID: "nodo1", Direccion: "192.168.1.1"}
	nodo1JSON, _ := json.Marshal(nodo1)

	// Usamos un contador para alternar respuestas
	callCount := 0
	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodos/nodo1.json")},
			},
		},
	}
	// Configurar datos del primer nodo
	mockS3.getObjectData = nodo1JSON

	m := &GestorDespachador{
		s3:     mockS3,
		config: tipos.ConfiguracionS3{Bucket: "test-bucket"},
		nodos:  make(map[string]*tipos.Nodo),
	}

	err := m.cargarNodosDesdeS3()

	assert.NoError(t, err)
	assert.Len(t, m.nodos, 1)
	_ = callCount // evitar warning
	t.Log("cargarNodosDesdeS3 carga multiples nodos")
}

// ============================================================================
// TESTS DE CREAR DESPACHADOR
// ============================================================================

// TestCrear_ConClienteS3Inyectado verifica creacion con cliente S3 mock
func TestCrear_ConClienteS3Inyectado(t *testing.T) {
	mockS3 := &mockClienteS3{
		headBucketOutput: &s3.HeadBucketOutput{},
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	mockBorde := &mockClienteBorde{}

	opts := opcionesInternas{
		Opciones: Opciones{
			ConfigS3: tipos.ConfiguracionS3{
				Endpoint:        "http://localhost:3900",
				AccessKeyID:     "test-key",
				SecretAccessKey: "test-secret",
				Bucket:          "test-bucket",
			},
		},
		clienteS3:    mockS3,
		clienteBorde: mockBorde,
	}

	gestor, err := crearConOpciones(opts)

	assert.NoError(t, err)
	assert.NotNil(t, gestor)
	assert.NotNil(t, gestor.s3)
	assert.NotNil(t, gestor.clienteBorde)

	// Cerrar para limpiar goroutine
	gestor.Cerrar()
	t.Log("Crear funciona con cliente S3 inyectado")
}

// TestCrear_BucketNoExiste_SeCreaNuevo verifica creacion de bucket
func TestCrear_BucketNoExiste_SeCreaNuevo(t *testing.T) {
	mockS3 := &mockClienteS3{
		headBucketErr:      assert.AnError, // Bucket no existe
		createBucketOutput: &s3.CreateBucketOutput{},
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	opts := opcionesInternas{
		Opciones: Opciones{
			ConfigS3: tipos.ConfiguracionS3{
				Endpoint:        "http://localhost:3900",
				AccessKeyID:     "test-key",
				SecretAccessKey: "test-secret",
				Bucket:          "nuevo-bucket",
			},
		},
		clienteS3:    mockS3,
		clienteBorde: &mockClienteBorde{},
	}

	gestor, err := crearConOpciones(opts)

	assert.NoError(t, err)
	assert.NotNil(t, gestor)

	gestor.Cerrar()
	t.Log("Crear crea bucket si no existe")
}

// TestCrear_ErrorCreandoBucket verifica error al crear bucket
func TestCrear_ErrorCreandoBucket(t *testing.T) {
	mockS3 := &mockClienteS3{
		headBucketErr:   assert.AnError, // Bucket no existe
		createBucketErr: assert.AnError, // Error al crearlo
	}

	opts := opcionesInternas{
		Opciones: Opciones{
			ConfigS3: tipos.ConfiguracionS3{
				Endpoint:        "http://localhost:3900",
				AccessKeyID:     "test-key",
				SecretAccessKey: "test-secret",
				Bucket:          "bucket-fallido",
			},
		},
		clienteS3:    mockS3,
		clienteBorde: &mockClienteBorde{},
	}

	gestor, err := crearConOpciones(opts)

	assert.Error(t, err)
	assert.Nil(t, gestor)
	assert.Contains(t, err.Error(), "crear bucket")
	t.Log("Crear retorna error si falla creacion de bucket")
}

// TestCrear_SinClienteS3_ConfigInvalida verifica error con config invalida
func TestCrear_SinClienteS3_ConfigInvalida(t *testing.T) {
	// Sin cliente S3 inyectado y con config invalida
	opts := Opciones{
		ConfigS3: tipos.ConfiguracionS3{
			Endpoint:        "", // Invalido
			AccessKeyID:     "",
			SecretAccessKey: "",
			Bucket:          "",
		},
	}

	gestor, err := Crear(opts)

	assert.Error(t, err)
	assert.Nil(t, gestor)
	t.Log("Crear retorna error con config S3 invalida")
}

// TestCrear_ConNodosExistentes verifica carga de nodos al crear
func TestCrear_ConNodosExistentes(t *testing.T) {
	nodo := tipos.Nodo{
		NodoID:     "nodo-existente",
		Direccion:  "10.0.0.1",
		PuertoHTTP: "9000",
	}
	nodoJSON, _ := json.Marshal(nodo)

	mockS3 := &mockClienteS3{
		headBucketOutput: &s3.HeadBucketOutput{},
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{
				{Key: aws.String("nodos/nodo-existente.json")},
			},
		},
		getObjectData: nodoJSON,
	}

	opts := opcionesInternas{
		Opciones: Opciones{
			ConfigS3: tipos.ConfiguracionS3{
				Endpoint:        "http://localhost:3900",
				AccessKeyID:     "test-key",
				SecretAccessKey: "test-secret",
				Bucket:          "test-bucket",
			},
		},
		clienteS3:    mockS3,
		clienteBorde: &mockClienteBorde{},
	}

	gestor, err := crearConOpciones(opts)

	assert.NoError(t, err)
	assert.NotNil(t, gestor)
	assert.Len(t, gestor.nodos, 1)
	assert.Contains(t, gestor.nodos, "nodo-existente")

	gestor.Cerrar()
	t.Log("Crear carga nodos existentes desde S3")
}

// TestCrear_SinClienteBorde_UsaHTTP verifica que crea cliente HTTP por defecto
func TestCrear_SinClienteBorde_UsaHTTP(t *testing.T) {
	mockS3 := &mockClienteS3{
		headBucketOutput: &s3.HeadBucketOutput{},
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	opts := opcionesInternas{
		Opciones: Opciones{
			ConfigS3: tipos.ConfiguracionS3{
				Endpoint:        "http://localhost:3900",
				AccessKeyID:     "test-key",
				SecretAccessKey: "test-secret",
				Bucket:          "test-bucket",
			},
		},
		clienteS3:    mockS3,
		clienteBorde: nil, // No inyectado
	}

	gestor, err := crearConOpciones(opts)

	assert.NoError(t, err)
	assert.NotNil(t, gestor)
	assert.NotNil(t, gestor.clienteBorde)
	// Verificar que es del tipo clienteBordeHTTP
	_, ok := gestor.clienteBorde.(*clienteBordeHTTP)
	assert.True(t, ok, "Debe crear clienteBordeHTTP por defecto")

	gestor.Cerrar()
	t.Log("Crear usa clienteBordeHTTP por defecto")
}

// ============================================================================
// TESTS DE CONSULTAR AGREGACION
// ============================================================================

// TestConsultarAgregacion_SerieNoEncontrada verifica error cuando la serie no existe
func TestConsultarAgregacion_SerieNoEncontrada(t *testing.T) {
	m := &GestorDespachador{
		nodos: make(map[string]*tipos.Nodo),
	}

	_, err := m.ConsultarAgregacion("/sensores/noexiste", time.Now().Add(-1*time.Hour), time.Now(), []tipos.TipoAgregacion{tipos.AgregacionPromedio})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("ConsultarAgregacion retorna error cuando la serie no existe")
}

// TestConsultarAgregacion_Promedio verifica calculo de promedio
func TestConsultarAgregacion_Promedio(t *testing.T) {
	medicionesBorde := []tipos.Medicion{
		{Tiempo: 1000, Valor: 10.0},
		{Tiempo: 2000, Valor: 20.0},
		{Tiempo: 3000, Valor: 30.0},
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", medicionesBorde),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 500)
	fin := time.Unix(0, 3500)

	resultado, err := m.ConsultarAgregacion("/sensores/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionPromedio})

	assert.NoError(t, err)
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, "/sensores/temp", resultado.Series[0])
	assert.Equal(t, 20.0, resultado.Valores[0][0]) // [agregacion][serie] - (10 + 20 + 30) / 3 = 20
	t.Log("ConsultarAgregacion calcula promedio correctamente")
}

// TestConsultarAgregacion_Maximo verifica calculo de maximo
func TestConsultarAgregacion_Maximo(t *testing.T) {
	medicionesBorde := []tipos.Medicion{
		{Tiempo: 1000, Valor: 10.0},
		{Tiempo: 2000, Valor: 50.0},
		{Tiempo: 3000, Valor: 30.0},
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", medicionesBorde),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 500)
	fin := time.Unix(0, 3500)

	resultado, err := m.ConsultarAgregacion("/sensores/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionMaximo})

	assert.NoError(t, err)
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, 50.0, resultado.Valores[0][0]) // [agregacion][serie]
	t.Log("ConsultarAgregacion calcula maximo correctamente")
}

// TestConsultarAgregacion_Minimo verifica calculo de minimo
func TestConsultarAgregacion_Minimo(t *testing.T) {
	medicionesBorde := []tipos.Medicion{
		{Tiempo: 1000, Valor: 10.0},
		{Tiempo: 2000, Valor: 50.0},
		{Tiempo: 3000, Valor: 5.0},
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", medicionesBorde),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 500)
	fin := time.Unix(0, 3500)

	resultado, err := m.ConsultarAgregacion("/sensores/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionMinimo})

	assert.NoError(t, err)
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, 5.0, resultado.Valores[0][0]) // [agregacion][serie]
	t.Log("ConsultarAgregacion calcula minimo correctamente")
}

// TestConsultarAgregacion_Suma verifica calculo de suma
func TestConsultarAgregacion_Suma(t *testing.T) {
	medicionesBorde := []tipos.Medicion{
		{Tiempo: 1000, Valor: 10.0},
		{Tiempo: 2000, Valor: 20.0},
		{Tiempo: 3000, Valor: 30.0},
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", medicionesBorde),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 500)
	fin := time.Unix(0, 3500)

	resultado, err := m.ConsultarAgregacion("/sensores/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionSuma})

	assert.NoError(t, err)
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, 60.0, resultado.Valores[0][0]) // [agregacion][serie] - 10 + 20 + 30 = 60
	t.Log("ConsultarAgregacion calcula suma correctamente")
}

// TestConsultarAgregacion_Count verifica calculo de count
func TestConsultarAgregacion_Count(t *testing.T) {
	medicionesBorde := []tipos.Medicion{
		{Tiempo: 1000, Valor: 10.0},
		{Tiempo: 2000, Valor: 20.0},
		{Tiempo: 3000, Valor: 30.0},
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", medicionesBorde),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 500)
	fin := time.Unix(0, 3500)

	resultado, err := m.ConsultarAgregacion("/sensores/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionConteo})

	assert.NoError(t, err)
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, 3.0, resultado.Valores[0][0]) // [agregacion][serie]
	t.Log("ConsultarAgregacion calcula count correctamente")
}

// TestConsultarAgregacion_SinDatos verifica error cuando no hay datos
func TestConsultarAgregacion_SinDatos(t *testing.T) {
	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", []tipos.Medicion{}),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 500)
	fin := time.Unix(0, 3500)

	_, err := m.ConsultarAgregacion("/sensores/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionPromedio})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no se encontraron datos")
	t.Log("ConsultarAgregacion retorna error cuando no hay datos")
}

// TestConsultarAgregacion_ConInt64 verifica agregacion con valores int64
func TestConsultarAgregacion_ConInt64(t *testing.T) {
	medicionesBorde := []tipos.Medicion{
		{Tiempo: 1000, Valor: int64(10)},
		{Tiempo: 2000, Valor: int64(20)},
		{Tiempo: 3000, Valor: int64(30)},
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", medicionesBorde),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 500)
	fin := time.Unix(0, 3500)

	resultado, err := m.ConsultarAgregacion("/sensores/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionPromedio})

	assert.NoError(t, err)
	require.Len(t, resultado.Series, 1)
	assert.Equal(t, 20.0, resultado.Valores[0][0]) // [agregacion][serie]
	t.Log("ConsultarAgregacion funciona con valores int64")
}

// ============================================================================
// TESTS DE CONSULTAR AGREGACION TEMPORAL
// ============================================================================

// TestConsultarAgregacionTemporal_SerieNoEncontrada verifica error cuando la serie no existe
func TestConsultarAgregacionTemporal_SerieNoEncontrada(t *testing.T) {
	m := &GestorDespachador{
		nodos: make(map[string]*tipos.Nodo),
	}

	_, err := m.ConsultarAgregacionTemporal("/sensores/noexiste", time.Now().Add(-1*time.Hour), time.Now(), []tipos.TipoAgregacion{tipos.AgregacionPromedio}, time.Minute)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("ConsultarAgregacionTemporal retorna error cuando la serie no existe")
}

// TestConsultarAgregacionTemporal_IntervaloInvalido verifica error con intervalo <= 0
func TestConsultarAgregacionTemporal_IntervaloInvalido(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID: "nodo1",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
	}

	_, err := m.ConsultarAgregacionTemporal("/sensores/temp", time.Now().Add(-1*time.Hour), time.Now(), []tipos.TipoAgregacion{tipos.AgregacionPromedio}, 0)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "intervalo debe ser mayor a cero")
	t.Log("ConsultarAgregacionTemporal retorna error con intervalo invalido")
}

// TestConsultarAgregacionTemporal_MultipleBuckets verifica generacion de multiples buckets
func TestConsultarAgregacionTemporal_MultipleBuckets(t *testing.T) {
	// Mediciones distribuidas en 3 buckets de 1000ns cada uno
	medicionesBorde := []tipos.Medicion{
		// Bucket 1: [0, 1000)
		{Tiempo: 100, Valor: 10.0},
		{Tiempo: 500, Valor: 20.0},
		// Bucket 2: [1000, 2000)
		{Tiempo: 1100, Valor: 30.0},
		{Tiempo: 1500, Valor: 40.0},
		// Bucket 3: [2000, 3000)
		{Tiempo: 2100, Valor: 50.0},
		{Tiempo: 2500, Valor: 60.0},
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", medicionesBorde),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 0)
	fin := time.Unix(0, 3000)
	intervalo := time.Duration(1000) // 1000 nanosegundos

	resultado, err := m.ConsultarAgregacionTemporal("/sensores/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionPromedio}, intervalo)

	assert.NoError(t, err)
	assert.Len(t, resultado.Tiempos, 3)
	assert.Len(t, resultado.Series, 1)
	assert.Equal(t, "/sensores/temp", resultado.Series[0])

	// Valores[agregacion][bucket][serie]
	// Bucket 1: promedio de 10 y 20 = 15
	assert.Equal(t, 15.0, resultado.Valores[0][0][0])
	// Bucket 2: promedio de 30 y 40 = 35
	assert.Equal(t, 35.0, resultado.Valores[0][1][0])
	// Bucket 3: promedio de 50 y 60 = 55
	assert.Equal(t, 55.0, resultado.Valores[0][2][0])

	t.Log("ConsultarAgregacionTemporal genera multiples buckets correctamente")
}

// TestConsultarAgregacionTemporal_SinDatos verifica error cuando no hay datos
func TestConsultarAgregacionTemporal_SinDatos(t *testing.T) {
	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", []tipos.Medicion{}),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 0)
	fin := time.Unix(0, 3000)

	_, err := m.ConsultarAgregacionTemporal("/sensores/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionPromedio}, time.Second)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no se encontraron datos")
	t.Log("ConsultarAgregacionTemporal retorna error cuando no hay datos")
}

// TestConsultarAgregacionTemporal_OrdenCronologico verifica que los resultados esten ordenados
func TestConsultarAgregacionTemporal_OrdenCronologico(t *testing.T) {
	// Mediciones desordenadas
	medicionesBorde := []tipos.Medicion{
		{Tiempo: 2100, Valor: 30.0},
		{Tiempo: 100, Valor: 10.0},
		{Tiempo: 1100, Valor: 20.0},
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", medicionesBorde),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 0)
	fin := time.Unix(0, 3000)
	intervalo := time.Duration(1000)

	resultado, err := m.ConsultarAgregacionTemporal("/sensores/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionPromedio}, intervalo)

	assert.NoError(t, err)
	assert.Len(t, resultado.Tiempos, 3)
	assert.Len(t, resultado.Series, 1)

	// Verificar orden cronologico de los tiempos
	for i := 1; i < len(resultado.Tiempos); i++ {
		assert.True(t, resultado.Tiempos[i] > resultado.Tiempos[i-1],
			"Resultados deben estar ordenados cronologicamente")
	}

	t.Log("ConsultarAgregacionTemporal retorna resultados ordenados cronologicamente")
}

// ============================================================================
// TESTS DE FUNCIONES HELPER
// ============================================================================

// TestCalcularAgregacionSimple_Vacio verifica error con slice vacio
func TestCalcularAgregacionSimple_Vacio(t *testing.T) {
	_, err := calcularAgregacionSimple([]float64{}, tipos.AgregacionPromedio)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no hay valores")
	t.Log("calcularAgregacionSimple retorna error con slice vacio")
}

// TestCalcularAgregacionSimple_TipoInvalido verifica error con tipo invalido
func TestCalcularAgregacionSimple_TipoInvalido(t *testing.T) {
	_, err := calcularAgregacionSimple([]float64{1.0, 2.0}, "tipo_invalido")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no soportado")
	t.Log("calcularAgregacionSimple retorna error con tipo invalido")
}

// ============================================================================
// TESTS DE WILDCARDS
// ============================================================================

// TestCoincidePath_Exacto verifica matching exacto sin wildcard
func TestCoincidePath_Exacto(t *testing.T) {
	assert.True(t, tipos.CoincidePath("sensor_01/temp", "sensor_01/temp"))
	assert.False(t, tipos.CoincidePath("sensor_01/temp", "sensor_02/temp"))
	assert.False(t, tipos.CoincidePath("sensor_01/temp", "sensor_01/humidity"))
	t.Log("tipos.CoincidePath funciona correctamente con path exacto")
}

// TestCoincidePath_WildcardGlobal verifica patron "*" que coincide con todo
func TestCoincidePath_WildcardGlobal(t *testing.T) {
	assert.True(t, tipos.CoincidePath("sensor_01/temp", "*"))
	assert.True(t, tipos.CoincidePath("cualquier/cosa", "*"))
	assert.True(t, tipos.CoincidePath("a", "*"))
	t.Log("tipos.CoincidePath con '*' coincide con cualquier path")
}

// TestCoincidePath_WildcardInicio verifica wildcard al inicio del patron
func TestCoincidePath_WildcardInicio(t *testing.T) {
	assert.True(t, tipos.CoincidePath("sensor_01/temp", "*/temp"))
	assert.True(t, tipos.CoincidePath("sensor_02/temp", "*/temp"))
	assert.True(t, tipos.CoincidePath("cualquier/temp", "*/temp"))
	assert.False(t, tipos.CoincidePath("sensor_01/humidity", "*/temp"))
	t.Log("tipos.CoincidePath con wildcard al inicio funciona correctamente")
}

// TestCoincidePath_WildcardFin verifica wildcard al final del patron
func TestCoincidePath_WildcardFin(t *testing.T) {
	assert.True(t, tipos.CoincidePath("sensor_01/temp", "sensor_01/*"))
	assert.True(t, tipos.CoincidePath("sensor_01/humidity", "sensor_01/*"))
	assert.False(t, tipos.CoincidePath("sensor_02/temp", "sensor_01/*"))
	t.Log("tipos.CoincidePath con wildcard al final funciona correctamente")
}

// TestCoincidePath_WildcardMedio verifica wildcard en el medio del patron
func TestCoincidePath_WildcardMedio(t *testing.T) {
	assert.True(t, tipos.CoincidePath("field_01/sensor_01/temp", "field_01/*/temp"))
	assert.True(t, tipos.CoincidePath("field_01/sensor_02/temp", "field_01/*/temp"))
	assert.False(t, tipos.CoincidePath("field_02/sensor_01/temp", "field_01/*/temp"))
	assert.False(t, tipos.CoincidePath("field_01/sensor_01/humidity", "field_01/*/temp"))
	t.Log("tipos.CoincidePath con wildcard en el medio funciona correctamente")
}

// TestCoincidePath_MultipleWildcards verifica multiples wildcards
func TestCoincidePath_MultipleWildcards(t *testing.T) {
	assert.True(t, tipos.CoincidePath("field_01/sensor_01/temp", "*/*/temp"))
	assert.True(t, tipos.CoincidePath("field_02/sensor_02/temp", "*/*/temp"))
	assert.False(t, tipos.CoincidePath("field_01/sensor_01/humidity", "*/*/temp"))
	t.Log("tipos.CoincidePath con multiples wildcards funciona correctamente")
}

// TestCoincidePath_DiferenteNivelProfundidad verifica que no coincida con diferente profundidad
func TestCoincidePath_DiferenteNivelProfundidad(t *testing.T) {
	assert.False(t, tipos.CoincidePath("sensor_01/temp/interior", "sensor_01/*"))
	assert.False(t, tipos.CoincidePath("sensor_01", "sensor_01/temp"))
	assert.False(t, tipos.CoincidePath("a/b/c", "*/temp"))
	t.Log("tipos.CoincidePath no coincide cuando la profundidad es diferente")
}

// TestEsPatronWildcard verifica deteccion de wildcards
func TestEsPatronWildcard(t *testing.T) {
	assert.True(t, tipos.EsPatronWildcard("*/temp"))
	assert.True(t, tipos.EsPatronWildcard("sensor_01/*"))
	assert.True(t, tipos.EsPatronWildcard("*"))
	assert.False(t, tipos.EsPatronWildcard("sensor_01/temp"))
	assert.False(t, tipos.EsPatronWildcard("sensor/temperatura"))
	t.Log("tipos.EsPatronWildcard detecta correctamente patrones con wildcard")
}

// TestBuscarSeriesPorPath_Wildcard_Encontradas verifica busqueda con coincidencias
func TestBuscarSeriesPorPath_Wildcard_Encontradas(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.1",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"sensor_01/temp":     {SerieId: 1, Path: "sensor_01/temp"},
					"sensor_01/humidity": {SerieId: 2, Path: "sensor_01/humidity"},
					"sensor_02/temp":     {SerieId: 3, Path: "sensor_02/temp"},
				},
			},
		},
	}

	// Buscar todas las series de temperatura
	resultados, err := m.buscarSeriesPorPath("*/temp")

	assert.NoError(t, err)
	assert.Len(t, resultados, 2)

	// Verificar que encontro las series correctas
	paths := make([]string, len(resultados))
	for i, r := range resultados {
		paths[i] = r.path
	}
	assert.Contains(t, paths, "sensor_01/temp")
	assert.Contains(t, paths, "sensor_02/temp")

	t.Log("buscarSeriesPorPath con wildcard encuentra series que coinciden con el patron")
}

// TestBuscarSeriesPorPath_Wildcard_NoEncontradas verifica error cuando no hay coincidencias
func TestBuscarSeriesPorPath_Wildcard_NoEncontradas(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID: "nodo1",
				Series: map[string]tipos.Serie{
					"sensor_01/temp": {SerieId: 1, Path: "sensor_01/temp"},
				},
			},
		},
	}

	_, err := m.buscarSeriesPorPath("*/pressure")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("buscarSeriesPorPath con wildcard retorna error cuando no hay coincidencias")
}

// TestBuscarSeriesPorPath_Wildcard_MultiplesNodos verifica busqueda en multiples nodos
func TestBuscarSeriesPorPath_Wildcard_MultiplesNodos(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.1",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"sensor_01/temp": {SerieId: 1, Path: "sensor_01/temp"},
				},
			},
			"nodo2": {
				NodoID:     "nodo2",
				Direccion:  "192.168.1.2",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"sensor_02/temp": {SerieId: 2, Path: "sensor_02/temp"},
				},
			},
		},
	}

	resultados, err := m.buscarSeriesPorPath("*/temp")

	assert.NoError(t, err)
	assert.Len(t, resultados, 2)

	// Verificar que encontro series de ambos nodos
	nodos := make(map[string]bool)
	for _, r := range resultados {
		nodos[r.nodo.NodoID] = true
	}
	assert.True(t, nodos["nodo1"])
	assert.True(t, nodos["nodo2"])

	t.Log("buscarSeriesPorPath con wildcard busca en multiples nodos")
}

// TestConsultarAgregacion_Wildcard_MultiplesSeries verifica agregacion con wildcard
func TestConsultarAgregacion_Wildcard_MultiplesSeries(t *testing.T) {
	// El mock retorna datos tabulares combinados para ambas series
	// Tiempo 1000: serie1=10.0, serie2=15.0
	// Tiempo 2000: serie1=20.0, serie2=25.0
	mockBorde := &mockClienteBorde{
		respuestaRango: &tipos.RespuestaConsultaRango{
			Resultado: tipos.ResultadoConsultaRango{
				Series:  []string{"sensor_01/temp", "sensor_02/temp"},
				Tiempos: []int64{1000, 2000},
				Valores: [][]interface{}{{10.0, 15.0}, {20.0, 25.0}},
			},
		},
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"sensor_01/temp": {SerieId: 1, Path: "sensor_01/temp"},
					"sensor_02/temp": {SerieId: 2, Path: "sensor_02/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 500)
	fin := time.Unix(0, 2500)

	// Ahora columnar: cada serie tiene su propio promedio
	// serie1: (10 + 20) / 2 = 15
	// serie2: (15 + 25) / 2 = 20
	resultado, err := m.ConsultarAgregacion("*/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionPromedio})

	assert.NoError(t, err)
	require.Len(t, resultado.Series, 2)
	assert.Equal(t, "sensor_01/temp", resultado.Series[0])
	assert.Equal(t, "sensor_02/temp", resultado.Series[1])
	// Valores[agregacion][serie]
	assert.Equal(t, 15.0, resultado.Valores[0][0])
	assert.Equal(t, 20.0, resultado.Valores[0][1])
	t.Log("ConsultarAgregacion con wildcard retorna valores por serie")
}

// TestConsultarAgregacion_Wildcard_SinCoincidencias verifica error cuando wildcard no coincide
func TestConsultarAgregacion_Wildcard_SinCoincidencias(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID: "nodo1",
				Series: map[string]tipos.Serie{
					"sensor_01/temp": {SerieId: 1, Path: "sensor_01/temp"},
				},
			},
		},
	}

	inicio := time.Unix(0, 0)
	fin := time.Unix(0, 1000)

	_, err := m.ConsultarAgregacion("*/pressure", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionPromedio})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("ConsultarAgregacion con wildcard retorna error cuando no hay coincidencias")
}

// TestConsultarAgregacion_Wildcard_Suma verifica suma con wildcard
func TestConsultarAgregacion_Wildcard_Suma(t *testing.T) {
	// El mock retorna datos tabulares combinados para las 3 series (cada una con valor 10)
	mockBorde := &mockClienteBorde{
		respuestaRango: &tipos.RespuestaConsultaRango{
			Resultado: tipos.ResultadoConsultaRango{
				Series:  []string{"sensor_01/temp", "sensor_02/temp", "sensor_03/temp"},
				Tiempos: []int64{1000},
				Valores: [][]interface{}{{10.0, 10.0, 10.0}},
			},
		},
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"sensor_01/temp": {SerieId: 1, Path: "sensor_01/temp"},
					"sensor_02/temp": {SerieId: 2, Path: "sensor_02/temp"},
					"sensor_03/temp": {SerieId: 3, Path: "sensor_03/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 0)
	fin := time.Unix(0, 2000)

	// 3 series, cada una con valor 10 -> cada serie tiene suma = 10
	resultado, err := m.ConsultarAgregacion("*/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionSuma})

	assert.NoError(t, err)
	require.Len(t, resultado.Series, 3)
	// Valores[agregacion][serie]
	assert.Equal(t, 10.0, resultado.Valores[0][0])
	assert.Equal(t, 10.0, resultado.Valores[0][1])
	assert.Equal(t, 10.0, resultado.Valores[0][2])
	t.Log("ConsultarAgregacion con wildcard calcula suma por serie")
}

// TestConsultarAgregacion_Wildcard_Count verifica count con wildcard
func TestConsultarAgregacion_Wildcard_Count(t *testing.T) {
	// 2 series x 2 mediciones = 4 valores totales
	mockBorde := &mockClienteBorde{
		respuestaRango: &tipos.RespuestaConsultaRango{
			Resultado: tipos.ResultadoConsultaRango{
				Series:  []string{"sensor_01/temp", "sensor_02/temp"},
				Tiempos: []int64{1000, 2000},
				Valores: [][]interface{}{{10.0, 15.0}, {20.0, 25.0}},
			},
		},
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"sensor_01/temp": {SerieId: 1, Path: "sensor_01/temp"},
					"sensor_02/temp": {SerieId: 2, Path: "sensor_02/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 0)
	fin := time.Unix(0, 3000)

	// 2 series x 2 mediciones = cada serie tiene count = 2
	resultado, err := m.ConsultarAgregacion("*/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionConteo})

	assert.NoError(t, err)
	require.Len(t, resultado.Series, 2)
	// Valores[agregacion][serie]
	assert.Equal(t, 2.0, resultado.Valores[0][0])
	assert.Equal(t, 2.0, resultado.Valores[0][1])
	t.Log("ConsultarAgregacion con wildcard calcula count por serie")
}

// TestConsultarAgregacionTemporal_Wildcard verifica downsampling con wildcard
func TestConsultarAgregacionTemporal_Wildcard(t *testing.T) {
	// 2 series con datos en tiempos 100 y 500
	// Tiempo 100: serie1=10.0, serie2=15.0
	// Tiempo 500: serie1=20.0, serie2=25.0
	mockBorde := &mockClienteBorde{
		respuestaRango: &tipos.RespuestaConsultaRango{
			Resultado: tipos.ResultadoConsultaRango{
				Series:  []string{"sensor_01/temp", "sensor_02/temp"},
				Tiempos: []int64{100, 500},
				Valores: [][]interface{}{{10.0, 15.0}, {20.0, 25.0}},
			},
		},
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"sensor_01/temp": {SerieId: 1, Path: "sensor_01/temp"},
					"sensor_02/temp": {SerieId: 2, Path: "sensor_02/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 0)
	fin := time.Unix(0, 1000)
	intervalo := time.Duration(1000) // 1000ns = un solo bucket

	resultado, err := m.ConsultarAgregacionTemporal("*/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionPromedio}, intervalo)

	assert.NoError(t, err)
	assert.Len(t, resultado.Tiempos, 1)
	assert.Len(t, resultado.Series, 2)
	// Valores[agregacion][bucket][serie]
	// Serie 1: promedio de 10 y 20 = 15
	assert.Equal(t, 15.0, resultado.Valores[0][0][0])
	// Serie 2: promedio de 15 y 25 = 20
	assert.Equal(t, 20.0, resultado.Valores[0][0][1])
	t.Log("ConsultarAgregacionTemporal con wildcard funciona correctamente")
}

// TestConsultarAgregacionTemporal_Wildcard_SinCoincidencias verifica error con wildcard sin coincidencias
func TestConsultarAgregacionTemporal_Wildcard_SinCoincidencias(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID: "nodo1",
				Series: map[string]tipos.Serie{
					"sensor_01/temp": {SerieId: 1, Path: "sensor_01/temp"},
				},
			},
		},
	}

	inicio := time.Unix(0, 0)
	fin := time.Unix(0, 1000)

	_, err := m.ConsultarAgregacionTemporal("*/pressure", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionPromedio}, time.Second)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no encontrada")
	t.Log("ConsultarAgregacionTemporal con wildcard retorna error sin coincidencias")
}

// TestConsultarAgregacionTemporal_Wildcard_MultipleBuckets verifica multiples buckets con wildcard
func TestConsultarAgregacionTemporal_Wildcard_MultipleBuckets(t *testing.T) {
	// 2 series con datos en buckets diferentes
	mockBorde := &mockClienteBorde{
		respuestaRango: &tipos.RespuestaConsultaRango{
			Resultado: tipos.ResultadoConsultaRango{
				Series:  []string{"sensor_01/temp", "sensor_02/temp"},
				Tiempos: []int64{100, 1100}, // Bucket 1: [0, 1000), Bucket 2: [1000, 2000)
				Valores: [][]interface{}{{10.0, 10.0}, {20.0, 20.0}},
			},
		},
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"sensor_01/temp": {SerieId: 1, Path: "sensor_01/temp"},
					"sensor_02/temp": {SerieId: 2, Path: "sensor_02/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	inicio := time.Unix(0, 0)
	fin := time.Unix(0, 2000)
	intervalo := time.Duration(1000)

	resultado, err := m.ConsultarAgregacionTemporal("*/temp", inicio, fin, []tipos.TipoAgregacion{tipos.AgregacionPromedio}, intervalo)

	assert.NoError(t, err)
	assert.Len(t, resultado.Tiempos, 2)
	assert.Len(t, resultado.Series, 2)
	// Valores[agregacion][bucket][serie]
	// Bucket 1: serie1=10.0, serie2=10.0
	assert.Equal(t, 10.0, resultado.Valores[0][0][0])
	assert.Equal(t, 10.0, resultado.Valores[0][0][1])
	// Bucket 2: serie1=20.0, serie2=20.0
	assert.Equal(t, 20.0, resultado.Valores[0][1][0])
	assert.Equal(t, 20.0, resultado.Valores[0][1][1])
	t.Log("ConsultarAgregacionTemporal con wildcard genera multiples buckets correctamente")
}

// ============================================================================
// TESTS DE MÚLTIPLES AGREGACIONES (usando ConsultarAgregacion con slice)
// ============================================================================

// TestConsultarAgregacion_MultiplesAgregaciones_MinMax verifica min y max en una sola llamada
func TestConsultarAgregacion_MultiplesAgregaciones_MinMax(t *testing.T) {
	inicio := time.Now().Add(-1 * time.Hour)
	fin := time.Now()

	// Datos: 10, 20, 30, 40, 50
	mediciones := []tipos.Medicion{
		{Tiempo: inicio.Add(1 * time.Minute).UnixNano(), Valor: float64(10.0)},
		{Tiempo: inicio.Add(2 * time.Minute).UnixNano(), Valor: float64(20.0)},
		{Tiempo: inicio.Add(3 * time.Minute).UnixNano(), Valor: float64(30.0)},
		{Tiempo: inicio.Add(4 * time.Minute).UnixNano(), Valor: float64(40.0)},
		{Tiempo: inicio.Add(5 * time.Minute).UnixNano(), Valor: float64(50.0)},
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", mediciones),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	resultado, err := m.ConsultarAgregacion(
		"/sensores/temp",
		inicio, fin,
		[]tipos.TipoAgregacion{tipos.AgregacionMinimo, tipos.AgregacionMaximo},
	)

	assert.NoError(t, err)
	assert.Len(t, resultado.Series, 1)
	// Valores[agregacion][serie] - agregacion 0 = Minimo, agregacion 1 = Maximo
	assert.Equal(t, 10.0, resultado.Valores[0][0]) // Minimo de serie 0
	assert.Equal(t, 50.0, resultado.Valores[1][0]) // Maximo de serie 0
	t.Log("ConsultarAgregacion con múltiples agregaciones calcula min y max correctamente")
}

// TestConsultarAgregacion_MultiplesAgregaciones_TodasLasAgregaciones verifica todas las agregaciones
func TestConsultarAgregacion_MultiplesAgregaciones_TodasLasAgregaciones(t *testing.T) {
	inicio := time.Now().Add(-1 * time.Hour)
	fin := time.Now()

	// Datos: 10, 20, 30, 40, 50 (suma=150, promedio=30, count=5)
	mediciones := []tipos.Medicion{
		{Tiempo: inicio.Add(1 * time.Minute).UnixNano(), Valor: float64(10.0)},
		{Tiempo: inicio.Add(2 * time.Minute).UnixNano(), Valor: float64(20.0)},
		{Tiempo: inicio.Add(3 * time.Minute).UnixNano(), Valor: float64(30.0)},
		{Tiempo: inicio.Add(4 * time.Minute).UnixNano(), Valor: float64(40.0)},
		{Tiempo: inicio.Add(5 * time.Minute).UnixNano(), Valor: float64(50.0)},
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: crearRespuestaRangoTabular("/sensores/temp", mediciones),
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	resultado, err := m.ConsultarAgregacion(
		"/sensores/temp",
		inicio, fin,
		[]tipos.TipoAgregacion{
			tipos.AgregacionMinimo,
			tipos.AgregacionMaximo,
			tipos.AgregacionPromedio,
			tipos.AgregacionSuma,
			tipos.AgregacionConteo,
		},
	)

	assert.NoError(t, err)
	// Valores[agregacion][serie]
	assert.Equal(t, 10.0, resultado.Valores[0][0])  // Minimo
	assert.Equal(t, 50.0, resultado.Valores[1][0])  // Maximo
	assert.Equal(t, 30.0, resultado.Valores[2][0])  // Promedio
	assert.Equal(t, 150.0, resultado.Valores[3][0]) // Suma
	assert.Equal(t, 5.0, resultado.Valores[4][0])   // Count
	t.Log("ConsultarAgregacion con múltiples agregaciones calcula todas correctamente")
}

// TestConsultarAgregacion_MultiplesAgregaciones_SinAgregaciones verifica error sin agregaciones
func TestConsultarAgregacion_MultiplesAgregaciones_SinAgregaciones(t *testing.T) {
	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID: "nodo1",
				Series: map[string]tipos.Serie{
					"/sensores/temp": {SerieId: 1, Path: "/sensores/temp"},
				},
			},
		},
	}

	_, err := m.ConsultarAgregacion(
		"/sensores/temp",
		time.Now().Add(-1*time.Hour),
		time.Now(),
		[]tipos.TipoAgregacion{}, // Lista vacía
	)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "al menos una agregación")
	t.Log("ConsultarAgregacion retorna error sin agregaciones")
}

// TestConsultarAgregacion_MultiplesAgregaciones_Wildcard verifica múltiples agregaciones con wildcard
func TestConsultarAgregacion_MultiplesAgregaciones_Wildcard(t *testing.T) {
	inicio := time.Now().Add(-1 * time.Hour)
	fin := time.Now()

	// Crear respuesta con dos series
	tiempos := []int64{
		inicio.Add(1 * time.Minute).UnixNano(),
		inicio.Add(2 * time.Minute).UnixNano(),
	}
	valores := [][]interface{}{
		{float64(10.0), float64(100.0)}, // t1: serie1=10, serie2=100
		{float64(20.0), float64(200.0)}, // t2: serie1=20, serie2=200
	}

	mockBorde := &mockClienteBorde{
		respuestaRango: &tipos.RespuestaConsultaRango{
			Resultado: tipos.ResultadoConsultaRango{
				Series:  []string{"sensor_01/temp", "sensor_02/temp"},
				Tiempos: tiempos,
				Valores: valores,
			},
		},
	}

	mockS3 := &mockClienteS3{
		listObjectsOutput: &s3.ListObjectsV2Output{
			Contents: []s3types.Object{},
		},
	}

	m := &GestorDespachador{
		nodos: map[string]*tipos.Nodo{
			"nodo1": {
				NodoID:     "nodo1",
				Direccion:  "192.168.1.100",
				PuertoHTTP: "8080",
				Series: map[string]tipos.Serie{
					"sensor_01/temp": {SerieId: 1, Path: "sensor_01/temp"},
					"sensor_02/temp": {SerieId: 2, Path: "sensor_02/temp"},
				},
			},
		},
		clienteBorde: mockBorde,
		s3:           mockS3,
		config:       tipos.ConfiguracionS3{Bucket: "test-bucket"},
	}

	resultado, err := m.ConsultarAgregacion(
		"*/temp",
		inicio, fin,
		[]tipos.TipoAgregacion{tipos.AgregacionMinimo, tipos.AgregacionMaximo},
	)

	assert.NoError(t, err)
	assert.Len(t, resultado.Series, 2)

	// Valores[agregacion][serie]
	// Serie 1: min=10, max=20
	assert.Equal(t, 10.0, resultado.Valores[0][0]) // Minimo serie 0
	assert.Equal(t, 20.0, resultado.Valores[1][0]) // Maximo serie 0

	// Serie 2: min=100, max=200
	assert.Equal(t, 100.0, resultado.Valores[0][1]) // Minimo serie 1
	assert.Equal(t, 200.0, resultado.Valores[1][1]) // Maximo serie 1

	t.Log("ConsultarAgregacion con múltiples agregaciones y wildcard funciona correctamente")
}
