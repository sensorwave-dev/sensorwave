package tipos

import (
	"math"
	"testing"
	"time"
)

// ==================== Tests de SerializarGob ====================

// TestSerializarDeserializarGob_Medicion verifica serialización de Medicion
func TestSerializarDeserializarGob_Medicion(t *testing.T) {
	original := Medicion{
		Tiempo: time.Now().UnixNano(),
		Valor:  float64(25.5),
	}

	// Serializar
	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar Medicion: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("Datos serializados vacíos")
	}

	// Deserializar
	var deserializada Medicion
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar Medicion: %v", err)
	}

	// Verificar
	if deserializada.Tiempo != original.Tiempo {
		t.Errorf("Tiempo incorrecto: esperado %d, obtenido %d", original.Tiempo, deserializada.Tiempo)
	}

	valorOriginal := original.Valor.(float64)
	valorDeserializado := deserializada.Valor.(float64)
	if valorDeserializado != valorOriginal {
		t.Errorf("Valor incorrecto: esperado %f, obtenido %f", valorOriginal, valorDeserializado)
	}

	t.Logf("✓ Medicion serializada/deserializada correctamente: tiempo=%d, valor=%v", deserializada.Tiempo, deserializada.Valor)
}

// TestSerializarDeserializarGob_MedicionTodosTipos verifica serialización con diferentes tipos de valor
func TestSerializarDeserializarGob_MedicionTodosTipos(t *testing.T) {
	casosDePrueba := []struct {
		nombre string
		valor  interface{}
	}{
		{"Boolean true", true},
		{"Boolean false", false},
		{"Integer", int64(12345)},
		{"Real", float64(3.14159)},
		{"Text", "sensor_value"},
	}

	for _, tc := range casosDePrueba {
		t.Run(tc.nombre, func(t *testing.T) {
			original := Medicion{
				Tiempo: time.Now().UnixNano(),
				Valor:  tc.valor,
			}

			data, err := SerializarGob(original)
			if err != nil {
				t.Fatalf("Error al serializar: %v", err)
			}

			var deserializada Medicion
			err = DeserializarGob(data, &deserializada)
			if err != nil {
				t.Fatalf("Error al deserializar: %v", err)
			}

			if deserializada.Valor != tc.valor {
				t.Errorf("Valor incorrecto: esperado %v (%T), obtenido %v (%T)",
					tc.valor, tc.valor, deserializada.Valor, deserializada.Valor)
			}

			t.Logf("✓ Tipo %s serializado correctamente", tc.nombre)
		})
	}
}

// TestSerializarDeserializarGob_Serie verifica serialización de Serie
func TestSerializarDeserializarGob_Serie(t *testing.T) {
	original := Serie{
		SerieId:              123,
		Path:                 "dispositivo/temperatura",
		TipoDatos:            Real,
		CompresionBloque:     LZ4,
		CompresionBytes:      DeltaDelta,
		TamañoBloque:         1000,
		TiempoAlmacenamiento: 7 * 24 * time.Hour.Nanoseconds(),
		Tags: map[string]string{
			"ubicacion": "sala1",
			"tipo":      "DHT22",
		},
	}

	// Serializar
	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar Serie: %v", err)
	}

	// Deserializar
	var deserializada Serie
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar Serie: %v", err)
	}

	// Verificar campos
	if deserializada.SerieId != original.SerieId {
		t.Errorf("SerieId incorrecto: %d != %d", deserializada.SerieId, original.SerieId)
	}
	if deserializada.Path != original.Path {
		t.Errorf("Path incorrecto: %s != %s", deserializada.Path, original.Path)
	}
	if deserializada.TipoDatos != original.TipoDatos {
		t.Errorf("TipoDatos incorrecto: %v != %v", deserializada.TipoDatos, original.TipoDatos)
	}
	if deserializada.TamañoBloque != original.TamañoBloque {
		t.Errorf("TamañoBloque incorrecto: %d != %d", deserializada.TamañoBloque, original.TamañoBloque)
	}
	if deserializada.Tags["ubicacion"] != "sala1" {
		t.Errorf("Tag 'ubicacion' incorrecto: %s", deserializada.Tags["ubicacion"])
	}

	t.Log("✓ Serie serializada/deserializada correctamente")
}

// TestSerializarDeserializarGob_SolicitudConsultaRango verifica serialización de solicitud
func TestSerializarDeserializarGob_SolicitudConsultaRango(t *testing.T) {
	original := SolicitudConsultaRango{
		Serie:        "sensor/temperatura",
		TiempoInicio: time.Now().Add(-1 * time.Hour).UnixNano(),
		TiempoFin:    time.Now().UnixNano(),
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar SolicitudConsultaRango: %v", err)
	}

	var deserializada SolicitudConsultaRango
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar SolicitudConsultaRango: %v", err)
	}

	if deserializada.Serie != original.Serie {
		t.Errorf("Serie incorrecta: %s != %s", deserializada.Serie, original.Serie)
	}
	if deserializada.TiempoInicio != original.TiempoInicio {
		t.Errorf("TiempoInicio incorrecto")
	}
	if deserializada.TiempoFin != original.TiempoFin {
		t.Errorf("TiempoFin incorrecto")
	}

	t.Log("✓ SolicitudConsultaRango serializada/deserializada correctamente")
}

// TestSerializarDeserializarGob_SolicitudConsultaPunto verifica serialización de consulta punto
func TestSerializarDeserializarGob_SolicitudConsultaPunto(t *testing.T) {
	original := SolicitudConsultaPunto{
		Serie: "sensor/humedad",
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar SolicitudConsultaPunto: %v", err)
	}

	var deserializada SolicitudConsultaPunto
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar SolicitudConsultaPunto: %v", err)
	}

	if deserializada.Serie != original.Serie {
		t.Errorf("Serie incorrecta")
	}

	t.Log("✓ SolicitudConsultaPunto serializada/deserializada correctamente")
}

// TestSerializarDeserializarGob_SolicitudConsultaPuntoConTiempos verifica serialización con tiempos opcionales
func TestSerializarDeserializarGob_SolicitudConsultaPuntoConTiempos(t *testing.T) {
	tiempoInicio := time.Now().Add(-1 * time.Hour).UnixNano()
	tiempoFin := time.Now().UnixNano()

	original := SolicitudConsultaPunto{
		Serie:        "sensor/temperatura",
		TiempoInicio: &tiempoInicio,
		TiempoFin:    &tiempoFin,
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar SolicitudConsultaPunto con tiempos: %v", err)
	}

	var deserializada SolicitudConsultaPunto
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar SolicitudConsultaPunto con tiempos: %v", err)
	}

	if deserializada.Serie != original.Serie {
		t.Errorf("Serie incorrecta: esperada %s, obtenida %s", original.Serie, deserializada.Serie)
	}

	if deserializada.TiempoInicio == nil {
		t.Error("TiempoInicio es nil, se esperaba un valor")
	} else if *deserializada.TiempoInicio != tiempoInicio {
		t.Errorf("TiempoInicio incorrecto: esperado %d, obtenido %d", tiempoInicio, *deserializada.TiempoInicio)
	}

	if deserializada.TiempoFin == nil {
		t.Error("TiempoFin es nil, se esperaba un valor")
	} else if *deserializada.TiempoFin != tiempoFin {
		t.Errorf("TiempoFin incorrecto: esperado %d, obtenido %d", tiempoFin, *deserializada.TiempoFin)
	}

	t.Log("✓ SolicitudConsultaPunto con tiempos opcionales serializada/deserializada correctamente")
}

// TestSerializarDeserializarGob_SolicitudConsultaPuntoTiemposNil verifica que tiempos nil se preservan
func TestSerializarDeserializarGob_SolicitudConsultaPuntoTiemposNil(t *testing.T) {
	original := SolicitudConsultaPunto{
		Serie:        "sensor/humedad",
		TiempoInicio: nil, // Explícitamente nil
		TiempoFin:    nil, // Explícitamente nil
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar SolicitudConsultaPunto con tiempos nil: %v", err)
	}

	var deserializada SolicitudConsultaPunto
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar SolicitudConsultaPunto con tiempos nil: %v", err)
	}

	if deserializada.Serie != original.Serie {
		t.Errorf("Serie incorrecta")
	}

	if deserializada.TiempoInicio != nil {
		t.Errorf("TiempoInicio debería ser nil, obtenido %d", *deserializada.TiempoInicio)
	}

	if deserializada.TiempoFin != nil {
		t.Errorf("TiempoFin debería ser nil, obtenido %d", *deserializada.TiempoFin)
	}

	t.Log("✓ SolicitudConsultaPunto con tiempos nil preserva valores nil correctamente")
}

// TestSerializarDeserializarGob_ResultadoConsultaPunto verifica serialización de ResultadoConsultaPunto
func TestSerializarDeserializarGob_ResultadoConsultaPunto(t *testing.T) {
	ahora := time.Now().UnixNano()
	original := ResultadoConsultaPunto{
		Series:  []string{"sensor1/temp", "sensor2/temp", "sensor3/temp"},
		Tiempos: []int64{ahora, ahora - 1000, ahora - 2000},
		Valores: []interface{}{25.5, 26.0, 24.8},
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar ResultadoConsultaPunto: %v", err)
	}

	var deserializado ResultadoConsultaPunto
	err = DeserializarGob(data, &deserializado)
	if err != nil {
		t.Fatalf("Error al deserializar ResultadoConsultaPunto: %v", err)
	}

	// Verificar series
	if len(deserializado.Series) != len(original.Series) {
		t.Errorf("Cantidad de series incorrecta: esperadas %d, obtenidas %d",
			len(original.Series), len(deserializado.Series))
	}
	for i, serie := range deserializado.Series {
		if serie != original.Series[i] {
			t.Errorf("Serie %d incorrecta: esperada %s, obtenida %s", i, original.Series[i], serie)
		}
	}

	// Verificar tiempos
	if len(deserializado.Tiempos) != len(original.Tiempos) {
		t.Errorf("Cantidad de tiempos incorrecta: esperados %d, obtenidos %d",
			len(original.Tiempos), len(deserializado.Tiempos))
	}
	for i, tiempo := range deserializado.Tiempos {
		if tiempo != original.Tiempos[i] {
			t.Errorf("Tiempo %d incorrecto: esperado %d, obtenido %d", i, original.Tiempos[i], tiempo)
		}
	}

	// Verificar valores
	if len(deserializado.Valores) != len(original.Valores) {
		t.Errorf("Cantidad de valores incorrecta: esperados %d, obtenidos %d",
			len(original.Valores), len(deserializado.Valores))
	}

	t.Logf("✓ ResultadoConsultaPunto con %d series serializado correctamente", len(deserializado.Series))
}

// TestSerializarDeserializarGob_RespuestaConsultaPunto verifica serialización de respuesta punto
func TestSerializarDeserializarGob_RespuestaConsultaPunto(t *testing.T) {
	ahora := time.Now().UnixNano()
	original := RespuestaConsultaPunto{
		Resultado: ResultadoConsultaPunto{
			Series:  []string{"sensor/temp"},
			Tiempos: []int64{ahora},
			Valores: []interface{}{25.5},
		},
		Error: "",
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar RespuestaConsultaPunto: %v", err)
	}

	var deserializada RespuestaConsultaPunto
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar RespuestaConsultaPunto: %v", err)
	}

	if len(deserializada.Resultado.Series) != 1 {
		t.Errorf("Cantidad de series incorrecta: esperada 1, obtenida %d", len(deserializada.Resultado.Series))
	}

	if deserializada.Resultado.Series[0] != "sensor/temp" {
		t.Errorf("Serie incorrecta: esperada sensor/temp, obtenida %s", deserializada.Resultado.Series[0])
	}

	t.Log("✓ RespuestaConsultaPunto serializada/deserializada correctamente")
}

// TestSerializarDeserializarGob_RespuestaConsultaPuntoConError verifica respuesta con error
func TestSerializarDeserializarGob_RespuestaConsultaPuntoConError(t *testing.T) {
	original := RespuestaConsultaPunto{
		Resultado: ResultadoConsultaPunto{},
		Error:     "no hay datos para la serie",
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar RespuestaConsultaPunto con error: %v", err)
	}

	var deserializada RespuestaConsultaPunto
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar RespuestaConsultaPunto con error: %v", err)
	}

	if deserializada.Error != original.Error {
		t.Errorf("Error incorrecto: esperado '%s', obtenido '%s'", original.Error, deserializada.Error)
	}

	if len(deserializada.Resultado.Series) != 0 {
		t.Errorf("Series deberían estar vacías, obtenidas %d", len(deserializada.Resultado.Series))
	}

	t.Log("✓ RespuestaConsultaPunto con error serializada correctamente")
}

// TestSerializarDeserializarGob_RespuestaConsultaRango verifica respuesta con resultado tabular
func TestSerializarDeserializarGob_RespuestaConsultaRango(t *testing.T) {
	ahora := time.Now().UnixNano()
	original := RespuestaConsultaRango{
		Resultado: ResultadoConsultaRango{
			Series:  []string{"sensor/temp"},
			Tiempos: []int64{ahora, ahora + int64(time.Second), ahora + int64(2*time.Second)},
			Valores: [][]interface{}{{25.0}, {26.0}, {27.0}},
		},
		Error: "",
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar RespuestaConsultaRango: %v", err)
	}

	var deserializada RespuestaConsultaRango
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar RespuestaConsultaRango: %v", err)
	}

	if len(deserializada.Resultado.Tiempos) != 3 {
		t.Errorf("Cantidad de tiempos incorrecta: esperadas 3, obtenidas %d", len(deserializada.Resultado.Tiempos))
	}

	if len(deserializada.Resultado.Series) != 1 {
		t.Errorf("Cantidad de series incorrecta: esperada 1, obtenida %d", len(deserializada.Resultado.Series))
	}

	t.Logf("✓ RespuestaConsultaRango con %d tiempos serializada correctamente", len(deserializada.Resultado.Tiempos))
}

// TestDeserializarGob_DatosInvalidos verifica error con datos corruptos
func TestDeserializarGob_DatosInvalidos(t *testing.T) {
	datosInvalidos := []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE}

	var medicion Medicion
	err := DeserializarGob(datosInvalidos, &medicion)
	if err == nil {
		t.Error("Se esperaba error al deserializar datos inválidos")
	} else {
		t.Logf("✓ Error esperado con datos inválidos: %v", err)
	}
}

// TestDeserializarGob_DatosVacios verifica error con datos vacíos
func TestDeserializarGob_DatosVacios(t *testing.T) {
	datosVacios := []byte{}

	var medicion Medicion
	err := DeserializarGob(datosVacios, &medicion)
	if err == nil {
		t.Error("Se esperaba error al deserializar datos vacíos")
	} else {
		t.Logf("✓ Error esperado con datos vacíos: %v", err)
	}
}

// TestSerializarGob_SliceMediciones verifica serialización de slice
func TestSerializarGob_SliceMediciones(t *testing.T) {
	original := []Medicion{
		{Tiempo: 1000000000, Valor: int64(100)},
		{Tiempo: 2000000000, Valor: int64(200)},
		{Tiempo: 3000000000, Valor: int64(300)},
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar slice: %v", err)
	}

	var deserializado []Medicion
	err = DeserializarGob(data, &deserializado)
	if err != nil {
		t.Fatalf("Error al deserializar slice: %v", err)
	}

	if len(deserializado) != len(original) {
		t.Errorf("Longitud incorrecta: %d != %d", len(deserializado), len(original))
	}

	t.Logf("✓ Slice de %d mediciones serializado correctamente", len(deserializado))
}

// TestSerializarGob_ErrorTipoNoSerializable verifica error con tipo no serializable
func TestSerializarGob_ErrorTipoNoSerializable(t *testing.T) {
	// Los canales no se pueden serializar con gob
	ch := make(chan int)

	_, err := SerializarGob(ch)
	if err == nil {
		t.Error("Se esperaba error al serializar un channel")
	} else {
		t.Logf("✓ Error esperado con channel: %v", err)
	}
}

// TestSerializarGob_ErrorFuncion verifica error con función
func TestSerializarGob_ErrorFuncion(t *testing.T) {
	// Las funciones no se pueden serializar con gob
	fn := func() {}

	_, err := SerializarGob(fn)
	if err == nil {
		t.Error("Se esperaba error al serializar una función")
	} else {
		t.Logf("✓ Error esperado con función: %v", err)
	}
}

// TestSerializarDeserializarGob_ResultadoAgregacion verifica serialización de ResultadoAgregacion
func TestSerializarDeserializarGob_ResultadoAgregacion(t *testing.T) {
	original := ResultadoAgregacion{
		Series:       []string{"sensor1/temp", "sensor2/temp", "sensor3/temp"},
		Agregaciones: []TipoAgregacion{AgregacionPromedio},
		Valores:      [][]float64{{25.5, 26.0, 24.8}}, // [agregacion][serie]
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar ResultadoAgregacion: %v", err)
	}

	var deserializado ResultadoAgregacion
	err = DeserializarGob(data, &deserializado)
	if err != nil {
		t.Fatalf("Error al deserializar ResultadoAgregacion: %v", err)
	}

	if len(deserializado.Series) != len(original.Series) {
		t.Errorf("Cantidad de series incorrecta: esperadas %d, obtenidas %d",
			len(original.Series), len(deserializado.Series))
	}

	if len(deserializado.Agregaciones) != len(original.Agregaciones) {
		t.Errorf("Cantidad de agregaciones incorrecta: esperadas %d, obtenidas %d",
			len(original.Agregaciones), len(deserializado.Agregaciones))
	}

	if len(deserializado.Valores) != len(original.Valores) {
		t.Errorf("Cantidad de filas de valores incorrecta: esperadas %d, obtenidas %d",
			len(original.Valores), len(deserializado.Valores))
	}

	for i, serie := range deserializado.Series {
		if serie != original.Series[i] {
			t.Errorf("Serie %d incorrecta: esperada %s, obtenida %s", i, original.Series[i], serie)
		}
	}

	// Verificar valores [agregacion][serie]
	for i, agregacion := range deserializado.Valores {
		for j, valor := range agregacion {
			if valor != original.Valores[i][j] {
				t.Errorf("Valor [%d][%d] incorrecto: esperado %f, obtenido %f", i, j, original.Valores[i][j], valor)
			}
		}
	}

	t.Logf("✓ ResultadoAgregacion con %d series y %d agregaciones serializado correctamente",
		len(deserializado.Series), len(deserializado.Agregaciones))
}

// TestSerializarDeserializarGob_RespuestaConsultaAgregacion verifica serialización de respuesta agregación
func TestSerializarDeserializarGob_RespuestaConsultaAgregacion(t *testing.T) {
	original := RespuestaConsultaAgregacion{
		Resultado: ResultadoAgregacion{
			Series:       []string{"sensor/temp"},
			Agregaciones: []TipoAgregacion{AgregacionPromedio},
			Valores:      [][]float64{{25.5}}, // [agregacion][serie]
		},
		Error: "",
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar RespuestaConsultaAgregacion: %v", err)
	}

	var deserializada RespuestaConsultaAgregacion
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar RespuestaConsultaAgregacion: %v", err)
	}

	if len(deserializada.Resultado.Series) != 1 {
		t.Errorf("Cantidad de series incorrecta: esperada 1, obtenida %d", len(deserializada.Resultado.Series))
	}

	if deserializada.Resultado.Series[0] != "sensor/temp" {
		t.Errorf("Serie incorrecta: esperada sensor/temp, obtenida %s", deserializada.Resultado.Series[0])
	}

	if deserializada.Resultado.Valores[0][0] != 25.5 {
		t.Errorf("Valor incorrecto: esperado 25.5, obtenido %f", deserializada.Resultado.Valores[0][0])
	}

	t.Log("✓ RespuestaConsultaAgregacion serializada/deserializada correctamente")
}

// TestSerializarDeserializarGob_RespuestaConsultaAgregacionConError verifica respuesta con error
func TestSerializarDeserializarGob_RespuestaConsultaAgregacionConError(t *testing.T) {
	original := RespuestaConsultaAgregacion{
		Resultado: ResultadoAgregacion{},
		Error:     "no hay datos en el rango especificado",
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar RespuestaConsultaAgregacion con error: %v", err)
	}

	var deserializada RespuestaConsultaAgregacion
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar RespuestaConsultaAgregacion con error: %v", err)
	}

	if deserializada.Error != original.Error {
		t.Errorf("Error incorrecto: esperado '%s', obtenido '%s'", original.Error, deserializada.Error)
	}

	if len(deserializada.Resultado.Series) != 0 {
		t.Errorf("Series deberían estar vacías, obtenidas %d", len(deserializada.Resultado.Series))
	}

	t.Log("✓ RespuestaConsultaAgregacion con error serializada correctamente")
}

// TestSerializarDeserializarGob_ResultadoAgregacionTemporal verifica serialización de ResultadoAgregacionTemporal matricial
func TestSerializarDeserializarGob_ResultadoAgregacionTemporal(t *testing.T) {
	ahora := time.Now().UnixNano()
	intervalo := int64(time.Minute)

	original := ResultadoAgregacionTemporal{
		Series:       []string{"sensor1/temp", "sensor2/temp"},
		Tiempos:      []int64{ahora, ahora + intervalo, ahora + 2*intervalo},
		Agregaciones: []TipoAgregacion{AgregacionPromedio},
		Valores: [][][]float64{
			{ // agregación 0 (promedio)
				{25.5, 26.0}, // bucket 0
				{25.8, 26.3}, // bucket 1
				{26.0, 26.5}, // bucket 2
			},
		},
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar ResultadoAgregacionTemporal: %v", err)
	}

	var deserializado ResultadoAgregacionTemporal
	err = DeserializarGob(data, &deserializado)
	if err != nil {
		t.Fatalf("Error al deserializar ResultadoAgregacionTemporal: %v", err)
	}

	// Verificar series
	if len(deserializado.Series) != len(original.Series) {
		t.Errorf("Cantidad de series incorrecta: esperadas %d, obtenidas %d",
			len(original.Series), len(deserializado.Series))
	}
	for i, serie := range deserializado.Series {
		if serie != original.Series[i] {
			t.Errorf("Serie %d incorrecta: esperada %s, obtenida %s", i, original.Series[i], serie)
		}
	}

	// Verificar tiempos
	if len(deserializado.Tiempos) != len(original.Tiempos) {
		t.Errorf("Cantidad de tiempos incorrecta: esperados %d, obtenidos %d",
			len(original.Tiempos), len(deserializado.Tiempos))
	}
	for i, tiempo := range deserializado.Tiempos {
		if tiempo != original.Tiempos[i] {
			t.Errorf("Tiempo %d incorrecto: esperado %d, obtenido %d", i, original.Tiempos[i], tiempo)
		}
	}

	// Verificar agregaciones
	if len(deserializado.Agregaciones) != len(original.Agregaciones) {
		t.Errorf("Cantidad de agregaciones incorrecta: esperadas %d, obtenidas %d",
			len(original.Agregaciones), len(deserializado.Agregaciones))
	}

	// Verificar matriz de valores [agregacion][bucket][serie]
	if len(deserializado.Valores) != len(original.Valores) {
		t.Errorf("Cantidad de agregaciones en valores incorrecta: esperadas %d, obtenidas %d",
			len(original.Valores), len(deserializado.Valores))
	}
	for a, agregacion := range deserializado.Valores {
		if len(agregacion) != len(original.Valores[a]) {
			t.Errorf("Agregación %d: cantidad de buckets incorrecta: esperados %d, obtenidos %d",
				a, len(original.Valores[a]), len(agregacion))
		}
		for b, bucket := range agregacion {
			if len(bucket) != len(original.Valores[a][b]) {
				t.Errorf("Bucket [%d][%d]: cantidad de series incorrecta: esperadas %d, obtenidas %d",
					a, b, len(original.Valores[a][b]), len(bucket))
			}
			for s, valor := range bucket {
				if valor != original.Valores[a][b][s] {
					t.Errorf("Valor [%d][%d][%d] incorrecto: esperado %f, obtenido %f",
						a, b, s, original.Valores[a][b][s], valor)
				}
			}
		}
	}

	t.Logf("✓ ResultadoAgregacionTemporal con %d series, %d buckets y %d agregaciones serializado correctamente",
		len(deserializado.Series), len(deserializado.Tiempos), len(deserializado.Agregaciones))
}

// TestSerializarDeserializarGob_ResultadoAgregacionTemporalConNaN verifica serialización con valores NaN
func TestSerializarDeserializarGob_ResultadoAgregacionTemporalConNaN(t *testing.T) {
	ahora := time.Now().UnixNano()
	intervalo := int64(time.Minute)

	original := ResultadoAgregacionTemporal{
		Series:       []string{"sensor1/temp", "sensor2/temp"},
		Tiempos:      []int64{ahora, ahora + intervalo},
		Agregaciones: []TipoAgregacion{AgregacionPromedio},
		Valores: [][][]float64{
			{ // agregación 0 (promedio)
				{25.5, math.NaN()}, // bucket 0: sensor1 tiene dato, sensor2 no
				{math.NaN(), 26.0}, // bucket 1: sensor1 no tiene, sensor2 sí
			},
		},
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar ResultadoAgregacionTemporal con NaN: %v", err)
	}

	var deserializado ResultadoAgregacionTemporal
	err = DeserializarGob(data, &deserializado)
	if err != nil {
		t.Fatalf("Error al deserializar ResultadoAgregacionTemporal con NaN: %v", err)
	}

	// Verificar que los NaN se preservan [agregacion][bucket][serie]
	if !math.IsNaN(deserializado.Valores[0][0][1]) {
		t.Errorf("Valor [0][0][1] debería ser NaN, obtenido %f", deserializado.Valores[0][0][1])
	}
	if !math.IsNaN(deserializado.Valores[0][1][0]) {
		t.Errorf("Valor [0][1][0] debería ser NaN, obtenido %f", deserializado.Valores[0][1][0])
	}

	// Verificar que los valores normales se preservan
	if deserializado.Valores[0][0][0] != 25.5 {
		t.Errorf("Valor [0][0][0] incorrecto: esperado 25.5, obtenido %f", deserializado.Valores[0][0][0])
	}
	if deserializado.Valores[0][1][1] != 26.0 {
		t.Errorf("Valor [0][1][1] incorrecto: esperado 26.0, obtenido %f", deserializado.Valores[0][1][1])
	}

	t.Log("✓ ResultadoAgregacionTemporal con NaN serializado/deserializado correctamente")
}

// TestSerializarDeserializarGob_RespuestaConsultaAgregacionTemporal verifica serialización de respuesta
func TestSerializarDeserializarGob_RespuestaConsultaAgregacionTemporal(t *testing.T) {
	ahora := time.Now().UnixNano()
	intervalo := int64(time.Minute)

	original := RespuestaConsultaAgregacionTemporal{
		Resultado: ResultadoAgregacionTemporal{
			Series:       []string{"sensor/temp"},
			Tiempos:      []int64{ahora, ahora + intervalo},
			Agregaciones: []TipoAgregacion{AgregacionPromedio},
			Valores: [][][]float64{
				{ // agregación 0
					{25.5}, // bucket 0
					{26.0}, // bucket 1
				},
			},
		},
		Error: "",
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar RespuestaConsultaAgregacionTemporal: %v", err)
	}

	var deserializada RespuestaConsultaAgregacionTemporal
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar RespuestaConsultaAgregacionTemporal: %v", err)
	}

	if len(deserializada.Resultado.Series) != 1 {
		t.Errorf("Cantidad de series incorrecta: esperada 1, obtenida %d", len(deserializada.Resultado.Series))
	}

	if len(deserializada.Resultado.Tiempos) != 2 {
		t.Errorf("Cantidad de tiempos incorrecta: esperados 2, obtenidos %d", len(deserializada.Resultado.Tiempos))
	}

	// [agregacion][bucket][serie]
	if deserializada.Resultado.Valores[0][0][0] != 25.5 {
		t.Errorf("Valor [0][0][0] incorrecto: esperado 25.5, obtenido %f", deserializada.Resultado.Valores[0][0][0])
	}

	t.Log("✓ RespuestaConsultaAgregacionTemporal serializada/deserializada correctamente")
}

// TestSerializarDeserializarGob_RespuestaConsultaAgregacionTemporalConError verifica respuesta con error
func TestSerializarDeserializarGob_RespuestaConsultaAgregacionTemporalConError(t *testing.T) {
	original := RespuestaConsultaAgregacionTemporal{
		Resultado: ResultadoAgregacionTemporal{},
		Error:     "intervalo inválido",
	}

	data, err := SerializarGob(original)
	if err != nil {
		t.Fatalf("Error al serializar RespuestaConsultaAgregacionTemporal con error: %v", err)
	}

	var deserializada RespuestaConsultaAgregacionTemporal
	err = DeserializarGob(data, &deserializada)
	if err != nil {
		t.Fatalf("Error al deserializar RespuestaConsultaAgregacionTemporal con error: %v", err)
	}

	if deserializada.Error != original.Error {
		t.Errorf("Error incorrecto: esperado '%s', obtenido '%s'", original.Error, deserializada.Error)
	}

	if len(deserializada.Resultado.Series) != 0 {
		t.Errorf("Series deberían estar vacías, obtenidas %d", len(deserializada.Resultado.Series))
	}

	t.Log("✓ RespuestaConsultaAgregacionTemporal con error serializada correctamente")
}
