package compresor

import (
	"math"
	"testing"

	"github.com/sensorwave-dev/sensorwave/tipos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Tests para CompresorNingunoGenerico (Sin Compresion)
// Soporta: int64, float64, bool, string
// =============================================================================

func TestCompresorNinguno_Int64(t *testing.T) {
	c := &CompresorNingunoGenerico[int64]{}

	testCases := []struct {
		name    string
		valores []int64
	}{
		{"vacio", []int64{}},
		{"un_valor", []int64{42}},
		{"multiples_valores", []int64{1, 2, 3, 4, 5}},
		{"valores_negativos", []int64{-100, -50, 0, 50, 100}},
		{"valores_extremos", []int64{math.MinInt64, 0, math.MaxInt64}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

func TestCompresorNinguno_Float64(t *testing.T) {
	c := &CompresorNingunoGenerico[float64]{}

	testCases := []struct {
		name    string
		valores []float64
	}{
		{"vacio", []float64{}},
		{"un_valor", []float64{3.14159}},
		{"multiples_valores", []float64{1.1, 2.2, 3.3, 4.4, 5.5}},
		{"valores_negativos", []float64{-100.5, -50.25, 0.0, 50.25, 100.5}},
		{"valores_especiales", []float64{math.Inf(1), math.Inf(-1), math.SmallestNonzeroFloat64}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

func TestCompresorNinguno_Bool(t *testing.T) {
	c := &CompresorNingunoGenerico[bool]{}

	testCases := []struct {
		name    string
		valores []bool
	}{
		{"vacio", []bool{}},
		{"un_valor_true", []bool{true}},
		{"un_valor_false", []bool{false}},
		{"multiples_valores", []bool{true, false, true, false, true}},
		{"todos_true", []bool{true, true, true, true}},
		{"todos_false", []bool{false, false, false, false}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

func TestCompresorNinguno_String(t *testing.T) {
	c := &CompresorNingunoGenerico[string]{}

	testCases := []struct {
		name    string
		valores []string
	}{
		{"vacio", []string{}},
		{"un_valor", []string{"hola"}},
		{"multiples_valores", []string{"hola", "mundo", "test"}},
		{"strings_vacios", []string{"", "", ""}},
		{"mixto", []string{"", "texto", "", "otro"}},
		{"unicode", []string{"hola", "mundo", "unicode"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

// =============================================================================
// Tests para CompresorRLEGenerico (Run-Length Encoding)
// Soporta: Boolean, Integer, Real, Text
// =============================================================================

func TestCompresorRLE_Int64(t *testing.T) {
	c := &CompresorRLEGenerico[int64]{}

	testCases := []struct {
		name    string
		valores []int64
	}{
		{"vacio", []int64{}},
		{"un_valor", []int64{42}},
		{"sin_repeticion", []int64{1, 2, 3, 4, 5}},
		{"con_repeticion", []int64{1, 1, 1, 2, 2, 3}},
		{"todos_iguales", []int64{5, 5, 5, 5, 5}},
		{"valores_negativos", []int64{-1, -1, 0, 0, 1, 1}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

func TestCompresorRLE_Float64(t *testing.T) {
	c := &CompresorRLEGenerico[float64]{}

	testCases := []struct {
		name    string
		valores []float64
	}{
		{"vacio", []float64{}},
		{"un_valor", []float64{3.14}},
		{"con_repeticion", []float64{1.5, 1.5, 1.5, 2.5, 2.5}},
		{"sin_repeticion", []float64{1.1, 2.2, 3.3}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

func TestCompresorRLE_Bool(t *testing.T) {
	c := &CompresorRLEGenerico[bool]{}

	testCases := []struct {
		name    string
		valores []bool
	}{
		{"vacio", []bool{}},
		{"un_valor", []bool{true}},
		{"alternado", []bool{true, false, true, false}},
		{"con_repeticion", []bool{true, true, true, false, false}},
		{"todos_true", []bool{true, true, true, true}},
		{"todos_false", []bool{false, false, false, false}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

func TestCompresorRLE_String(t *testing.T) {
	c := &CompresorRLEGenerico[string]{}

	testCases := []struct {
		name    string
		valores []string
	}{
		{"vacio", []string{}},
		{"un_valor", []string{"test"}},
		{"con_repeticion", []string{"a", "a", "a", "b", "b"}},
		{"sin_repeticion", []string{"a", "b", "c"}},
		{"strings_vacios", []string{"", "", ""}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

// Test de compresion efectiva RLE con muchas repeticiones
func TestCompresorRLE_EficienciaCompresion(t *testing.T) {
	c := &CompresorRLEGenerico[int64]{}

	// 100 valores todos iguales -> deberia comprimir muy bien
	valores := make([]int64, 100)
	for i := range valores {
		valores[i] = 42
	}

	comprimido, err := c.Comprimir(valores)
	require.NoError(t, err)

	descomprimido, err := c.Descomprimir(comprimido)
	require.NoError(t, err)

	assert.Equal(t, valores, descomprimido)
	// Verificar que la compresion fue efectiva
	// 100 int64 = 800 bytes, RLE con todos iguales deberia ser mucho menor
	assert.Less(t, len(comprimido), 800)
}

// =============================================================================
// Tests para CompresorDeltaDeltaGenerico
// Soporta: Integer, Real
// =============================================================================

func TestCompresorDeltaDelta_Int64(t *testing.T) {
	c := &CompresorDeltaDeltaGenerico[int64]{}

	testCases := []struct {
		name    string
		valores []int64
	}{
		{"vacio", []int64{}},
		{"un_valor", []int64{100}},
		{"dos_valores", []int64{100, 200}},
		{"monotono_creciente", []int64{100, 200, 300, 400, 500}},
		{"monotono_decreciente", []int64{500, 400, 300, 200, 100}},
		{"delta_constante", []int64{0, 10, 20, 30, 40, 50}},
		{"valores_aleatorios", []int64{5, 12, 7, 25, 3, 18}},
		{"valores_negativos", []int64{-100, -50, 0, 50, 100}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

func TestCompresorDeltaDelta_Float64(t *testing.T) {
	c := &CompresorDeltaDeltaGenerico[float64]{}

	testCases := []struct {
		name    string
		valores []float64
	}{
		{"vacio", []float64{}},
		{"un_valor", []float64{100.5}},
		{"dos_valores", []float64{100.5, 200.5}},
		{"monotono_creciente", []float64{100.0, 200.0, 300.0, 400.0}},
		{"valores_aleatorios", []float64{1.5, 2.7, 3.2, 4.8, 5.1}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

// Test de eficiencia para series monotonas (caso optimo para DeltaDelta)
func TestCompresorDeltaDelta_EficienciaSerieMnotona(t *testing.T) {
	c := &CompresorDeltaDeltaGenerico[int64]{}

	// Serie monotona con delta constante (timestamps tipicos)
	valores := make([]int64, 100)
	for i := range valores {
		valores[i] = int64(1000 + i*10) // delta constante de 10
	}

	comprimido, err := c.Comprimir(valores)
	require.NoError(t, err)

	descomprimido, err := c.Descomprimir(comprimido)
	require.NoError(t, err)

	assert.Equal(t, valores, descomprimido)
}

// =============================================================================
// Tests para CompresorBitsGenerico
// Soporta: Integer (int64)
// =============================================================================

func TestCompresorBits_Int64(t *testing.T) {
	c := &CompresorBitsGenerico[int64]{}

	testCases := []struct {
		name    string
		valores []int64
	}{
		{"vacio", []int64{}},
		{"un_valor", []int64{42}},
		{"rango_pequeno", []int64{0, 1, 2, 3, 4, 5, 6, 7}},
		{"rango_0_255", []int64{0, 128, 255}},
		{"todos_iguales", []int64{100, 100, 100, 100}},
		{"valores_negativos", []int64{-10, -5, 0, 5, 10}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

// Test de eficiencia para rangos pequenos
func TestCompresorBits_EficienciaRangoPequeno(t *testing.T) {
	c := &CompresorBitsGenerico[int64]{}

	// Valores en rango 0-15 -> solo necesitan 4 bits cada uno
	valores := make([]int64, 100)
	for i := range valores {
		valores[i] = int64(i % 16)
	}

	comprimido, err := c.Comprimir(valores)
	require.NoError(t, err)

	descomprimido, err := c.Descomprimir(comprimido)
	require.NoError(t, err)

	assert.Equal(t, valores, descomprimido)
}

// =============================================================================
// Tests para CompresorXor (Gorilla)
// Soporta: Real (float64)
// =============================================================================

func TestCompresorXor_Float64(t *testing.T) {
	c := &CompresorXor{}

	testCases := []struct {
		name    string
		valores []float64
	}{
		{"vacio", []float64{}},
		{"un_valor", []float64{25.5}},
		{"valores_similares", []float64{25.0, 25.1, 25.2, 25.3, 25.4}},
		{"valores_identicos", []float64{20.0, 20.0, 20.0, 20.0}},
		{"cambios_grandes", []float64{0.0, 100.0, 200.0, 300.0}},
		{"valores_negativos", []float64{-10.5, -10.3, -10.1, -9.9}},
		{"valores_mixtos", []float64{1.0, -1.0, 2.0, -2.0, 3.0}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

// Test de eficiencia para valores similares (caso optimo para Xor/Gorilla)
func TestCompresorXor_EficienciaValoresSimilares(t *testing.T) {
	c := &CompresorXor{}

	// Valores de temperatura con cambios pequenos
	valores := make([]float64, 100)
	valores[0] = 25.0
	for i := 1; i < len(valores); i++ {
		// Pequenas variaciones
		valores[i] = valores[i-1] + 0.1
	}

	comprimido, err := c.Comprimir(valores)
	require.NoError(t, err)

	descomprimido, err := c.Descomprimir(comprimido)
	require.NoError(t, err)

	assert.Equal(t, valores, descomprimido)
	// Deberia comprimir bien porque los XOR tienen muchos ceros
	assert.Less(t, len(comprimido), 800) // 100 float64 = 800 bytes
}

// =============================================================================
// Tests para CompresorDiccionario
// Soporta: Text (string)
// =============================================================================

func TestCompresorDiccionario_String(t *testing.T) {
	c := &CompresorDiccionario{}

	testCases := []struct {
		name    string
		valores []string
	}{
		{"vacio", []string{}},
		{"un_valor", []string{"activo"}},
		{"valores_unicos", []string{"a", "b", "c", "d"}},
		{"con_repeticion", []string{"activo", "activo", "inactivo", "activo"}},
		{"vocabulario_limitado", []string{"on", "off", "on", "off", "on", "off"}},
		{"estados_tipicos", []string{"error", "ok", "warning", "ok", "error", "ok"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido, err := c.Comprimir(tc.valores)
			require.NoError(t, err)

			descomprimido, err := c.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, tc.valores, descomprimido)
		})
	}
}

// Test de eficiencia con vocabulario limitado
func TestCompresorDiccionario_EficienciaVocabularioLimitado(t *testing.T) {
	c := &CompresorDiccionario{}

	// 100 valores con solo 3 opciones
	opciones := []string{"activo", "inactivo", "error"}
	valores := make([]string, 100)
	for i := range valores {
		valores[i] = opciones[i%3]
	}

	comprimido, err := c.Comprimir(valores)
	require.NoError(t, err)

	descomprimido, err := c.Descomprimir(comprimido)
	require.NoError(t, err)

	assert.Equal(t, valores, descomprimido)
}

// =============================================================================
// Tests de casos edge
// =============================================================================

func TestCompresores_CasosEdge_ValoresExtremos(t *testing.T) {
	t.Run("int64_extremos", func(t *testing.T) {
		c := &CompresorNingunoGenerico[int64]{}
		valores := []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}

		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)

		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)

		assert.Equal(t, valores, descomprimido)
	})

	t.Run("float64_especiales", func(t *testing.T) {
		c := &CompresorNingunoGenerico[float64]{}
		valores := []float64{math.MaxFloat64, math.SmallestNonzeroFloat64, math.Inf(1), math.Inf(-1)}

		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)

		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)

		assert.Equal(t, valores, descomprimido)
	})
}

func TestCompresores_CasosEdge_ArrayGrande(t *testing.T) {
	t.Run("1000_elementos_int64", func(t *testing.T) {
		c := &CompresorNingunoGenerico[int64]{}
		valores := make([]int64, 1000)
		for i := range valores {
			valores[i] = int64(i * 100)
		}

		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)

		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)

		assert.Equal(t, valores, descomprimido)
	})

	t.Run("1000_elementos_rle", func(t *testing.T) {
		c := &CompresorRLEGenerico[int64]{}
		valores := make([]int64, 1000)
		for i := range valores {
			valores[i] = int64(i / 10) // grupos de 10 valores iguales
		}

		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)

		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)

		assert.Equal(t, valores, descomprimido)
	})
}

// =============================================================================
// Tests de compatibilidad de algoritmos con tipos de datos
// Segun tipo_datos.go:
// - Boolean: SinCompresion, RLE
// - Integer: SinCompresion, DeltaDelta, RLE, Bits
// - Real: SinCompresion, DeltaDelta, Xor, RLE
// - Text: SinCompresion, RLE, Diccionario
// =============================================================================

func TestCompatibilidadTipos_Boolean(t *testing.T) {
	valores := []bool{true, true, false, true, false, false}

	t.Run("SinCompresion", func(t *testing.T) {
		c := &CompresorNingunoGenerico[bool]{}
		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valores, descomprimido)
	})

	t.Run("RLE", func(t *testing.T) {
		c := &CompresorRLEGenerico[bool]{}
		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valores, descomprimido)
	})
}

func TestCompatibilidadTipos_Integer(t *testing.T) {
	valores := []int64{100, 110, 120, 130, 140, 150}

	t.Run("SinCompresion", func(t *testing.T) {
		c := &CompresorNingunoGenerico[int64]{}
		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valores, descomprimido)
	})

	t.Run("DeltaDelta", func(t *testing.T) {
		c := &CompresorDeltaDeltaGenerico[int64]{}
		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valores, descomprimido)
	})

	t.Run("RLE", func(t *testing.T) {
		valoresRLE := []int64{100, 100, 100, 200, 200}
		c := &CompresorRLEGenerico[int64]{}
		comprimido, err := c.Comprimir(valoresRLE)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valoresRLE, descomprimido)
	})

	t.Run("Bits", func(t *testing.T) {
		c := &CompresorBitsGenerico[int64]{}
		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valores, descomprimido)
	})
}

func TestCompatibilidadTipos_Real(t *testing.T) {
	valores := []float64{25.0, 25.1, 25.2, 25.3, 25.4}

	t.Run("SinCompresion", func(t *testing.T) {
		c := &CompresorNingunoGenerico[float64]{}
		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valores, descomprimido)
	})

	t.Run("DeltaDelta", func(t *testing.T) {
		c := &CompresorDeltaDeltaGenerico[float64]{}
		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valores, descomprimido)
	})

	t.Run("Xor", func(t *testing.T) {
		c := &CompresorXor{}
		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valores, descomprimido)
	})

	t.Run("RLE", func(t *testing.T) {
		valoresRLE := []float64{25.0, 25.0, 25.0, 30.0, 30.0}
		c := &CompresorRLEGenerico[float64]{}
		comprimido, err := c.Comprimir(valoresRLE)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valoresRLE, descomprimido)
	})
}

func TestCompatibilidadTipos_Text(t *testing.T) {
	valores := []string{"activo", "activo", "inactivo", "error"}

	t.Run("SinCompresion", func(t *testing.T) {
		c := &CompresorNingunoGenerico[string]{}
		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valores, descomprimido)
	})

	t.Run("RLE", func(t *testing.T) {
		c := &CompresorRLEGenerico[string]{}
		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valores, descomprimido)
	})

	t.Run("Diccionario", func(t *testing.T) {
		c := &CompresorDiccionario{}
		comprimido, err := c.Comprimir(valores)
		require.NoError(t, err)
		descomprimido, err := c.Descomprimir(comprimido)
		require.NoError(t, err)
		assert.Equal(t, valores, descomprimido)
	})
}

// =============================================================================
// Tests para Compresores de Bloque (LZ4, ZSTD, Snappy, Gzip, Ninguna)
// =============================================================================

func TestCompresorBloque_LZ4(t *testing.T) {
	c := &CompresorLZ4{}
	datos := []byte("Este es un texto de prueba para compresión LZ4 con datos repetidos repetidos repetidos")

	comprimido, err := c.Comprimir(datos)
	require.NoError(t, err)

	descomprimido, err := c.Descomprimir(comprimido)
	require.NoError(t, err)

	assert.Equal(t, datos, descomprimido)
}

func TestCompresorBloque_ZSTD(t *testing.T) {
	c := &CompresorZSTD{}
	datos := []byte("Este es un texto de prueba para compresión ZSTD con datos repetidos repetidos repetidos")

	comprimido, err := c.Comprimir(datos)
	require.NoError(t, err)

	descomprimido, err := c.Descomprimir(comprimido)
	require.NoError(t, err)

	assert.Equal(t, datos, descomprimido)
}

func TestCompresorBloque_Snappy(t *testing.T) {
	c := &CompresorSnappy{}
	datos := []byte("Este es un texto de prueba para compresión Snappy con datos repetidos repetidos repetidos")

	comprimido, err := c.Comprimir(datos)
	require.NoError(t, err)

	descomprimido, err := c.Descomprimir(comprimido)
	require.NoError(t, err)

	assert.Equal(t, datos, descomprimido)
}

func TestCompresorBloque_Gzip(t *testing.T) {
	c := &CompresorGzip{}
	datos := []byte("Este es un texto de prueba para compresión Gzip con datos repetidos repetidos repetidos")

	comprimido, err := c.Comprimir(datos)
	require.NoError(t, err)

	descomprimido, err := c.Descomprimir(comprimido)
	require.NoError(t, err)

	assert.Equal(t, datos, descomprimido)
}

func TestCompresorBloque_Ninguna(t *testing.T) {
	c := &CompresorBloqueNinguno{}
	datos := []byte("Este texto no se comprime")

	comprimido, err := c.Comprimir(datos)
	require.NoError(t, err)
	assert.Equal(t, datos, comprimido)

	descomprimido, err := c.Descomprimir(comprimido)
	require.NoError(t, err)
	assert.Equal(t, datos, descomprimido)
}

func TestObtenerCompresorBloque(t *testing.T) {
	testCases := []struct {
		tipo     tipos.TipoCompresionBloque
		expected string
	}{
		{tipos.LZ4, "*CompresorLZ4"},
		{tipos.ZSTD, "*CompresorZSTD"},
		{tipos.Snappy, "*CompresorSnappy"},
		{tipos.Gzip, "*CompresorGzip"},
		{tipos.Ninguna, "*CompresorBloqueNinguno"},
		{tipos.TipoCompresionBloque("desconocido"), "*CompresorBloqueNinguno"}, // default
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			c := ObtenerCompresorBloque(tc.tipo)
			assert.NotNil(t, c)
		})
	}
}

func TestCompresoresBloque_DatosVacios(t *testing.T) {
	compresores := []CompresorBloque{
		&CompresorLZ4{},
		&CompresorZSTD{},
		&CompresorSnappy{},
		&CompresorGzip{},
		&CompresorBloqueNinguno{},
	}

	for _, c := range compresores {
		comprimido, err := c.Comprimir([]byte{})
		require.NoError(t, err)
		assert.Empty(t, comprimido)

		descomprimido, err := c.Descomprimir([]byte{})
		require.NoError(t, err)
		assert.Empty(t, descomprimido)
	}
}

func TestCompresoresBloque_DatosGrandes(t *testing.T) {
	// Crear datos de 10KB con patron repetitivo (comprime bien)
	datos := make([]byte, 10*1024)
	for i := range datos {
		datos[i] = byte(i % 256)
	}

	compresores := []struct {
		nombre string
		comp   CompresorBloque
	}{
		{"LZ4", &CompresorLZ4{}},
		{"ZSTD", &CompresorZSTD{}},
		{"Snappy", &CompresorSnappy{}},
		{"Gzip", &CompresorGzip{}},
	}

	for _, tc := range compresores {
		t.Run(tc.nombre, func(t *testing.T) {
			comprimido, err := tc.comp.Comprimir(datos)
			require.NoError(t, err)

			descomprimido, err := tc.comp.Descomprimir(comprimido)
			require.NoError(t, err)

			assert.Equal(t, datos, descomprimido)
		})
	}
}

// =============================================================================
// Tests para Compresión de Tiempos (DeltaDelta específico para timestamps)
// =============================================================================

func TestCompresionTiempo_Vacio(t *testing.T) {
	mediciones := []tipos.Medicion{}

	comprimido := CompresionDeltaDeltaTiempo(mediciones)
	assert.Empty(t, comprimido)

	tiempos, err := DescompresionDeltaDeltaTiempo(comprimido)
	require.NoError(t, err)
	assert.Empty(t, tiempos)
}

func TestCompresionTiempo_UnValor(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: 25.5},
	}

	comprimido := CompresionDeltaDeltaTiempo(mediciones)
	require.NotEmpty(t, comprimido)

	tiempos, err := DescompresionDeltaDeltaTiempo(comprimido)
	require.NoError(t, err)
	require.Len(t, tiempos, 1)
	assert.Equal(t, int64(1000000000), tiempos[0])
}

func TestCompresionTiempo_DosValores(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: 25.5},
		{Tiempo: 1000001000, Valor: 26.0},
	}

	comprimido := CompresionDeltaDeltaTiempo(mediciones)
	require.NotEmpty(t, comprimido)

	tiempos, err := DescompresionDeltaDeltaTiempo(comprimido)
	require.NoError(t, err)
	require.Len(t, tiempos, 2)
	assert.Equal(t, int64(1000000000), tiempos[0])
	assert.Equal(t, int64(1000001000), tiempos[1])
}

func TestCompresionTiempo_MultiplesValores(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: 25.5},
		{Tiempo: 1000001000, Valor: 26.0},
		{Tiempo: 1000002000, Valor: 26.5},
		{Tiempo: 1000003000, Valor: 27.0},
		{Tiempo: 1000004000, Valor: 27.5},
	}

	comprimido := CompresionDeltaDeltaTiempo(mediciones)
	require.NotEmpty(t, comprimido)

	tiempos, err := DescompresionDeltaDeltaTiempo(comprimido)
	require.NoError(t, err)
	require.Len(t, tiempos, 5)

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, tiempos[i])
	}
}

func TestCompresionTiempo_DeltaConstante(t *testing.T) {
	// Simula timestamps con intervalo constante (caso óptimo para DeltaDelta)
	mediciones := make([]tipos.Medicion, 100)
	for i := range mediciones {
		mediciones[i] = tipos.Medicion{
			Tiempo: int64(1000000000 + i*1000), // delta constante de 1000
			Valor:  float64(i),
		}
	}

	comprimido := CompresionDeltaDeltaTiempo(mediciones)
	require.NotEmpty(t, comprimido)

	tiempos, err := DescompresionDeltaDeltaTiempo(comprimido)
	require.NoError(t, err)
	require.Len(t, tiempos, 100)

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, tiempos[i])
	}
}

func TestCompresionTiempo_DeltaVariable(t *testing.T) {
	// Timestamps con deltas variables
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: 1.0},
		{Tiempo: 1000001000, Valor: 2.0}, // delta 1000
		{Tiempo: 1000003000, Valor: 3.0}, // delta 2000
		{Tiempo: 1000003500, Valor: 4.0}, // delta 500
		{Tiempo: 1000010000, Valor: 5.0}, // delta 6500
	}

	comprimido := CompresionDeltaDeltaTiempo(mediciones)
	require.NotEmpty(t, comprimido)

	tiempos, err := DescompresionDeltaDeltaTiempo(comprimido)
	require.NoError(t, err)
	require.Len(t, tiempos, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, tiempos[i])
	}
}

func TestCompresionTiempo_DeltasGrandes(t *testing.T) {
	// Probar diferentes tamaños de delta-delta para cubrir flags 0x01, 0x02, 0x03
	testCases := []struct {
		name       string
		mediciones []tipos.Medicion
	}{
		{
			name: "delta_2bytes", // flag 0x01
			mediciones: []tipos.Medicion{
				{Tiempo: 1000000000, Valor: 1.0},
				{Tiempo: 1000001000, Valor: 2.0},
				{Tiempo: 1000002000, Valor: 3.0},
				{Tiempo: 1000020000, Valor: 4.0}, // delta grande
			},
		},
		{
			name: "delta_4bytes", // flag 0x02
			mediciones: []tipos.Medicion{
				{Tiempo: 1000000000, Valor: 1.0},
				{Tiempo: 1000001000, Valor: 2.0},
				{Tiempo: 1000002000, Valor: 3.0},
				{Tiempo: 1010000000, Valor: 4.0}, // delta muy grande
			},
		},
		{
			name: "delta_8bytes", // flag 0x03
			mediciones: []tipos.Medicion{
				{Tiempo: 1000000000, Valor: 1.0},
				{Tiempo: 1000001000, Valor: 2.0},
				{Tiempo: 1000002000, Valor: 3.0},
				{Tiempo: 5000000000000, Valor: 4.0}, // delta enorme
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			comprimido := CompresionDeltaDeltaTiempo(tc.mediciones)
			require.NotEmpty(t, comprimido)

			tiempos, err := DescompresionDeltaDeltaTiempo(comprimido)
			require.NoError(t, err)
			require.Len(t, tiempos, len(tc.mediciones))

			for i, m := range tc.mediciones {
				assert.Equal(t, m.Tiempo, tiempos[i])
			}
		})
	}
}

func TestDescompresionTiempo_DatosInsuficientes(t *testing.T) {
	// Menos de 8 bytes para el primer tiempo
	_, err := DescompresionDeltaDeltaTiempo([]byte{1, 2, 3})
	assert.Error(t, err)
}

// =============================================================================
// Tests para Funciones de Utilidad (compresion_utils.go)
// =============================================================================

func TestInt64ToBytes_BytesToInt64(t *testing.T) {
	testCases := []int64{
		0,
		1,
		-1,
		127,
		-128,
		32767,
		-32768,
		2147483647,
		-2147483648,
		1000000000000,
		-1000000000000,
	}

	for _, val := range testCases {
		bytes := Int64ToBytes(val)
		result := BytesToInt64(bytes)
		assert.Equal(t, val, result)
	}
}

func TestInt64ToBytes_BytesToInt64_Extremos(t *testing.T) {
	// Valores extremos de int64
	extremos := []int64{math.MinInt64, math.MaxInt64}

	for _, val := range extremos {
		bytes := Int64ToBytes(val)
		result := BytesToInt64(bytes)
		assert.Equal(t, val, result)
	}
}

func TestBytesToInt64_DatosInsuficientes(t *testing.T) {
	// Con menos de 8 bytes debe retornar 0
	result := BytesToInt64([]byte{1, 2, 3})
	assert.Equal(t, int64(0), result)
}

func TestExtraerValores(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000, Valor: 25.5},
		{Tiempo: 2000, Valor: "texto"},
		{Tiempo: 3000, Valor: int64(42)},
		{Tiempo: 4000, Valor: true},
	}

	valores := ExtraerValores(mediciones)

	require.Len(t, valores, 4)
	assert.Equal(t, 25.5, valores[0])
	assert.Equal(t, "texto", valores[1])
	assert.Equal(t, int64(42), valores[2])
	assert.Equal(t, true, valores[3])
}

func TestExtraerTiempos(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000, Valor: 25.5},
		{Tiempo: 2000, Valor: 26.0},
		{Tiempo: 3000, Valor: 26.5},
	}

	tiempos := ExtraerTiempos(mediciones)

	require.Len(t, tiempos, 3)
	assert.Equal(t, int64(1000), tiempos[0])
	assert.Equal(t, int64(2000), tiempos[1])
	assert.Equal(t, int64(3000), tiempos[2])
}

func TestConvertirAInt64Array(t *testing.T) {
	testCases := []struct {
		name     string
		input    []interface{}
		expected []int64
	}{
		{"int", []interface{}{int(1), int(2), int(3)}, []int64{1, 2, 3}},
		{"int32", []interface{}{int32(10), int32(20)}, []int64{10, 20}},
		{"int64", []interface{}{int64(100), int64(200)}, []int64{100, 200}},
		{"float64", []interface{}{float64(1.9), float64(2.1)}, []int64{1, 2}},
		{"vacio", []interface{}{}, []int64{}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ConvertirAInt64Array(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestConvertirAInt64Array_Error(t *testing.T) {
	// Tipo no soportado
	_, err := ConvertirAInt64Array([]interface{}{"texto"})
	assert.Error(t, err)
}

func TestConvertirAFloat64Array(t *testing.T) {
	testCases := []struct {
		name     string
		input    []interface{}
		expected []float64
	}{
		{"float64", []interface{}{float64(1.5), float64(2.5)}, []float64{1.5, 2.5}},
		{"float32", []interface{}{float32(1.5), float32(2.5)}, []float64{1.5, 2.5}},
		{"int", []interface{}{int(1), int(2)}, []float64{1.0, 2.0}},
		{"int32", []interface{}{int32(10), int32(20)}, []float64{10.0, 20.0}},
		{"int64", []interface{}{int64(100), int64(200)}, []float64{100.0, 200.0}},
		{"vacio", []interface{}{}, []float64{}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ConvertirAFloat64Array(tc.input)
			require.NoError(t, err)
			assert.InDeltaSlice(t, tc.expected, result, 0.001)
		})
	}
}

func TestConvertirAFloat64Array_Error(t *testing.T) {
	_, err := ConvertirAFloat64Array([]interface{}{"texto"})
	assert.Error(t, err)
}

func TestConvertirAStringArray(t *testing.T) {
	input := []interface{}{"hola", "mundo", "test"}
	expected := []string{"hola", "mundo", "test"}

	result, err := ConvertirAStringArray(input)
	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestConvertirAStringArray_Error(t *testing.T) {
	_, err := ConvertirAStringArray([]interface{}{123})
	assert.Error(t, err)
}

func TestConvertirABoolArray(t *testing.T) {
	testCases := []struct {
		name     string
		input    []interface{}
		expected []bool
	}{
		{"bool", []interface{}{true, false, true}, []bool{true, false, true}},
		{"int", []interface{}{int(0), int(1), int(0)}, []bool{false, true, false}},
		{"int64", []interface{}{int64(0), int64(1)}, []bool{false, true}},
		{"float64", []interface{}{float64(0.0), float64(1.5)}, []bool{false, true}},
		{"vacio", []interface{}{}, []bool{}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ConvertirABoolArray(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestConvertirABoolArray_Error(t *testing.T) {
	_, err := ConvertirABoolArray([]interface{}{"texto"})
	assert.Error(t, err)
}

func TestCombinarDatos_SepararDatos(t *testing.T) {
	tiemposComprimidos := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	valoresComprimidos := []byte{10, 20, 30, 40}

	combinados := CombinarDatos(tiemposComprimidos, valoresComprimidos)
	require.NotEmpty(t, combinados)

	tiemposSeparados, valoresSeparados, err := SepararDatos(combinados)
	require.NoError(t, err)

	assert.Equal(t, tiemposComprimidos, tiemposSeparados)
	assert.Equal(t, valoresComprimidos, valoresSeparados)
}

func TestSepararDatos_DatosInsuficientes(t *testing.T) {
	// Menos de 8 bytes (header)
	_, _, err := SepararDatos([]byte{1, 2, 3})
	assert.Error(t, err)
}

func TestSepararDatos_TamañoInconsistente(t *testing.T) {
	// Header indica más datos de los disponibles
	datos := []byte{
		0, 0, 0, 100, // tamaño tiempos = 100
		0, 0, 0, 100, // tamaño valores = 100
		1, 2, 3, // solo 3 bytes de datos
	}
	_, _, err := SepararDatos(datos)
	assert.Error(t, err)
}

// =============================================================================
// Tests para DescomprimirBloqueSerie (compresion_bloque_serie.go)
// Integración completa: compresión de tiempos + valores + bloque
// =============================================================================

// Helper para crear bloque comprimido de prueba
func crearBloqueComprimido(t *testing.T, mediciones []tipos.Medicion, tipoDatos tipos.TipoDatos,
	compresionBytes tipos.TipoCompresion, compresionBloque tipos.TipoCompresionBloque) []byte {

	// Comprimir tiempos
	tiemposComprimidos := CompresionDeltaDeltaTiempo(mediciones)

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
			c := &CompresorDeltaDeltaGenerico[int64]{}
			valoresComprimidos, err = c.Comprimir(valores)
		case tipos.RLE:
			c := &CompresorRLEGenerico[int64]{}
			valoresComprimidos, err = c.Comprimir(valores)
		case tipos.Bits:
			c := &CompresorBitsGenerico[int64]{}
			valoresComprimidos, err = c.Comprimir(valores)
		case tipos.SinCompresion:
			c := &CompresorNingunoGenerico[int64]{}
			valoresComprimidos, err = c.Comprimir(valores)
		}
	case tipos.Real:
		valores := make([]float64, len(mediciones))
		for i, m := range mediciones {
			valores[i] = m.Valor.(float64)
		}
		switch compresionBytes {
		case tipos.Xor:
			c := &CompresorXor{}
			valoresComprimidos, err = c.Comprimir(valores)
		case tipos.DeltaDelta:
			c := &CompresorDeltaDeltaGenerico[float64]{}
			valoresComprimidos, err = c.Comprimir(valores)
		case tipos.RLE:
			c := &CompresorRLEGenerico[float64]{}
			valoresComprimidos, err = c.Comprimir(valores)
		case tipos.SinCompresion:
			c := &CompresorNingunoGenerico[float64]{}
			valoresComprimidos, err = c.Comprimir(valores)
		}
	case tipos.Boolean:
		valores := make([]bool, len(mediciones))
		for i, m := range mediciones {
			valores[i] = m.Valor.(bool)
		}
		switch compresionBytes {
		case tipos.RLE:
			c := &CompresorRLEGenerico[bool]{}
			valoresComprimidos, err = c.Comprimir(valores)
		case tipos.SinCompresion:
			c := &CompresorNingunoGenerico[bool]{}
			valoresComprimidos, err = c.Comprimir(valores)
		}
	case tipos.Text:
		valores := make([]string, len(mediciones))
		for i, m := range mediciones {
			valores[i] = m.Valor.(string)
		}
		switch compresionBytes {
		case tipos.Diccionario:
			c := &CompresorDiccionario{}
			valoresComprimidos, err = c.Comprimir(valores)
		case tipos.RLE:
			c := &CompresorRLEGenerico[string]{}
			valoresComprimidos, err = c.Comprimir(valores)
		case tipos.SinCompresion:
			c := &CompresorNingunoGenerico[string]{}
			valoresComprimidos, err = c.Comprimir(valores)
		}
	}
	require.NoError(t, err)

	// Combinar tiempos y valores
	datosCombinados := CombinarDatos(tiemposComprimidos, valoresComprimidos)

	// Comprimir bloque
	compBloque := ObtenerCompresorBloque(compresionBloque)
	bloqueComprimido, err := compBloque.Comprimir(datosCombinados)
	require.NoError(t, err)

	return bloqueComprimido
}

func TestDescomprimirBloqueSerie_Integer_DeltaDelta(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: int64(100)},
		{Tiempo: 1000001000, Valor: int64(110)},
		{Tiempo: 1000002000, Valor: int64(120)},
		{Tiempo: 1000003000, Valor: int64(130)},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Integer, tipos.DeltaDelta, tipos.LZ4)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Integer, tipos.DeltaDelta, tipos.LZ4)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Integer_RLE(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: int64(100)},
		{Tiempo: 1000001000, Valor: int64(100)},
		{Tiempo: 1000002000, Valor: int64(100)},
		{Tiempo: 1000003000, Valor: int64(200)},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Integer, tipos.RLE, tipos.Snappy)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Integer, tipos.RLE, tipos.Snappy)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Integer_Bits(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: int64(0)},
		{Tiempo: 1000001000, Valor: int64(5)},
		{Tiempo: 1000002000, Valor: int64(10)},
		{Tiempo: 1000003000, Valor: int64(15)},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Integer, tipos.Bits, tipos.ZSTD)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Integer, tipos.Bits, tipos.ZSTD)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Integer_SinCompresion(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: int64(100)},
		{Tiempo: 1000001000, Valor: int64(200)},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Integer, tipos.SinCompresion, tipos.Ninguna)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Integer, tipos.SinCompresion, tipos.Ninguna)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Real_Xor(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: float64(25.5)},
		{Tiempo: 1000001000, Valor: float64(25.6)},
		{Tiempo: 1000002000, Valor: float64(25.7)},
		{Tiempo: 1000003000, Valor: float64(25.8)},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Real, tipos.Xor, tipos.LZ4)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Real, tipos.Xor, tipos.LZ4)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Real_DeltaDelta(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: float64(100.0)},
		{Tiempo: 1000001000, Valor: float64(200.0)},
		{Tiempo: 1000002000, Valor: float64(300.0)},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Real, tipos.DeltaDelta, tipos.Gzip)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Real, tipos.DeltaDelta, tipos.Gzip)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Real_RLE(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: float64(25.5)},
		{Tiempo: 1000001000, Valor: float64(25.5)},
		{Tiempo: 1000002000, Valor: float64(25.5)},
		{Tiempo: 1000003000, Valor: float64(30.0)},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Real, tipos.RLE, tipos.Snappy)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Real, tipos.RLE, tipos.Snappy)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Real_SinCompresion(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: float64(25.5)},
		{Tiempo: 1000001000, Valor: float64(26.5)},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Real, tipos.SinCompresion, tipos.Ninguna)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Real, tipos.SinCompresion, tipos.Ninguna)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Boolean_RLE(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: true},
		{Tiempo: 1000001000, Valor: true},
		{Tiempo: 1000002000, Valor: false},
		{Tiempo: 1000003000, Valor: false},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Boolean, tipos.RLE, tipos.LZ4)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Boolean, tipos.RLE, tipos.LZ4)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Boolean_SinCompresion(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: true},
		{Tiempo: 1000001000, Valor: false},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Boolean, tipos.SinCompresion, tipos.Ninguna)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Boolean, tipos.SinCompresion, tipos.Ninguna)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Text_Diccionario(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: "activo"},
		{Tiempo: 1000001000, Valor: "activo"},
		{Tiempo: 1000002000, Valor: "inactivo"},
		{Tiempo: 1000003000, Valor: "error"},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Text, tipos.Diccionario, tipos.ZSTD)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Text, tipos.Diccionario, tipos.ZSTD)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Text_RLE(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: "on"},
		{Tiempo: 1000001000, Valor: "on"},
		{Tiempo: 1000002000, Valor: "off"},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Text, tipos.RLE, tipos.Snappy)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Text, tipos.RLE, tipos.Snappy)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_Text_SinCompresion(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: "hola"},
		{Tiempo: 1000001000, Valor: "mundo"},
	}

	bloque := crearBloqueComprimido(t, mediciones, tipos.Text, tipos.SinCompresion, tipos.Ninguna)

	resultado, err := DescomprimirBloqueSerie(bloque, tipos.Text, tipos.SinCompresion, tipos.Ninguna)
	require.NoError(t, err)
	require.Len(t, resultado, len(mediciones))

	for i, m := range mediciones {
		assert.Equal(t, m.Tiempo, resultado[i].Tiempo)
		assert.Equal(t, m.Valor, resultado[i].Valor)
	}
}

func TestDescomprimirBloqueSerie_ErrorCompresionNoSoportada(t *testing.T) {
	mediciones := []tipos.Medicion{
		{Tiempo: 1000000000, Valor: int64(100)},
	}

	// Crear bloque con DeltaDelta pero intentar descomprimir con Xor (no soportado para Integer)
	bloque := crearBloqueComprimido(t, mediciones, tipos.Integer, tipos.DeltaDelta, tipos.LZ4)

	_, err := DescomprimirBloqueSerie(bloque, tipos.Integer, tipos.Xor, tipos.LZ4)
	assert.Error(t, err)
}
