package borde

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sensorwave-dev/sensorwave/tipos"
	"github.com/stretchr/testify/require"
)

func TestHandlerInsertar_JSONEntero(t *testing.T) {
	gestor, err := Crear(Opciones{
		NombreDB:  t.TempDir() + "/entero",
		Direccion: "127.0.0.1",
	})
	require.NoError(t, err)
	t.Cleanup(func() { gestor.Cerrar() })
	gestor.HabilitarMotorReglas(false)

	require.NoError(t, gestor.CrearSerie(tipos.Serie{
		Path:             "compresion/integer/contador/Bits/LZ4",
		TipoDatos:        tipos.Integer,
		TamañoBloque:     100,
		CompresionBytes:  tipos.Bits,
		CompresionBloque: tipos.LZ4,
	}))

	marca := time.Date(2016, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	cuerpo := `{"path":"compresion/integer/contador/Bits/LZ4","valor":7,"marca_tiempo":` + strconv.FormatInt(marca, 10) + `}`
	rec := httptest.NewRecorder()
	HandlerInsertar(gestor)(rec, httptest.NewRequest(http.MethodPost, "/api/insertar", strings.NewReader(cuerpo)))
	require.Equal(t, http.StatusOK, rec.Code)

	res, err := gestor.ConsultarRango("compresion/integer/contador/Bits/LZ4", time.Unix(0, marca), time.Unix(0, marca))
	require.NoError(t, err)
	require.Equal(t, []int64{marca}, res.Tiempos)
	require.Equal(t, int64(7), res.Valores[0][0])

	fraccion := `{"path":"compresion/integer/contador/Bits/LZ4","valor":1.5,"marca_tiempo":` + strconv.FormatInt(marca+1, 10) + `}`
	rec = httptest.NewRecorder()
	HandlerInsertar(gestor)(rec, httptest.NewRequest(http.MethodPost, "/api/insertar", strings.NewReader(fraccion)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerInsertar_BooleanoFalso(t *testing.T) {
	gestor, err := Crear(Opciones{
		NombreDB:  t.TempDir() + "/bool",
		Direccion: "127.0.0.1",
	})
	require.NoError(t, err)
	t.Cleanup(func() { gestor.Cerrar() })
	gestor.HabilitarMotorReglas(false)

	require.NoError(t, gestor.CrearSerie(tipos.Serie{
		Path:             "compresion/boolean/rachas/RLE/LZ4",
		TipoDatos:        tipos.Boolean,
		TamañoBloque:     100,
		CompresionBytes:  tipos.RLE,
		CompresionBloque: tipos.LZ4,
	}))

	marca := time.Date(2016, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	cuerpo := `{"path":"compresion/boolean/rachas/RLE/LZ4","valor":false,"marca_tiempo":` + strconv.FormatInt(marca, 10) + `}`
	rec := httptest.NewRecorder()
	HandlerInsertar(gestor)(rec, httptest.NewRequest(http.MethodPost, "/api/insertar", strings.NewReader(cuerpo)))
	require.Equal(t, http.StatusOK, rec.Code)

	res, err := gestor.ConsultarRango("compresion/boolean/rachas/RLE/LZ4", time.Unix(0, marca), time.Unix(0, marca))
	require.NoError(t, err)
	require.Equal(t, false, res.Valores[0][0])
}
