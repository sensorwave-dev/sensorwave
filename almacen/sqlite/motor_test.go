package sqlite

import (
	"math"
	"testing"

	"github.com/sensorwave-dev/sensorwave/almacen"
	"github.com/stretchr/testify/require"
)

func TestCerrarBloque_PuntoIntermedioPermanece(t *testing.T) {
	db, err := Abrir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { db.Cerrar() })

	for _, tiempo := range []int64{10, 30, 50} {
		require.NoError(t, db.AgregarPunto(almacen.Punto{
			SerieID: 1,
			Tiempo:  tiempo,
			Datos:   []byte{byte(tiempo)},
		}))
	}

	antiguos, err := db.PuntosIngestaAntiguos(1, 2)
	require.NoError(t, err)
	require.Len(t, antiguos, 2)
	tiempos := []int64{antiguos[0].Tiempo, antiguos[1].Tiempo}
	require.Equal(t, []int64{10, 30}, tiempos)

	require.NoError(t, db.AgregarPunto(almacen.Punto{
		SerieID: 1,
		Tiempo:  20,
		Datos:   []byte("medio"),
	}))

	require.NoError(t, db.CerrarBloque(almacen.Bloque{
		SerieID: 1,
		Inicio:  10,
		Fin:     30,
		Datos:   []byte("bloque"),
	}, tiempos))

	quedan, err := db.PuntosEnRango(1, math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	require.Len(t, quedan, 2)
	require.Equal(t, int64(20), quedan[0].Tiempo)
	require.Equal(t, []byte("medio"), quedan[0].Datos)
	require.Equal(t, int64(50), quedan[1].Tiempo)

	iter, err := db.BloquesEnRango(1, 10, 30)
	require.NoError(t, err)
	defer iter.Close()
	require.True(t, iter.Siguiente())
	bloque := iter.Bloque()
	require.Equal(t, int64(10), bloque.Inicio)
	require.Equal(t, int64(30), bloque.Fin)
	require.Equal(t, []byte("bloque"), bloque.Datos)
	require.False(t, iter.Siguiente())
}
