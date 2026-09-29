// Package pebble implementa almacen.Almacen sobre Pebble.
// Las claves son las que el borde ya escribía: un directorio existente se lee igual.
package pebble

import (
	"errors"
	"slices"
	"strings"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/sensorwave-dev/sensorwave/almacen"
)

const (
	claveNodoID   = "metadatos/nodo_id"
	claveTagsNodo = "metadatos/tags"
	claveContador = "metadatos/contador"
	prefijoSeries = "series/"
	cotaSeries    = "series0"
	prefijoReglas = "reglas/"
	cotaReglas    = "reglas0"
	prefijoPend   = "pendientes/eliminar/"
	cotaPend      = "pendientes/eliminar0"
)

type motor struct {
	db *pebbledb.DB
}

// Abrir abre o crea un directorio Pebble.
func Abrir(ruta string) (almacen.Almacen, error) {
	db, err := pebbledb.Open(ruta, &pebbledb.Options{})
	if err != nil {
		return nil, err
	}
	return &motor{db: db}, nil
}

func (m *motor) AgregarPunto(p almacen.Punto) error {
	return m.db.Set(claveIngesta(p.SerieID, p.Tiempo), p.Datos, pebbledb.Sync)
}

func (m *motor) PuntosIngestaAntiguos(serieID, n int) ([]almacen.Punto, error) {
	if n <= 0 {
		return nil, nil
	}
	desde, hasta := cotaIngesta(serieID)
	var puntos []almacen.Punto
	err := m.recorrer(desde, hasta, func(clave, valor []byte) error {
		if len(puntos) >= n {
			return errCorte
		}
		tiempo, ok := tiempoDeClaveIngesta(string(clave))
		if !ok {
			return nil
		}
		puntos = append(puntos, almacen.Punto{
			SerieID: serieID,
			Tiempo:  tiempo,
			Datos:   slices.Clone(valor),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return puntos, nil
}

func (m *motor) PuntosEnRango(serieID int, desdeT, hastaT int64) ([]almacen.Punto, error) {
	if desdeT > hastaT {
		return nil, nil
	}
	desde := claveIngesta(serieID, desdeT)
	hasta := append(claveIngesta(serieID, hastaT), '~')
	var puntos []almacen.Punto
	err := m.recorrer(desde, hasta, func(clave, valor []byte) error {
		tiempo, ok := tiempoDeClaveIngesta(string(clave))
		if !ok {
			return nil
		}
		puntos = append(puntos, almacen.Punto{
			SerieID: serieID,
			Tiempo:  tiempo,
			Datos:   slices.Clone(valor),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return puntos, nil
}

func (m *motor) UltimoPunto(serieID int) (almacen.Punto, bool, error) {
	desde, hasta := cotaIngesta(serieID)
	iter, err := m.db.NewIter(&pebbledb.IterOptions{
		LowerBound: desde,
		UpperBound: hasta,
	})
	if err != nil {
		return almacen.Punto{}, false, err
	}
	defer iter.Close()
	if !iter.Last() {
		return almacen.Punto{}, false, iter.Error()
	}
	tiempo, ok := tiempoDeClaveIngesta(string(iter.Key()))
	if !ok {
		return almacen.Punto{}, false, nil
	}
	return almacen.Punto{
		SerieID: serieID,
		Tiempo:  tiempo,
		Datos:   slices.Clone(iter.Value()),
	}, true, nil
}

func (m *motor) CerrarBloque(b almacen.Bloque, tiempos []int64) error {
	batch := m.db.NewBatch()
	defer batch.Close()
	if len(b.Datos) > 0 {
		if err := batch.Set(claveDatos(b.SerieID, b.Inicio, b.Fin), b.Datos, nil); err != nil {
			return err
		}
	}
	for _, tiempo := range tiempos {
		if err := batch.Delete(claveIngesta(b.SerieID, tiempo), nil); err != nil {
			return err
		}
	}
	return m.db.Apply(batch, pebbledb.Sync)
}

func (m *motor) BloquesEnRango(serieID int, desdeT, hastaT int64) (almacen.IterBloques, error) {
	desde, hasta := cotaDatos(serieID)
	var bloques []almacen.Bloque
	err := m.recorrer(desde, hasta, func(clave, valor []byte) error {
		inicio, fin, ok := parseClaveDatos(string(clave))
		if !ok {
			return nil
		}
		if fin < desdeT || inicio > hastaT {
			return nil
		}
		bloques = append(bloques, almacen.Bloque{
			SerieID: serieID,
			Inicio:  inicio,
			Fin:     fin,
			Datos:   slices.Clone(valor),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return almacen.NuevosBloques(bloques), nil
}

func (m *motor) BorrarBloque(serieID int, inicio, fin int64) error {
	return m.db.Delete(claveDatos(serieID, inicio, fin), pebbledb.Sync)
}

func (m *motor) BorrarDatosDeSerie(serieID int) error {
	claves, err := clavesDatosYIngesta(m, serieID)
	if err != nil {
		return err
	}
	return m.borrarClaves(claves)
}

func (m *motor) GuardarSerie(path string, valor []byte) error {
	return m.db.Set(claveSerie(path), valor, pebbledb.Sync)
}

func (m *motor) ObtenerSerie(path string) ([]byte, bool, error) {
	return m.obtener(claveSerie(path))
}

func (m *motor) ListarSeries() (almacen.IterClave, error) {
	return m.listarPrefijo(prefijoSeries, cotaSeries, prefijoSeries)
}

func (m *motor) BorrarSerie(path string, serieID int) error {
	claves, err := clavesDatosYIngesta(m, serieID)
	if err != nil {
		return err
	}
	claves = append(claves, slices.Clone(claveSerie(path)))
	return m.borrarClaves(claves)
}

func (m *motor) GuardarRegla(id string, valor []byte) error {
	return m.db.Set(claveRegla(id), valor, pebbledb.Sync)
}

func (m *motor) ObtenerRegla(id string) ([]byte, bool, error) {
	return m.obtener(claveRegla(id))
}

func (m *motor) ListarReglas() (almacen.IterClave, error) {
	return m.listarPrefijo(prefijoReglas, cotaReglas, prefijoReglas)
}

func (m *motor) BorrarRegla(id string) error {
	return m.db.Delete(claveRegla(id), pebbledb.Sync)
}

func (m *motor) GuardarNodoID(id string) error {
	return m.db.Set([]byte(claveNodoID), []byte(id), pebbledb.Sync)
}

func (m *motor) ObtenerNodoID() (string, bool, error) {
	valor, ok, err := m.obtener([]byte(claveNodoID))
	if err != nil || !ok {
		return "", ok, err
	}
	return string(valor), true, nil
}

func (m *motor) GuardarTagsNodo(valor []byte) error {
	if valor == nil {
		return m.db.Delete([]byte(claveTagsNodo), pebbledb.Sync)
	}
	return m.db.Set([]byte(claveTagsNodo), valor, pebbledb.Sync)
}

func (m *motor) ObtenerTagsNodo() ([]byte, bool, error) {
	return m.obtener([]byte(claveTagsNodo))
}

func (m *motor) GuardarContador(valor []byte) error {
	return m.db.Set([]byte(claveContador), valor, pebbledb.Sync)
}

func (m *motor) ObtenerContador() ([]byte, bool, error) {
	return m.obtener([]byte(claveContador))
}

func (m *motor) GuardarPendiente(serieID int, valor []byte) error {
	return m.db.Set(clavePendiente(serieID), valor, pebbledb.Sync)
}

func (m *motor) ListarPendientes() (almacen.IterPendiente, error) {
	var ids []int
	var vals [][]byte
	err := m.recorrer([]byte(prefijoPend), []byte(cotaPend), func(clave, valor []byte) error {
		id, ok := serieDeClavePendiente(string(clave))
		if !ok {
			return nil
		}
		ids = append(ids, id)
		vals = append(vals, slices.Clone(valor))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return almacen.NuevosPendientes(ids, vals), nil
}

func (m *motor) BorrarPendiente(serieID int) error {
	return m.db.Delete(clavePendiente(serieID), pebbledb.Sync)
}

func (m *motor) Cerrar() error {
	return m.db.Close()
}

func (m *motor) obtener(clave []byte) ([]byte, bool, error) {
	valor, closer, err := m.db.Get(clave)
	if err != nil {
		if errors.Is(err, pebbledb.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer closer.Close()
	return slices.Clone(valor), true, nil
}

func (m *motor) borrarClaves(claves [][]byte) error {
	if len(claves) == 0 {
		return nil
	}
	batch := m.db.NewBatch()
	defer batch.Close()
	for _, clave := range claves {
		if err := batch.Delete(clave, nil); err != nil {
			return err
		}
	}
	return m.db.Apply(batch, pebbledb.Sync)
}

func clavesDatosYIngesta(m *motor, serieID int) ([][]byte, error) {
	var claves [][]byte
	desdeI, hastaI := cotaIngesta(serieID)
	err := m.recorrer(desdeI, hastaI, func(clave, _ []byte) error {
		claves = append(claves, slices.Clone(clave))
		return nil
	})
	if err != nil {
		return nil, err
	}
	desdeD, hastaD := cotaDatos(serieID)
	err = m.recorrer(desdeD, hastaD, func(clave, _ []byte) error {
		claves = append(claves, slices.Clone(clave))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claves, nil
}

func (m *motor) listarPrefijo(desde, hasta, prefijo string) (almacen.IterClave, error) {
	var claves []string
	var vals [][]byte
	err := m.recorrer([]byte(desde), []byte(hasta), func(clave, valor []byte) error {
		nombre, ok := strings.CutPrefix(string(clave), prefijo)
		if !ok || nombre == "" {
			return nil
		}
		claves = append(claves, nombre)
		vals = append(vals, slices.Clone(valor))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return almacen.NuevasClaves(claves, vals), nil
}

// errCorte corta un recorrido cuando ya se juntaron los puntos pedidos.
var errCorte = errStop{}

type errStop struct{}

func (errStop) Error() string { return "corte" }

func (m *motor) recorrer(desde, hasta []byte, fn func(clave, valor []byte) error) error {
	iter, err := m.db.NewIter(&pebbledb.IterOptions{
		LowerBound: desde,
		UpperBound: hasta,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := fn(iter.Key(), iter.Value()); err != nil {
			if errors.Is(err, errCorte) {
				return nil
			}
			return err
		}
	}
	return iter.Error()
}
