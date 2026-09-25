// Package pebble implementa almacen.Motor sobre Pebble.
package pebble

import (
	"errors"
	"slices"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/sensorwave-dev/sensorwave/almacen"
)

type motor struct {
	db *pebbledb.DB
}

// Abrir abre o crea un directorio Pebble.
func Abrir(ruta string) (almacen.Motor, error) {
	db, err := pebbledb.Open(ruta, &pebbledb.Options{})
	if err != nil {
		return nil, err
	}
	return &motor{db: db}, nil
}

func (m *motor) Obtener(clave []byte) ([]byte, error) {
	valor, closer, err := m.db.Get(clave)
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil, almacen.ErrNoEncontrado
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return slices.Clone(valor), nil
}

func (m *motor) Poner(clave, valor []byte) error {
	return m.db.Set(clave, valor, pebbledb.Sync)
}

func (m *motor) Borrar(clave []byte) error {
	return m.db.Delete(clave, pebbledb.Sync)
}

func (m *motor) BorrarVarias(claves [][]byte) error {
	batch := m.db.NewBatch()
	defer batch.Close()
	for _, clave := range claves {
		if err := batch.Delete(clave, nil); err != nil {
			return err
		}
	}
	return m.db.Apply(batch, pebbledb.Sync)
}

func (m *motor) Recorrer(desde, hasta []byte) (almacen.Iterador, error) {
	iter, err := m.db.NewIter(&pebbledb.IterOptions{
		LowerBound: desde,
		UpperBound: hasta,
	})
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (m *motor) Cerrar() error {
	return m.db.Close()
}
