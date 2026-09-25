// Package almacen define el contrato de clave-valor ordenada del borde.
// Un motor por nodo. La ingesta, los bloques, las series y las reglas usan las mismas operaciones.
package almacen

import "errors"

// ErrNoEncontrado indica que la clave no existe.
var ErrNoEncontrado = errors.New("clave no encontrada")

// Motor es el almacenamiento local del nodo.
// Poner y Borrar quedan durables. Recorrer incluye desde y excluye hasta.
// Key y Value del iterador solo valen hasta el siguiente movimiento.
type Motor interface {
	Obtener(clave []byte) ([]byte, error)
	Poner(clave, valor []byte) error
	Borrar(clave []byte) error
	BorrarVarias(claves [][]byte) error
	Recorrer(desde, hasta []byte) (Iterador, error)
	Cerrar() error
}

// Iterador recorre claves en orden lexicográfico.
type Iterador interface {
	First() bool
	Last() bool
	Next() bool
	Valid() bool
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}
