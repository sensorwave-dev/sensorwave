package compresor

import (
	"github.com/sensorwave-dev/sensorwave/tipos"
)

// CompresorBloque define la interfaz para compresores de bloques (nivel 2)
type CompresorBloque interface {
	Comprimir(datos []byte) ([]byte, error)
	Descomprimir(datos []byte) ([]byte, error)
}

// obtenerCompresorBloque factory para crear compresores de bloques
func ObtenerCompresorBloque(tipo tipos.TipoCompresionBloque) CompresorBloque {
	switch tipo {
	case tipos.LZ4:
		return &CompresorLZ4{}
	case tipos.ZSTD:
		return &CompresorZSTD{}
	case tipos.Snappy:
		return &CompresorSnappy{}
	case tipos.Gzip:
		return &CompresorGzip{}
	case tipos.SinCompresionBloque:
		return &CompresorBloqueNinguno{}
	default:
		return &CompresorBloqueNinguno{}
	}
}

// CompresorBloqueNinguno implementa sin compresión para bloques
type CompresorBloqueNinguno struct{}

func (c *CompresorBloqueNinguno) Comprimir(datos []byte) ([]byte, error) {
	return datos, nil
}

func (c *CompresorBloqueNinguno) Descomprimir(datos []byte) ([]byte, error) {
	return datos, nil
}
