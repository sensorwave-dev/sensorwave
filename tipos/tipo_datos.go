package tipos

import (
	"fmt"
)

// TipoDatos representa el tipo de datos de una serie temporal
type TipoDatos struct {
	valor string
}

func (td TipoDatos) String() string {
	return td.valor
}

// GobEncode implementa gob.GobEncoder para serialización
func (td TipoDatos) GobEncode() ([]byte, error) {
	return []byte(td.valor), nil
}

// GobDecode implementa gob.GobDecoder para deserialización
func (td *TipoDatos) GobDecode(data []byte) error {
	td.valor = string(data)
	return nil
}

// MarshalJSON implementa json.Marshaler para serialización JSON
func (td TipoDatos) MarshalJSON() ([]byte, error) {
	return []byte(`"` + td.valor + `"`), nil
}

// UnmarshalJSON implementa json.Unmarshaler para deserialización JSON
func (td *TipoDatos) UnmarshalJSON(data []byte) error {
	// Remover comillas del string JSON
	if len(data) >= 2 && data[0] == '"' && data[len(data)-1] == '"' {
		td.valor = string(data[1 : len(data)-1])
	} else {
		td.valor = string(data)
	}
	return nil
}

// Valores posibles para TipoDatos
var (
	Desconocido = TipoDatos{}          // Tipo de datos desconocido
	Boolean     = TipoDatos{"Boolean"} // Datos booleanos (true/false)
	Integer     = TipoDatos{"Integer"} // Enteros
	Real        = TipoDatos{"Real"}    // Números de punto flotante
	Text        = TipoDatos{"Text"}    // Cadenas de texto
)

// Sistema extensible de algoritmos de compresión por tipo de datos

// algoritmosPorTipo mapea cada tipo de datos con sus algoritmos compatibles
var algoritmosPorTipo = map[TipoDatos][]TipoCompresion{
	Boolean: {
		SinCompresionBytes, // Sin compresión
		RLE,                // Óptimo para secuencias de valores repetidos (true, true, false, false...)
	},
	Integer: {
		SinCompresionBytes, // Sin compresión
		DeltaDelta,         // Óptimo para series monótonas (IDs incrementales, contadores)
		RLE,                // Óptimo para valores repetidos
		Bits,               // Óptimo para rangos pequeños (0-100, estados enumerados)
	},
	Real: {
		SinCompresionBytes, // Sin compresión
		DeltaDelta,         // Bueno para series con tendencia lineal
		Xor,                // Óptimo para flotantes con cambios pequeños (sensores de temperatura)
		RLE,                // Para valores repetidos (poco común en flotantes)
	},
	Text: {
		SinCompresionBytes, // Sin compresión
		RLE,                // Para secuencias repetidas de strings
		Diccionario,        // Óptimo para vocabulario limitado (estados: "activo", "inactivo", "error")
	},
}

// AlgoritmosCompresion retorna los algoritmos de compresión válidos para este tipo de datos
func (td TipoDatos) AlgoritmosCompresion() []TipoCompresion {
	if algoritmos, existe := algoritmosPorTipo[td]; existe {
		return algoritmos
	}
	// Si no hay algoritmos registrados, retornar solo SinCompresionBytes
	return []TipoCompresion{SinCompresionBytes}
}

// ValidarCompresion verifica si un algoritmo es compatible con este tipo de datos
func (td TipoDatos) ValidarCompresion(alg TipoCompresion) error {
	algoritmos := td.AlgoritmosCompresion()
	for _, a := range algoritmos {
		if a == alg {
			return nil
		}
	}
	return fmt.Errorf("algoritmo de compresión '%s' no es válido para tipo de datos '%s' (algoritmos válidos: %v)",
		alg, td, algoritmos)
}
