package servidor

import (
	"io"
	"log"
	"testing"
)

func init() {
	log.SetOutput(io.Discard)
}

// TestEsMensajeRebotado verifica la detección de mensajes que regresaron al origen
func TestEsMensajeRebotado(t *testing.T) {
	idLocal := obtenerIDInstancia()

	tests := []struct {
		name     string
		mensaje  Mensaje
		expected bool
	}{
		{
			name:     "mensaje sin origen no es rebotado",
			mensaje:  Mensaje{Topico: "sensores/temp", Origen: ""},
			expected: false,
		},
		{
			name:     "mensaje de origen remoto no es rebotado",
			mensaje:  Mensaje{Topico: "sensores/temp", Origen: "instancia-remota"},
			expected: false,
		},
		{
			name:     "mensaje con origen local SI es rebotado",
			mensaje:  Mensaje{Topico: "sensores/temp", Origen: idLocal},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := esMensajeRebotado(tt.mensaje)
			if got != tt.expected {
				t.Errorf("esMensajeRebotado() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestReenviarUpstream_SiReenviaLocal verifica que un mensaje originado localmente
// SÍ se reenvía upstream (reenviarUpstream solo bloquea mensajes de otra instancia)
func TestReenviarUpstream_SiReenviaLocal(t *testing.T) {
	mock := &upstreamMock{}
	ConfigurarUpstream(mock)
	defer ConfigurarUpstream(nil)

	m := Mensaje{
		Topico:   "sensores/temperatura",
		Payload:  []byte("25"),
		Origen:   obtenerIDInstancia(),
		Original: true,
	}

	reenviarUpstream(m)

	if mock.publicaciones.Load() != 1 {
		t.Errorf("reenviarUpstream no reenvió mensaje local; publicaciones = %d, want 1", mock.publicaciones.Load())
	}
}

// TestReenviarUpstream_NoReenviaRemoto verifica que un mensaje de origen remoto
// NO se reenvía upstream (protección contra bucles de reenvío)
func TestReenviarUpstream_NoReenviaRemoto(t *testing.T) {
	mock := &upstreamMock{}
	ConfigurarUpstream(mock)
	defer ConfigurarUpstream(nil)

	m := Mensaje{
		Topico:   "sensores/temperatura",
		Payload:  []byte("25"),
		Origen:   "instancia-remota",
		Original: true,
	}

	reenviarUpstream(m)

	if mock.publicaciones.Load() != 0 {
		t.Errorf("reenviarUpstream reenvió mensaje remoto; publicaciones = %d, want 0", mock.publicaciones.Load())
	}
}

// TestTopologiaSimetrica_NoDobleEntrega simula el flujo de federación simétrica:
// 1. Mensaje nace localmente (Origen = idLocal, Original = true)
// 2. Se distribuye localmente y se reenvía upstream por reenviarUpstream
// 3. El mensaje "regresa" del upstream (mismo Origen, Original = true)
// 4. esMensajeRebotado detecta que regresó al origen
// 5. Los manejadores de entrada lo descartan para evitar doble entrega
func TestTopologiaSimetrica_NoDobleEntrega(t *testing.T) {
	idLocal := obtenerIDInstancia()

	// Mensaje que "regresó" del upstream (como si la otra instancia lo reenvió)
	mensajeDeVuelta := Mensaje{
		Topico:   "sensores/temperatura",
		Payload:  []byte("25"),
		Origen:   idLocal,
		Original: true,
	}

	// Paso 4: esMensajeRebotado debe detectar que regresó al origen
	if !esMensajeRebotado(mensajeDeVuelta) {
		t.Fatal("esMensajeRebotado debería devolver true para mensaje que regresó al origen")
	}

	// Paso 5: en los manejadores de entrada, esMensajeRebotado evitaría distribuir
	// localmente otra vez. Este test no puede invocar directamente los manejadores
	// HTTP/MQTT/CoAP sin mocks de red, pero verifica la primitiva central.
}
