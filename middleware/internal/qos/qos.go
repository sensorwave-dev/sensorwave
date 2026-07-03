package qos

import (
	"math/rand/v2"
	"time"
)

// Temporizadores de retransmisión para QoS1, alineados con RFC 7252 (CoAP):
//   - ACK_TIMEOUT = 2s, aleatorizado a [ACK_TIMEOUT * (1 ± RANDOM_FACTOR)] -> [1.5s, 2.5s].
//   - MAX_RETRANSMIT = 4   - backoff exponencial ×2 por intento.
//
// Estas constantes son compartidas por el egress HTTP (servidor) y el ingreso
// HTTP (cliente), de modo que ambos extremos del handshake QoS1 usen ventanas
// de retransmisión isócronas. En CoAP el redelivery lo hace el stack del
// protocolo (mensajes Confirmable); el middleware no inventa retransmisiones
// de aplicación encima. En MQTT lo gestiona el broker embebido via PUBACK
// inflight nativo.
const (
	AckTimeout         = 2 * time.Second
	RandomFactor       = 0.5
	FactorBackoff      = 2
	MaxRetransmisiones = 4
)

// JitterAckTimeout devuelve el ACK_TIMEOUT aleatorizado dentro del rango
// RFC 7252: [AckTimeout * (1 - RandomFactor), AckTimeout * (1 + RandomFactor)]
// -> [1.5s, 2.5s].
func JitterAckTimeout() time.Duration {
	min := float64(AckTimeout) * (1 - RandomFactor)
	max := float64(AckTimeout) * (1 + RandomFactor)
	return time.Duration(min + rand.Float64()*(max-min))
}
