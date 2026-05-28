package middleware

// Tipo de callback
type CallbackFunc func(topico string, payload []byte)

// Cliente es la interfaz común para todos los clientes de protocolo
type Cliente interface {
	Desconectar()
	Publicar(topico string, mensaje interface{}, opciones ...PublicarOpcion)
	Suscribir(topico string, manejador CallbackFunc)
	Desuscribir(topico string)
}

// PublicarOpcion es una función que modifica un mensaje antes de enviarlo
type PublicarOpcion func(*Mensaje) error

// ConQoS crea una opción para especificar el QoS del mensaje (0 o 1)
func ConQoS(qos int) PublicarOpcion {
	return func(m *Mensaje) error {
		m.QoS = qos
		return nil
	}
}

// Mensaje representa un mensaje en el sistema
type Mensaje struct {
	Original  bool   `json:"original"`
	Topico    string `json:"topico"`
	Payload   []byte `json:"payload"`
	Interno   bool   `json:"interno"`
	QoS       int    `json:"qos,omitempty"`
	MensajeID string `json:"mensajeId,omitempty"`
	Origen    string `json:"origen,omitempty"`
}
