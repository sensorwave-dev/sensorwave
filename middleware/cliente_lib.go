package middleware

type CallbackFunc func(topico string, payload interface{})

type Cliente interface {
	Desconectar()
	Publicar(topico string, mensaje interface{}, opciones ...PublicarOpcion)
	Suscribir(topico string, manejador CallbackFunc)
	Desuscribir(topico string)
}

type PublicarOpcion func(*Mensaje) error

func ConQoS(qos int) PublicarOpcion {
	return func(m *Mensaje) error {
		m.QoS = qos
		return nil
	}
}

// almacena si el mensaje es original o replica
type Mensaje struct {
	Original  bool   `json:"original"`
	Topico    string `json:"topico"`
	Payload   []byte `json:"payload"`
	Interno   bool   `json:"interno"`
	QoS       int    `json:"qos,omitempty"`
	MessageID string `json:"messageId,omitempty"`
}
