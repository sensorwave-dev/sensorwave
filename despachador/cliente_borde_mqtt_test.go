package despachador

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sensorwave-dev/sensorwave/tipos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mqttMessageFake struct {
	topic   string
	payload []byte
}

func (m mqttMessageFake) Duplicate() bool   { return false }
func (m mqttMessageFake) Qos() byte         { return 1 }
func (m mqttMessageFake) Retained() bool    { return false }
func (m mqttMessageFake) Topic() string     { return m.topic }
func (m mqttMessageFake) MessageID() uint16 { return 0 }
func (m mqttMessageFake) Payload() []byte   { return m.payload }
func (m mqttMessageFake) Ack()              {}

func TestConstruirTopicoConsultaCancelar(t *testing.T) {
	assert.Equal(t,
		"swctl/nodos/n1/consulta/cancelar/c1",
		tipos.ConstruirTopicoConsultaCancelar("n1", "c1"),
	)
}

func TestDeserializarResultadoParte(t *testing.T) {
	resultado := tipos.ResultadoConsultaRango{
		Series:  []string{"/s/temp"},
		Tiempos: []int64{1000},
		Valores: [][]any{{25.0}},
	}
	resultadoJSON, err := json.Marshal(resultado)
	require.NoError(t, err)

	parte := tipos.RespuestaControlConsultaParte{
		Version:       1,
		IDConsulta:    "c1",
		IDNodo:        "n1",
		IndiceParte:   0,
		EsUltimaParte: true,
		Resultado:     resultadoJSON,
	}
	raw, err := json.Marshal(parte)
	require.NoError(t, err)

	got, err := deserializarResultadoParte[tipos.ResultadoConsultaRango](raw)
	require.NoError(t, err)
	assert.Equal(t, []string{"/s/temp"}, got.Series)
	assert.Equal(t, []int64{1000}, got.Tiempos)
}

func TestDeserializarResultadoParte_SinEnvelope(t *testing.T) {
	raw, err := json.Marshal(tipos.ResultadoConsultaRango{Series: []string{"/s"}})
	require.NoError(t, err)

	_, err = deserializarResultadoParte[tipos.ResultadoConsultaRango](raw)
	assert.Error(t, err)
}

func TestManejarRespuesta_ParteFin(t *testing.T) {
	cb := &clienteBordeMQTT{
		idDespachador: "desp-1",
		consultas:     make(map[string]*consultaPendiente),
	}
	id := "consulta-1"
	consulta := &consultaPendiente{
		idConsulta: id,
		partes:     make([]json.RawMessage, 0),
		fin:        make(chan struct{}),
		error:      make(chan error, 1),
		timeout:    time.Second,
	}
	cb.consultas[id] = consulta

	parte := tipos.RespuestaControlConsultaParte{
		Version:    1,
		IDConsulta: id,
		Resultado:  json.RawMessage(`{"series":["/s"],"tiempos":[1],"valores":[[2]]}`),
	}
	parteBytes, err := json.Marshal(parte)
	require.NoError(t, err)

	cb.manejarRespuesta(nil, mqttMessageFake{
		topic:   tipos.ConstruirTopicoConsultaParte("desp-1", id, 0),
		payload: parteBytes,
	})

	select {
	case <-consulta.fin:
	case <-time.After(time.Second):
		t.Fatal("no se cerró fin")
	}

	require.Len(t, consulta.partes, 1)
	got, err := deserializarResultadoParte[tipos.ResultadoConsultaRango](consulta.partes[0])
	require.NoError(t, err)
	assert.Equal(t, []string{"/s"}, got.Series)
}

func TestManejarRespuesta_IgnoraOtroDespachador(t *testing.T) {
	cb := &clienteBordeMQTT{
		idDespachador: "desp-1",
		consultas:     make(map[string]*consultaPendiente),
	}
	id := "consulta-1"
	consulta := &consultaPendiente{
		idConsulta: id,
		partes:     make([]json.RawMessage, 0),
		fin:        make(chan struct{}),
		error:      make(chan error, 1),
	}
	cb.consultas[id] = consulta

	cb.manejarRespuesta(nil, mqttMessageFake{
		topic:   tipos.ConstruirTopicoConsultaParte("desp-otro", id, 0),
		payload: []byte(`{}`),
	})

	select {
	case <-consulta.fin:
		t.Fatal("no debió cerrar fin de otra réplica")
	default:
	}
}

func TestManejarRespuesta_Error(t *testing.T) {
	cb := &clienteBordeMQTT{
		idDespachador: "desp-1",
		consultas:     make(map[string]*consultaPendiente),
	}
	id := "consulta-err"
	consulta := &consultaPendiente{
		idConsulta: id,
		fin:        make(chan struct{}),
		error:      make(chan error, 1),
	}
	cb.consultas[id] = consulta

	errPayload, _ := json.Marshal(tipos.RespuestaControlConsultaError{
		Codigo:  "consulta_error",
		Mensaje: "serie no encontrada",
	})
	cb.manejarRespuesta(nil, mqttMessageFake{
		topic:   tipos.ConstruirTopicoConsultaError("desp-1", id),
		payload: errPayload,
	})

	select {
	case err := <-consulta.error:
		assert.Contains(t, err.Error(), "consulta_error")
		assert.Contains(t, err.Error(), "serie no encontrada")
	case <-time.After(time.Second):
		t.Fatal("no se recibió error")
	}
}

func TestManejarRespuesta_FinDuplicadoNoPanic(t *testing.T) {
	cb := &clienteBordeMQTT{
		idDespachador: "desp-1",
		consultas:     make(map[string]*consultaPendiente),
	}
	id := "consulta-fin"
	consulta := &consultaPendiente{
		idConsulta: id,
		fin:        make(chan struct{}),
		error:      make(chan error, 1),
	}
	cb.consultas[id] = consulta

	msg := mqttMessageFake{topic: tipos.ConstruirTopicoConsultaParte("desp-1", id, 0), payload: []byte(`{}`)}
	cb.manejarRespuesta(nil, msg)
	cb.manejarRespuesta(nil, msg)
}
