//go:build integration

package despachador

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/sensorwave-dev/sensorwave/borde"
	"github.com/sensorwave-dev/sensorwave/tipos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const brokerMQTTIntegration = "tcp://127.0.0.1:1883"

func asegurarNanoMQ(t *testing.T) {
	t.Helper()
	if puertoAbierto("127.0.0.1:1883") {
		return
	}

	script := filepath.Join("..", "contenedores", "iniciar_mqtt.sh")
	if _, err := os.Stat(script); err != nil {
		script = filepath.Join("contenedores", "iniciar_mqtt.sh")
	}
	cmd := exec.Command("bash", script)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run(), "no se pudo iniciar NanoMQ con contenedores/iniciar_mqtt.sh")

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if puertoAbierto("127.0.0.1:1883") {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("NanoMQ no quedó escuchando en :1883")
}

func puertoAbierto(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func crearBordeConFederacion(t *testing.T, broker string) *borde.GestorBorde {
	t.Helper()
	dir := t.TempDir()
	gestor, err := borde.Crear(borde.Opciones{
		NombreDB:  filepath.Join(dir, "borde.db"),
		Direccion: "localhost",
		ConfigS3:  nil,
	})
	require.NoError(t, err)
	require.NoError(t, gestor.ActivarFederacionMQTT(broker))
	t.Cleanup(func() { gestor.Cerrar() })
	return gestor
}

func TestIntegracionMQTT_ConsultasBasicas(t *testing.T) {
	asegurarNanoMQ(t)

	gestor := crearBordeConFederacion(t, brokerMQTTIntegration)
	serie := "sensor/temp"
	require.NoError(t, gestor.CrearSerie(tipos.Serie{
		Path:             serie,
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.SinCompresionBloque,
		CompresionBytes:  tipos.SinCompresionBytes,
	}))

	ahora := time.Now().UnixNano()
	for i := range 5 {
		require.NoError(t, gestor.Insertar(serie, ahora+int64(i)*int64(time.Second), float64(20+i)))
	}

	t.Setenv("SENSORWAVE_DESPACHADOR_ID", "desp-integ-1")
	cliente, err := nuevoClienteBordeMQTT(brokerMQTTIntegration)
	require.NoError(t, err)
	t.Cleanup(cliente.cerrar)

	// Dar tiempo a suscripciones MQTT
	time.Sleep(300 * time.Millisecond)

	ctx := t.Context()
	nodoID := gestor.ObtenerNodoID()

	rango, err := cliente.ConsultarRango(ctx, nodoID, "", tipos.SolicitudConsultaRango{
		Serie:        serie,
		TiempoInicio: ahora - int64(time.Second),
		TiempoFin:    ahora + 10*int64(time.Second),
	})
	require.NoError(t, err)
	require.NotNil(t, rango)
	assert.NotEmpty(t, rango.Resultado.Tiempos)

	ultimo, err := cliente.ConsultarUltimoPunto(ctx, nodoID, "", tipos.SolicitudConsultaPunto{Serie: serie})
	require.NoError(t, err)
	require.Len(t, ultimo.Resultado.Series, 1)
	assert.Equal(t, serie, ultimo.Resultado.Series[0])

	agreg, err := cliente.ConsultarAgregacion(ctx, nodoID, "", tipos.SolicitudConsultaAgregacion{
		Serie:        serie,
		TiempoInicio: ahora - int64(time.Second),
		TiempoFin:    ahora + 10*int64(time.Second),
		Agregaciones: []tipos.TipoAgregacion{tipos.AgregacionPromedio, tipos.AgregacionMaximo},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, agreg.Resultado.Valores)

	agregT, err := cliente.ConsultarAgregacionTemporal(ctx, nodoID, "", tipos.SolicitudConsultaAgregacionTemporal{
		Serie:        serie,
		TiempoInicio: ahora - int64(time.Second),
		TiempoFin:    ahora + 10*int64(time.Second),
		Agregaciones: []tipos.TipoAgregacion{tipos.AgregacionPromedio},
		Intervalo:    int64(2 * time.Second),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, agregT.Resultado.Valores)
}

func TestIntegracionMQTT_DosDespachadoresAislados(t *testing.T) {
	asegurarNanoMQ(t)

	gestor := crearBordeConFederacion(t, brokerMQTTIntegration)
	serie := "sensor/humedad"
	require.NoError(t, gestor.CrearSerie(tipos.Serie{
		Path:             serie,
		TipoDatos:        tipos.Real,
		TamañoBloque:     100,
		CompresionBloque: tipos.SinCompresionBloque,
		CompresionBytes:  tipos.SinCompresionBytes,
	}))
	ts := time.Now().UnixNano()
	require.NoError(t, gestor.Insertar(serie, ts, 55.5))

	t.Setenv("SENSORWAVE_DESPACHADOR_ID", "desp-a")
	clienteA, err := nuevoClienteBordeMQTT(brokerMQTTIntegration)
	require.NoError(t, err)
	t.Cleanup(clienteA.cerrar)

	t.Setenv("SENSORWAVE_DESPACHADOR_ID", "desp-b")
	clienteB, err := nuevoClienteBordeMQTT(brokerMQTTIntegration)
	require.NoError(t, err)
	t.Cleanup(clienteB.cerrar)

	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	nodoID := gestor.ObtenerNodoID()

	type resultado struct {
		id  string
		err error
		ok  bool
	}
	ch := make(chan resultado, 2)

	go func() {
		r, err := clienteA.ConsultarUltimoPunto(ctx, nodoID, "", tipos.SolicitudConsultaPunto{Serie: serie})
		ch <- resultado{id: "a", err: err, ok: err == nil && len(r.Resultado.Series) == 1}
	}()
	go func() {
		r, err := clienteB.ConsultarUltimoPunto(ctx, nodoID, "", tipos.SolicitudConsultaPunto{Serie: serie})
		ch <- resultado{id: "b", err: err, ok: err == nil && len(r.Resultado.Series) == 1}
	}()

	for range 2 {
		res := <-ch
		require.NoError(t, res.err, "despachador %s", res.id)
		assert.True(t, res.ok, "despachador %s sin datos", res.id)
	}
}

func TestIntegracionMQTT_BrokerInalcanzable(t *testing.T) {
	// ConnectTimeout del cliente es 5s; no debe colgar el suite.
	_, err := nuevoClienteBordeMQTT("tcp://127.0.0.1:1")
	assert.Error(t, err)
}

func TestIntegracionMQTT_CancelPublicaAlTimeout(t *testing.T) {
	asegurarNanoMQ(t)

	nodoID := "nodo-fantasma-" + uuid.New().String()
	cancelCh := make(chan string, 1)

	opts := mqtt.NewClientOptions().
		AddBroker(brokerMQTTIntegration).
		SetClientID("sw-spy-cancel-" + uuid.New().String())
	spy := mqtt.NewClient(opts)
	require.True(t, spy.Connect().WaitTimeout(5*time.Second))
	t.Cleanup(func() { spy.Disconnect(250) })

	filtro := "swctl/nodos/" + nodoID + "/consulta/cancelar/+"
	token := spy.Subscribe(filtro, 1, func(_ mqtt.Client, msg mqtt.Message) {
		partes := splitTopic(msg.Topic())
		if len(partes) == 6 && partes[4] == "cancelar" {
			select {
			case cancelCh <- partes[5]:
			default:
			}
		}
	})
	require.NoError(t, token.Error())
	require.True(t, token.WaitTimeout(5*time.Second))

	t.Setenv("SENSORWAVE_DESPACHADOR_ID", "desp-cancel-timeout")
	cliente, err := nuevoClienteBordeMQTT(brokerMQTTIntegration)
	require.NoError(t, err)
	t.Cleanup(cliente.cerrar)

	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()

	_, err = cliente.ConsultarUltimoPunto(ctx, nodoID, "", tipos.SolicitudConsultaPunto{Serie: "s"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")

	select {
	case id := <-cancelCh:
		assert.NotEmpty(t, id)
	case <-time.After(3 * time.Second):
		t.Fatal("no se recibió publicación de cancelación tras el timeout")
	}
}

func TestIntegracionMQTT_CancelAntesDeSolicitudNoResponde(t *testing.T) {
	asegurarNanoMQ(t)

	gestor := crearBordeConFederacion(t, brokerMQTTIntegration)
	nodoID := gestor.ObtenerNodoID()
	idDespachador := "desp-cancel-pre"
	idConsulta := uuid.New().String()

	var mu sync.Mutex
	var respuestas []string
	opts := mqtt.NewClientOptions().
		AddBroker(brokerMQTTIntegration).
		SetClientID("sw-spy-resp-" + uuid.New().String())
	spy := mqtt.NewClient(opts)
	require.True(t, spy.Connect().WaitTimeout(5*time.Second))
	t.Cleanup(func() { spy.Disconnect(250) })

	filtro := tipos.ConstruirTopicoRespuestasDespachador(idDespachador)
	token := spy.Subscribe(filtro, 1, func(_ mqtt.Client, msg mqtt.Message) {
		mu.Lock()
		respuestas = append(respuestas, msg.Topic())
		mu.Unlock()
	})
	require.NoError(t, token.Error())
	require.True(t, token.WaitTimeout(5*time.Second))

	pubOpts := mqtt.NewClientOptions().
		AddBroker(brokerMQTTIntegration).
		SetClientID("sw-pub-cancel-" + uuid.New().String())
	pub := mqtt.NewClient(pubOpts)
	require.True(t, pub.Connect().WaitTimeout(5*time.Second))
	t.Cleanup(func() { pub.Disconnect(250) })

	time.Sleep(300 * time.Millisecond)

	topicoCancel := tipos.ConstruirTopicoConsultaCancelar(nodoID, idConsulta)
	require.True(t, pub.Publish(topicoCancel, 1, false, []byte{}).WaitTimeout(5*time.Second))
	// Esperar a que el borde registre la cancelación antes de la solicitud
	// (tópicos distintos: el orden de entrega no está garantizado sin esta pausa).
	time.Sleep(300 * time.Millisecond)

	solicitud := tipos.SolicitudControlConsulta{
		Version:       1,
		IDConsulta:    idConsulta,
		IDNodo:        nodoID,
		IDDespachador: idDespachador,
		TipoConsulta:  tipos.ConsultaUltimo,
		Argumentos:    tipos.ConsultaArgs{Serie: "serie/inexistente"},
	}
	payload, err := json.Marshal(solicitud)
	require.NoError(t, err)
	topicoSol := tipos.ConstruirTopicoConsultaSolicitud(nodoID, idConsulta)
	require.True(t, pub.Publish(topicoSol, 1, false, payload).WaitTimeout(5*time.Second))

	time.Sleep(800 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, respuestas, "el borde no debe responder una consulta ya cancelada")
}
