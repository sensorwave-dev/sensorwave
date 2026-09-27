package despachador

import (
	"context"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type tiemposKey struct{}

// tiempos acumula, en el reloj del proceso, la espera MQTT, la lectura de
// objetos y el armado de la respuesta. Las consultas de una serie hacen
// esas etapas una después de la otra.
type tiempos struct {
	mqtt  atomic.Int64
	r2    atomic.Int64
	union atomic.Int64
}

func nuevoTiempos(ctx context.Context) (context.Context, *tiempos) {
	t := &tiempos{}
	return context.WithValue(ctx, tiemposKey{}, t), t
}

func tiemposDe(ctx context.Context) *tiempos {
	t, _ := ctx.Value(tiemposKey{}).(*tiempos)
	return t
}

func (t *tiempos) sumarMQTT(d time.Duration) {
	if t == nil {
		return
	}
	t.mqtt.Add(int64(d))
}

func (t *tiempos) sumarR2(d time.Duration) {
	if t == nil {
		return
	}
	t.r2.Add(int64(d))
}

func (t *tiempos) sumarUnion(d time.Duration) {
	if t == nil {
		return
	}
	t.union.Add(int64(d))
}

func medirSolicitud(r *http.Request) (context.Context, *tiempos) {
	if r.Header.Get("X-Sensorwave-Medir") != "1" {
		return r.Context(), nil
	}
	return nuevoTiempos(r.Context())
}

func escribirTiempos(w http.ResponseWriter, t *tiempos) {
	if t == nil {
		return
	}
	w.Header().Set("X-Sensorwave-Mqtt-Ns", strconv.FormatInt(t.mqtt.Load(), 10))
	w.Header().Set("X-Sensorwave-R2-Ns", strconv.FormatInt(t.r2.Load(), 10))
	w.Header().Set("X-Sensorwave-Union-Ns", strconv.FormatInt(t.union.Load(), 10))
}
