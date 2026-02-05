package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

// RegistrarEnS3 registra el nodo, sus series y sus reglas en almacenamiento S3
// Esta función se llama cuando:
// - El nodo se crea por primera vez (en Crear)
// - Se agrega una nueva serie (en CrearSerie)
// - Se modifica una regla (en AgregarRegla, ActualizarRegla, EliminarRegla)
func (me *ManagerEdge) RegistrarEnS3() error {
	// Verificar que S3 esté configurado
	if clienteS3 == nil {
		return fmt.Errorf("S3 no está configurado")
	}

	// Obtener todas las series del cache
	me.cache.mu.RLock()
	series := make(map[string]tipos.Serie, len(me.cache.datos))
	for k, v := range me.cache.datos {
		series[k] = v
	}
	me.cache.mu.RUnlock()

	// Obtener todas las reglas del motor de reglas
	reglasMap := me.MotorReglas.ListarReglas()
	var reglas []tipos.Regla
	for _, regla := range reglasMap {
		// Convertir condiciones
		var condiciones []tipos.Condicion
		for _, c := range regla.Condiciones {
			condiciones = append(condiciones, tipos.Condicion{
				Path:          c.Path,
				VentanaT:      c.VentanaT.String(),
				Agregacion:    string(c.Agregacion),
				Operador:      string(c.Operador),
				Valor:         c.Valor,
				AgregarSeries: c.AgregarSeries,
			})
		}

		// Convertir acciones
		var acciones []tipos.Accion
		for _, a := range regla.Acciones {
			acciones = append(acciones, tipos.Accion{
				Tipo:    a.Tipo,
				Destino: a.Destino,
				Params:  a.Params,
			})
		}

		reglas = append(reglas, tipos.Regla{
			ID:          regla.ID,
			Nombre:      regla.Nombre,
			Activa:      regla.Activa,
			Logica:      string(regla.Logica),
			Condiciones: condiciones,
			Acciones:    acciones,
		})
	}

	// Ordenar reglas por ID para consistencia
	sort.Slice(reglas, func(i, j int) bool {
		return reglas[i].ID < reglas[j].ID
	})

	// Crear estructura de registro del nodo
	registro := struct {
		NodoID    string                 `json:"nodo_id"`
		Direccion string                 `json:"direccion"`
		Series    map[string]tipos.Serie `json:"series"`
		Tags      map[string]string      `json:"tags,omitempty"`
		Reglas    []tipos.Regla          `json:"reglas,omitempty"`
	}{
		NodoID:    me.nodoID,
		Direccion: me.direccion,
		Series:    series,
		Tags:      me.tags,
		Reglas:    reglas,
	}

	// Serializar a JSON
	registroJSON, err := json.Marshal(registro)
	if err != nil {
		return fmt.Errorf("error al serializar registro de nodo: %v", err)
	}

	// Subir a S3 como objeto
	// Formato de la clave: nodos/<nodoID>.json
	nombreArchivo := fmt.Sprintf("nodos/%s.json", me.nodoID)

	ctx := context.TODO()
	_, err = clienteS3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(configuracionS3.Bucket),
		Key:         aws.String(nombreArchivo),
		Body:        bytes.NewReader(registroJSON),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("error al registrar nodo en S3: %v", err)
	}

	log.Printf("Nodo %s registrado exitosamente en S3 con %d series y %d reglas", me.nodoID, len(series), len(reglas))
	return nil
}

// ============================================================================
// SERVIDOR HTTP REST PARA COMUNICACIÓN CON DESPACHADORES
// ============================================================================

// authMiddleware es un middleware que verifica el token de autorización
func authMiddleware(nodoID string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := r.Header.Get("Authorization")
			expected := "Bearer " + nodoID

			fmt.Println("Token recibido:", token)
			fmt.Println("Token esperado:", expected)

			if token != expected {
				http.Error(w, "No autorizado", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// iniciarServidorHTTP inicia el servidor HTTP con los endpoints REST para consultas
func (me *ManagerEdge) iniciarServidorHTTP() chan struct{} {
	listo := make(chan struct{})

	mux := http.NewServeMux()

	// Registrar handlers REST para consultas del despachador
	mux.HandleFunc("/api/consulta/rango", me.handleConsultaRango)
	mux.HandleFunc("/api/consulta/ultimo", me.handleConsultaUltimo)
	mux.HandleFunc("/api/consulta/agregacion", me.handleConsultaAgregacion)
	mux.HandleFunc("/api/consulta/agregacion-temporal", me.handleConsultaAgregacionTemporal)

	log.Println("Iniciando servidor HTTP para", me.nodoID, "en puerto", me.puertoHTTP)
	server := &http.Server{
		Addr:         "0.0.0.0:" + me.puertoHTTP,
		Handler:      authMiddleware(me.nodoID)(mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		log.Printf("Edge %s: servidor HTTP iniciando en puerto %s", me.nodoID, me.puertoHTTP)
		close(listo)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Error en servidor HTTP: %v", err)
		}
	}()

	return listo
}

// handleConsultaRango maneja consultas de rango de tiempo via REST
func (me *ManagerEdge) handleConsultaRango(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	// Leer body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		enviarRespuestaError(w, "Error leyendo body: "+err.Error())
		return
	}
	defer r.Body.Close()

	// Deserializar solicitud con Gob
	var solicitud tipos.SolicitudConsultaRango
	if err := tipos.DeserializarGob(body, &solicitud); err != nil {
		enviarRespuestaError(w, "Error deserializando solicitud: "+err.Error())
		return
	}

	// Ejecutar consulta
	tiempoInicio := time.Unix(0, solicitud.TiempoInicio)
	tiempoFin := time.Unix(0, solicitud.TiempoFin)

	resultado, err := me.ConsultarRango(solicitud.Serie, tiempoInicio, tiempoFin)

	// Construir respuesta
	respuesta := tipos.RespuestaConsultaRango{
		Resultado: resultado,
	}
	if err != nil {
		respuesta.Error = err.Error()
	}

	// Serializar y enviar respuesta
	enviarRespuestaGob(w, respuesta)
}

// handleConsultaUltimo maneja consultas del último punto via REST
func (me *ManagerEdge) handleConsultaUltimo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	// Leer body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		enviarRespuestaError(w, "Error leyendo body: "+err.Error())
		return
	}
	defer r.Body.Close()

	// Deserializar solicitud con Gob
	var solicitud tipos.SolicitudConsultaPunto
	if err := tipos.DeserializarGob(body, &solicitud); err != nil {
		enviarRespuestaError(w, "Error deserializando solicitud: "+err.Error())
		return
	}

	// Convertir tiempos opcionales a *time.Time
	var tiempoInicio, tiempoFin *time.Time
	if solicitud.TiempoInicio != nil {
		t := time.Unix(0, *solicitud.TiempoInicio)
		tiempoInicio = &t
	}
	if solicitud.TiempoFin != nil {
		t := time.Unix(0, *solicitud.TiempoFin)
		tiempoFin = &t
	}

	// Ejecutar consulta
	resultado, err := me.ConsultarUltimoPunto(solicitud.Serie, tiempoInicio, tiempoFin)

	// Construir respuesta
	respuesta := tipos.RespuestaConsultaPunto{
		Resultado: resultado,
	}
	if err != nil {
		respuesta.Error = err.Error()
	}

	// Serializar y enviar respuesta
	enviarRespuestaGob(w, respuesta)
}

// enviarRespuestaGob serializa y envía una respuesta usando Gob
func enviarRespuestaGob(w http.ResponseWriter, respuesta interface{}) {
	respuestaBytes, err := tipos.SerializarGob(respuesta)
	if err != nil {
		http.Error(w, "Error serializando respuesta", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	w.Write(respuestaBytes)
}

// enviarRespuestaError envía una respuesta de error
func enviarRespuestaError(w http.ResponseWriter, mensaje string) {
	log.Printf("Error en handler: %s", mensaje)
	http.Error(w, mensaje, http.StatusBadRequest)
}

// handleConsultaAgregacion maneja consultas de agregación simple via REST
func (me *ManagerEdge) handleConsultaAgregacion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	// Leer body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		enviarRespuestaError(w, "Error leyendo body: "+err.Error())
		return
	}
	defer r.Body.Close()

	// Deserializar solicitud con Gob
	var solicitud tipos.SolicitudConsultaAgregacion
	if err := tipos.DeserializarGob(body, &solicitud); err != nil {
		enviarRespuestaError(w, "Error deserializando solicitud: "+err.Error())
		return
	}

	// Ejecutar consulta
	tiempoInicio := time.Unix(0, solicitud.TiempoInicio)
	tiempoFin := time.Unix(0, solicitud.TiempoFin)

	resultado, err := me.ConsultarAgregacion(solicitud.Serie, tiempoInicio, tiempoFin, solicitud.Agregaciones)

	// Construir respuesta
	respuesta := tipos.RespuestaConsultaAgregacion{
		Resultado: resultado,
	}
	if err != nil {
		respuesta.Error = err.Error()
	}

	// Serializar y enviar respuesta
	enviarRespuestaGob(w, respuesta)
}

// handleConsultaAgregacionTemporal maneja consultas de downsampling via REST
func (me *ManagerEdge) handleConsultaAgregacionTemporal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	// Leer body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		enviarRespuestaError(w, "Error leyendo body: "+err.Error())
		return
	}
	defer r.Body.Close()

	// Deserializar solicitud con Gob
	var solicitud tipos.SolicitudConsultaAgregacionTemporal
	if err := tipos.DeserializarGob(body, &solicitud); err != nil {
		enviarRespuestaError(w, "Error deserializando solicitud: "+err.Error())
		return
	}

	// Ejecutar consulta
	tiempoInicio := time.Unix(0, solicitud.TiempoInicio)
	tiempoFin := time.Unix(0, solicitud.TiempoFin)
	intervalo := time.Duration(solicitud.Intervalo)

	resultado, err := me.ConsultarAgregacionTemporal(solicitud.Serie, tiempoInicio, tiempoFin, solicitud.Agregaciones, intervalo)

	// Construir respuesta
	respuesta := tipos.RespuestaConsultaAgregacionTemporal{
		Resultado: resultado,
	}
	if err != nil {
		respuesta.Error = err.Error()
	}

	// Serializar y enviar respuesta
	enviarRespuestaGob(w, respuesta)
}
