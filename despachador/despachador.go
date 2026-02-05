package despachador

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sensorwave-dev/sensorwave/compresor"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

type ManagerDespachador struct {
	nodos       map[string]*tipos.Nodo
	mu          sync.RWMutex
	s3          tipos.ClienteS3
	config      tipos.ConfiguracionS3
	done        chan struct{}
	clienteEdge clienteEdge
}

// Opciones configura la creación de un ManagerDespachador.
// El despachador SIEMPRE requiere S3 para coordinar nodos.
type Opciones struct {
	ConfigS3 tipos.ConfiguracionS3 // Siempre requerido
}

// opcionesInternas extiende Opciones con campos para testing.
// No se exporta para evitar uso en producción.
type opcionesInternas struct {
	Opciones
	clienteS3   tipos.ClienteS3 // Para inyección en tests
	clienteEdge clienteEdge     // Para inyección en tests
}

// clienteEdge define la interfaz para comunicación con nodos edge.
// Es privada para evitar que usuarios externos inyecten implementaciones.
type clienteEdge interface {
	// ConsultarRango consulta mediciones en un rango de tiempo (formato tabular)
	ConsultarRango(ctx context.Context, nodoID string, direccion string, req tipos.SolicitudConsultaRango) (*tipos.RespuestaConsultaRango, error)

	// ConsultarUltimoPunto consulta el último punto de una o más series
	ConsultarUltimoPunto(ctx context.Context, nodoID string, direccion string, req tipos.SolicitudConsultaPunto) (*tipos.RespuestaConsultaPunto, error)

	// ConsultarAgregacion consulta múltiples agregaciones (promedio, min, max, etc.)
	ConsultarAgregacion(ctx context.Context, nodoID string, direccion string, req tipos.SolicitudConsultaAgregacion) (*tipos.RespuestaConsultaAgregacion, error)

	// ConsultarAgregacionTemporal consulta múltiples agregaciones agrupadas por intervalos (downsampling)
	ConsultarAgregacionTemporal(ctx context.Context, nodoID string, direccion string, req tipos.SolicitudConsultaAgregacionTemporal) (*tipos.RespuestaConsultaAgregacionTemporal, error)
}

// clienteEdgeHTTP implementa clienteEdge usando HTTP directo
type clienteEdgeHTTP struct {
	httpClient *http.Client
}

// nuevoClienteEdgeHTTP crea un nuevo cliente HTTP para comunicación con edges
func nuevoClienteEdgeHTTP() *clienteEdgeHTTP {
	return &clienteEdgeHTTP{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// ConsultarRango implementa clienteEdge
func (c *clienteEdgeHTTP) ConsultarRango(ctx context.Context, cliente string, direccion string, req tipos.SolicitudConsultaRango) (*tipos.RespuestaConsultaRango, error) {
	// Serializar solicitud con Gob
	solicitudBytes, err := tipos.SerializarGob(req)
	if err != nil {
		return nil, fmt.Errorf("error serializando solicitud: %v", err)
	}

	// Construir URL (la direccion ya incluye http://host)
	url := fmt.Sprintf("%s/api/consulta/rango", direccion)

	fmt.Println("solicitud a: " + url)
	// Crear request con contexto
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(solicitudBytes))
	if err != nil {
		return nil, fmt.Errorf("error creando request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/octet-stream")
	httpReq.Header.Set("Authorization", "Bearer "+cliente) // Usa el token

	// Ejecutar request
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("error en request HTTP: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error del edge (status %d): %s", resp.StatusCode, string(body))
	}

	// Leer respuesta
	respuestaBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error leyendo respuesta: %v", err)
	}

	// Deserializar respuesta
	var respuesta tipos.RespuestaConsultaRango
	if err := tipos.DeserializarGob(respuestaBytes, &respuesta); err != nil {
		return nil, fmt.Errorf("error deserializando respuesta: %v", err)
	}

	return &respuesta, nil
}

// ConsultarUltimoPunto implementa clienteEdge
func (c *clienteEdgeHTTP) ConsultarUltimoPunto(ctx context.Context, nodoID string, direccion string, req tipos.SolicitudConsultaPunto) (*tipos.RespuestaConsultaPunto, error) {
	// Serializar solicitud con Gob
	solicitudBytes, err := tipos.SerializarGob(req)
	if err != nil {
		return nil, fmt.Errorf("error serializando solicitud: %v", err)
	}

	// Construir URL
	url := fmt.Sprintf("%s/api/consulta/ultimo", direccion)

	fmt.Println("solicitud a: " + url)

	// Crear request con contexto
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(solicitudBytes))
	if err != nil {
		return nil, fmt.Errorf("error creando request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/octet-stream")
	httpReq.Header.Set("Authorization", "Bearer "+nodoID) // Usa el token

	// Ejecutar request
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("error en request HTTP: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error del edge (status %d): %s", resp.StatusCode, string(body))
	}

	// Leer respuesta
	respuestaBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error leyendo respuesta: %v", err)
	}

	// Deserializar respuesta
	var respuesta tipos.RespuestaConsultaPunto
	if err := tipos.DeserializarGob(respuestaBytes, &respuesta); err != nil {
		return nil, fmt.Errorf("error deserializando respuesta: %v", err)
	}

	return &respuesta, nil
}

// ConsultarAgregacion implementa clienteEdge
func (c *clienteEdgeHTTP) ConsultarAgregacion(ctx context.Context, nodoID string, direccion string, req tipos.SolicitudConsultaAgregacion) (*tipos.RespuestaConsultaAgregacion, error) {
	// Serializar solicitud con Gob
	solicitudBytes, err := tipos.SerializarGob(req)
	if err != nil {
		return nil, fmt.Errorf("error serializando solicitud: %v", err)
	}

	// Construir URL
	url := fmt.Sprintf("%s/api/consulta/agregacion", direccion)

	// Crear request con contexto
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(solicitudBytes))
	if err != nil {
		return nil, fmt.Errorf("error creando request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/octet-stream")
	httpReq.Header.Set("Authorization", "Bearer "+nodoID) // Usa el token

	// Ejecutar request
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("error en request HTTP: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error del edge (status %d): %s", resp.StatusCode, string(body))
	}

	// Leer respuesta
	respuestaBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error leyendo respuesta: %v", err)
	}

	// Deserializar respuesta
	var respuesta tipos.RespuestaConsultaAgregacion
	if err := tipos.DeserializarGob(respuestaBytes, &respuesta); err != nil {
		return nil, fmt.Errorf("error deserializando respuesta: %v", err)
	}

	return &respuesta, nil
}

// ConsultarAgregacionTemporal implementa clienteEdge
func (c *clienteEdgeHTTP) ConsultarAgregacionTemporal(ctx context.Context, nodoID string, direccion string, req tipos.SolicitudConsultaAgregacionTemporal) (*tipos.RespuestaConsultaAgregacionTemporal, error) {
	// Serializar solicitud con Gob
	solicitudBytes, err := tipos.SerializarGob(req)
	if err != nil {
		return nil, fmt.Errorf("error serializando solicitud: %v", err)
	}

	// Construir URL
	url := fmt.Sprintf("%s/api/consulta/agregacion-temporal", direccion)

	// Crear request con contexto
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(solicitudBytes))
	if err != nil {
		return nil, fmt.Errorf("error creando request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/octet-stream")
	httpReq.Header.Set("Authorization", "Bearer "+nodoID) // Usa el token

	// Ejecutar request
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("error en request HTTP: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error del edge (status %d): %s", resp.StatusCode, string(body))
	}

	// Leer respuesta
	respuestaBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error leyendo respuesta: %v", err)
	}

	// Deserializar respuesta
	var respuesta tipos.RespuestaConsultaAgregacionTemporal
	if err := tipos.DeserializarGob(respuestaBytes, &respuesta); err != nil {
		return nil, fmt.Errorf("error deserializando respuesta: %v", err)
	}

	return &respuesta, nil
}

// Crear inicializa y retorna un nuevo ManagerDespachador.
// El despachador SIEMPRE requiere una configuración de S3 válida para coordinar nodos.
func Crear(opts Opciones) (*ManagerDespachador, error) {
	return crearConOpciones(opcionesInternas{Opciones: opts})
}

// crearConOpciones es la función interna que permite inyectar dependencias para testing.
// No se exporta para evitar uso en producción.
func crearConOpciones(opts opcionesInternas) (*ManagerDespachador, error) {
	cfg := opts.ConfigS3

	// Usar cliente S3 inyectado o crear uno nuevo
	var s3Client tipos.ClienteS3
	if opts.clienteS3 != nil {
		s3Client = opts.clienteS3
	} else {
		// Crear cliente S3 usando la función centralizada
		var err error
		s3Client, err = tipos.CrearClienteS3(cfg)
		if err != nil {
			return nil, err
		}
	}

	// Verificar que el bucket existe, si no, intentar crearlo
	ctx := context.TODO()
	_, err := s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	if err != nil {
		log.Printf("El bucket %s no existe, intentando crearlo...", cfg.Bucket)
		_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(cfg.Bucket),
		})
		if err != nil {
			return nil, fmt.Errorf("error al crear bucket: %w", err)
		}
		log.Printf("Bucket %s creado exitosamente", cfg.Bucket)
	}

	// Usar cliente Edge inyectado o crear uno HTTP real
	var edgeClient clienteEdge
	if opts.clienteEdge != nil {
		edgeClient = opts.clienteEdge
	} else {
		edgeClient = nuevoClienteEdgeHTTP()
	}

	// Crear ManagerDespachador
	manager := &ManagerDespachador{
		s3:          s3Client,
		config:      cfg,
		nodos:       make(map[string]*tipos.Nodo),
		done:        make(chan struct{}),
		clienteEdge: edgeClient,
	}

	// Cargar nodos iniciales desde S3
	if err := manager.cargarNodosDesdeS3(); err != nil {
		log.Printf("Advertencia: no se pudieron cargar nodos iniciales: %v", err)
	}

	// Iniciar gorutina que sincroniza periódicamente los nodos
	go manager.monitorearNodos()

	log.Printf("Conectado a S3 en %s (bucket: %s)", cfg.Endpoint, cfg.Bucket)
	log.Printf("Despachador iniciado")
	return manager, nil
}

// Cerrar limpia los recursos del ManagerDespachador
func (m *ManagerDespachador) Cerrar() error {
	log.Printf("Cerrando despachador...")
	// Señalizar cierre
	close(m.done)
	log.Printf("Despachador cerrado exitosamente")
	return nil
}

// ListarNodos retorna una lista de nodos registrados
func (m *ManagerDespachador) ListarNodos() []tipos.Nodo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Crear una copia de los nodos para retornar
	nodosLista := make([]tipos.Nodo, 0, len(m.nodos))
	for _, nodo := range m.nodos {
		nodosLista = append(nodosLista, *nodo)
	}
	return nodosLista
}

// SerieInfo contiene información de una serie incluyendo el nodo al que pertenece
type SerieInfo struct {
	tipos.Serie
	NodoID string
}

// ListarSeries retorna todas las series de todos los nodos que coinciden con el patrón.
// Si patron es vacío o "*", retorna todas las series.
// Soporta wildcards (ej: "sensor/*", "*/temp").
func (m *ManagerDespachador) ListarSeries(patron string) []SerieInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if patron == "" {
		patron = "*"
	}

	var series []SerieInfo
	for _, nodo := range m.nodos {
		for path, serie := range nodo.Series {
			if tipos.MatchPath(path, patron) {
				series = append(series, SerieInfo{
					Serie:  serie,
					NodoID: nodo.NodoID,
				})
			}
		}
	}

	// Ordenar por path
	sort.Slice(series, func(i, j int) bool {
		return series[i].Path < series[j].Path
	})

	return series
}

// ObtenerSerie retorna la información de una serie específica.
// Retorna nil si la serie no existe.
func (m *ManagerDespachador) ObtenerSerie(path string) *SerieInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, nodo := range m.nodos {
		if serie, existe := nodo.Series[path]; existe {
			return &SerieInfo{
				Serie:  serie,
				NodoID: nodo.NodoID,
			}
		}
	}
	return nil
}

// ObtenerEstadisticas retorna estadísticas generales del despachador
type EstadisticasDespachador struct {
	NumNodos  int
	NumSeries int
	NumReglas int
}

func (m *ManagerDespachador) ObtenerEstadisticas() EstadisticasDespachador {
	m.mu.RLock()
	defer m.mu.RUnlock()

	numSeries := 0
	numReglas := 0
	for _, nodo := range m.nodos {
		numSeries += len(nodo.Series)
		numReglas += len(nodo.Reglas)
	}

	return EstadisticasDespachador{
		NumNodos:  len(m.nodos),
		NumSeries: numSeries,
		NumReglas: numReglas,
	}
}

// ReglaInfo contiene información de una regla incluyendo el nodo al que pertenece
type ReglaInfo struct {
	tipos.Regla
	NodoID string
}

// ListarReglas retorna todas las reglas de todos los nodos
// Filtros opcionales: nodoID (string vacío = todos), soloActivas (bool)
func (m *ManagerDespachador) ListarReglas(nodoID string, soloActivas bool) []ReglaInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var reglas []ReglaInfo

	for _, nodo := range m.nodos {
		// Filtrar por nodo si se especificó
		if nodoID != "" && nodo.NodoID != nodoID {
			continue
		}

		for _, regla := range nodo.Reglas {
			// Filtrar por activas si se especificó
			if soloActivas && !regla.Activa {
				continue
			}

			reglas = append(reglas, ReglaInfo{
				Regla:  regla,
				NodoID: nodo.NodoID,
			})
		}
	}

	return reglas
}

// ObtenerRegla busca una regla por su ID en todos los nodos
// Retorna la regla, el nodoID, y un bool indicando si se encontró
func (m *ManagerDespachador) ObtenerRegla(id string) (*tipos.Regla, string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, nodo := range m.nodos {
		for i := range nodo.Reglas {
			if nodo.Reglas[i].ID == id {
				return &nodo.Reglas[i], nodo.NodoID, true
			}
		}
	}

	return nil, "", false
}

// ListarReglasPorNodo retorna todas las reglas de un nodo específico
func (m *ManagerDespachador) ListarReglasPorNodo(nodoID string) []tipos.Regla {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodo, existe := m.nodos[nodoID]
	if !existe {
		return nil
	}

	// Retornar copia para evitar modificaciones externas
	reglas := make([]tipos.Regla, len(nodo.Reglas))
	copy(reglas, nodo.Reglas)

	return reglas
}

// monitorearNodos verifica periódicamente el estado de los nodos
func (m *ManagerDespachador) monitorearNodos() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			if err := m.cargarNodosDesdeS3(); err != nil {
				log.Printf("Error al cargar nodos desde S3: %v", err)
			}
		}
	}
}

// cargarNodosDesdeS3 sincroniza la lista de nodos con S3
func (m *ManagerDespachador) cargarNodosDesdeS3() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ctx := context.TODO()

	// Listar todos los objetos en el bucket con prefijo "nodos/"
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(m.config.Bucket),
		Prefix: aws.String("nodos/"),
	}

	result, err := m.s3.ListObjectsV2(ctx, input)
	if err != nil {
		return fmt.Errorf("error listando nodos desde S3: %v", err)
	}

	// Actualizar la lista de nodos en memoria
	nuevosNodos := make(map[string]*tipos.Nodo)

	for _, obj := range result.Contents {
		// Obtener el objeto completo
		getInput := &s3.GetObjectInput{
			Bucket: aws.String(m.config.Bucket),
			Key:    obj.Key,
		}

		getOutput, err := m.s3.GetObject(ctx, getInput)
		if err != nil {
			log.Printf("Error obteniendo nodo %s: %v", *obj.Key, err)
			continue
		}

		// Leer el contenido
		data, err := io.ReadAll(getOutput.Body)
		getOutput.Body.Close()
		if err != nil {
			log.Printf("Error leyendo nodo %s: %v", *obj.Key, err)
			continue
		}

		// Deserializar el nodo
		var nodo tipos.Nodo
		if err := json.Unmarshal(data, &nodo); err != nil {
			log.Printf("Error deserializando nodo %s: %v", *obj.Key, err)
			continue
		}

		nuevosNodos[nodo.NodoID] = &nodo
	}

	// Reemplazar la lista de nodos
	m.nodos = nuevosNodos

	if len(nuevosNodos) > 0 {
		log.Printf("Cargados %d nodos desde S3", len(nuevosNodos))
	}

	return nil
}

// ============================================================================
// CONSULTAS (S3 + Edge)
// ============================================================================

// ============================================================================
// HELPERS REUTILIZABLES
// ============================================================================

// consultarPuntoEdge consulta el último punto al edge con timeout
// Los tiempos son opcionales (nil = sin filtro temporal)
// Retorna el resultado columnar y error si hubo problemas
func (m *ManagerDespachador) consultarPuntoEdge(nodo tipos.Nodo, nombreSerie string, tiempoInicio, tiempoFin *time.Time, timeout time.Duration) (tipos.ResultadoConsultaPunto, error) {
	solicitud := tipos.SolicitudConsultaPunto{
		Serie: nombreSerie,
	}

	// Convertir tiempos opcionales a *int64
	if tiempoInicio != nil {
		t := tiempoInicio.UnixNano()
		solicitud.TiempoInicio = &t
	}
	if tiempoFin != nil {
		t := tiempoFin.UnixNano()
		solicitud.TiempoFin = &t
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	respuesta, err := m.clienteEdge.ConsultarUltimoPunto(ctx, nodo.NodoID, nodo.Direccion, solicitud)
	if err != nil {
		return tipos.ResultadoConsultaPunto{}, err
	}

	if respuesta.Error != "" {
		return tipos.ResultadoConsultaPunto{}, fmt.Errorf("error del edge: %s", respuesta.Error)
	}

	return respuesta.Resultado, nil
}

// descargarYDescomprimirBloque descarga un bloque de S3 y lo descomprime
func (m *ManagerDespachador) descargarYDescomprimirBloque(clave string, serie tipos.Serie) ([]tipos.Medicion, error) {
	ctx := context.TODO()
	getOutput, err := m.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(m.config.Bucket),
		Key:    aws.String(clave),
	})
	if err != nil {
		return nil, fmt.Errorf("error descargando bloque %s: %v", clave, err)
	}

	datosComprimidos, err := io.ReadAll(getOutput.Body)
	getOutput.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("error leyendo bloque %s: %v", clave, err)
	}

	mediciones, err := compresor.DescomprimirBloqueSerie(
		datosComprimidos,
		serie.TipoDatos,
		serie.CompresionBytes,
		serie.CompresionBloque,
	)
	if err != nil {
		return nil, fmt.Errorf("error descomprimiendo bloque %s: %v", clave, err)
	}

	return mediciones, nil
}

// listarBloquesEnRango lista los bloques de S3 que intersectan con el rango de tiempo dado
// Retorna las claves de los objetos S3 ordenadas por tiempo
func (m *ManagerDespachador) listarBloquesEnRango(nodoID string, serieID int, inicio, fin int64) ([]string, error) {
	ctx := context.TODO()

	// Prefijo para buscar bloques: <nodoID>/<serieID>_
	prefijo := tipos.GenerarPrefijoS3Serie(nodoID, serieID)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(m.config.Bucket),
		Prefix: aws.String(prefijo),
	}

	result, err := m.s3.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("error listando bloques desde S3: %v", err)
	}

	var bloquesEnRango []string

	for _, obj := range result.Contents {
		// Extraer tiempos del nombre del bloque usando función centralizada
		// Formato: <nodoID>/<serieID>_<tiempoInicio>_<tiempoFin>
		clave := *obj.Key
		_, bloqueInicio, bloqueFin, err := tipos.ParsearClaveS3Datos(clave)
		if err != nil {
			continue // Ignorar bloques con formato inválido
		}

		// Verificar si el bloque intersecta con el rango solicitado
		// Un bloque intersecta si: bloqueInicio <= fin AND bloqueFin >= inicio
		if bloqueInicio <= fin && bloqueFin >= inicio {
			bloquesEnRango = append(bloquesEnRango, clave)
		}
	}

	// Ordenar bloques por tiempo de inicio (el nombre incluye el tiempo con padding)
	sort.Strings(bloquesEnRango)

	return bloquesEnRango, nil
}

// consultarDatosS3 descarga y descomprime bloques de S3 en el rango especificado
// Usa 10 workers para descargas paralelas, sin timeout por bloque para no perder datos
func (m *ManagerDespachador) consultarDatosS3(nodo tipos.Nodo, serie tipos.Serie, inicio, fin int64) ([]tipos.Medicion, error) {
	// Listar bloques en el rango
	bloques, err := m.listarBloquesEnRango(nodo.NodoID, serie.SerieId, inicio, fin)
	if err != nil {
		return nil, err
	}

	if len(bloques) == 0 {
		return []tipos.Medicion{}, nil
	}

	const numWorkers = 10

	// Canal para distribuir trabajo
	bloqueChan := make(chan string, len(bloques))
	// Canal para recolectar resultados
	resultadoChan := make(chan struct {
		mediciones []tipos.Medicion
		err        error
	}, len(bloques))

	var wg sync.WaitGroup

	// Lanzar workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for clave := range bloqueChan {
				mediciones, err := m.descargarYDescomprimirBloque(clave, serie)
				resultadoChan <- struct {
					mediciones []tipos.Medicion
					err        error
				}{mediciones: mediciones, err: err}
			}
		}(i)
	}

	// Enviar bloques al canal de trabajo
	go func() {
		for _, clave := range bloques {
			bloqueChan <- clave
		}
		close(bloqueChan)
	}()

	// Cerrar canal de resultados cuando todos los workers terminen
	go func() {
		wg.Wait()
		close(resultadoChan)
	}()

	// Recolectar todos los resultados
	var todasMediciones []tipos.Medicion
	errores := 0
	for res := range resultadoChan {
		if res.err != nil {
			log.Printf("%v", res.err)
			errores++
			continue
		}
		// Filtrar mediciones dentro del rango exacto
		for _, med := range res.mediciones {
			if med.Tiempo >= inicio && med.Tiempo <= fin {
				todasMediciones = append(todasMediciones, med)
			}
		}
	}

	// Si todos los bloques fallaron, retornar error
	if errores == len(bloques) {
		return nil, fmt.Errorf("todos los bloques fallaron al descargar de S3")
	}

	return todasMediciones, nil
}

// consultarEdgeConTimeout consulta datos al edge con un timeout específico
// Retorna resultado vacío y nil si el edge no está disponible (timeout o error de conexión)
func (m *ManagerDespachador) consultarEdgeConTimeout(nodo tipos.Nodo, serie string, inicio, fin int64, timeout time.Duration) (tipos.ResultadoConsultaRango, error) {
	solicitud := tipos.SolicitudConsultaRango{
		Serie:        serie,
		TiempoInicio: inicio,
		TiempoFin:    fin,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	respuesta, err := m.clienteEdge.ConsultarRango(ctx, nodo.NodoID, nodo.Direccion, solicitud)
	if err != nil {
		// Timeout o error de conexión no es crítico, el edge puede estar offline
		log.Printf("Error consultando edge %s (serie: %s): %v", nodo.NodoID, serie, err)
		return tipos.ResultadoConsultaRango{}, nil
	}

	if respuesta.Error != "" {
		return tipos.ResultadoConsultaRango{}, fmt.Errorf("error del edge: %s", respuesta.Error)
	}

	return respuesta.Resultado, nil
}

// combinarResultadosTabular combina datos de S3 (mediciones) y edge (tabular) en formato tabular.
// Los datos del edge tienen prioridad en caso de duplicados de timestamp.
func (m *ManagerDespachador) combinarResultadosTabular(datosS3 []tipos.Medicion, datosEdge tipos.ResultadoConsultaRango, seriePath string) tipos.ResultadoConsultaRango {
	// Mapa para almacenar valores: timestamp -> valor
	valoresPorTiempo := make(map[int64]interface{})
	timestampsUnicos := make(map[int64]struct{})

	// Primero agregar datos de S3
	for _, m := range datosS3 {
		valoresPorTiempo[m.Tiempo] = m.Valor
		timestampsUnicos[m.Tiempo] = struct{}{}
	}

	// Luego agregar datos del edge (tienen prioridad)
	// El edge puede tener múltiples series, buscamos la que coincide con seriePath
	indiceColumna := -1
	for i, s := range datosEdge.Series {
		if s == seriePath {
			indiceColumna = i
			break
		}
	}

	if indiceColumna >= 0 {
		for filaIdx, tiempo := range datosEdge.Tiempos {
			if filaIdx < len(datosEdge.Valores) && indiceColumna < len(datosEdge.Valores[filaIdx]) {
				valor := datosEdge.Valores[filaIdx][indiceColumna]
				if valor != nil {
					valoresPorTiempo[tiempo] = valor
					timestampsUnicos[tiempo] = struct{}{}
				}
			}
		}
	}

	// Construir resultado tabular para esta serie única
	tiemposOrdenados := make([]int64, 0, len(timestampsUnicos))
	for t := range timestampsUnicos {
		tiemposOrdenados = append(tiemposOrdenados, t)
	}
	sort.Slice(tiemposOrdenados, func(i, j int) bool {
		return tiemposOrdenados[i] < tiemposOrdenados[j]
	})

	// Crear matriz de valores (una sola columna para esta serie)
	valores := make([][]interface{}, len(tiemposOrdenados))
	for i, tiempo := range tiemposOrdenados {
		valores[i] = []interface{}{valoresPorTiempo[tiempo]}
	}

	return tipos.ResultadoConsultaRango{
		Series:  []string{seriePath},
		Tiempos: tiemposOrdenados,
		Valores: valores,
	}
}

// combinarResultadosTabulares combina múltiples resultados tabulares en uno solo.
// Las series se ordenan alfabéticamente, los timestamps se unifican y ordenan ascendente.
func (m *ManagerDespachador) combinarResultadosTabulares(resultados []tipos.ResultadoConsultaRango) tipos.ResultadoConsultaRango {
	if len(resultados) == 0 {
		return tipos.ResultadoConsultaRango{}
	}

	// Recolectar todos los datos: serie -> timestamp -> valor
	datosPorSerie := make(map[string]map[int64]interface{})
	timestampsUnicos := make(map[int64]struct{})

	for _, res := range resultados {
		for colIdx, seriePath := range res.Series {
			if datosPorSerie[seriePath] == nil {
				datosPorSerie[seriePath] = make(map[int64]interface{})
			}
			for filaIdx, tiempo := range res.Tiempos {
				if filaIdx < len(res.Valores) && colIdx < len(res.Valores[filaIdx]) {
					valor := res.Valores[filaIdx][colIdx]
					if valor != nil {
						datosPorSerie[seriePath][tiempo] = valor
						timestampsUnicos[tiempo] = struct{}{}
					}
				}
			}
		}
	}

	// Extraer y ordenar series alfabéticamente
	seriesOrdenadas := make([]string, 0, len(datosPorSerie))
	for path := range datosPorSerie {
		seriesOrdenadas = append(seriesOrdenadas, path)
	}
	sort.Strings(seriesOrdenadas)

	// Extraer y ordenar timestamps
	tiemposOrdenados := make([]int64, 0, len(timestampsUnicos))
	for t := range timestampsUnicos {
		tiemposOrdenados = append(tiemposOrdenados, t)
	}
	sort.Slice(tiemposOrdenados, func(i, j int) bool {
		return tiemposOrdenados[i] < tiemposOrdenados[j]
	})

	// Construir matriz de valores
	valores := make([][]interface{}, len(tiemposOrdenados))
	for i, tiempo := range tiemposOrdenados {
		valores[i] = make([]interface{}, len(seriesOrdenadas))
		for j, path := range seriesOrdenadas {
			if valoresSerie, existe := datosPorSerie[path]; existe {
				if valor, tieneValor := valoresSerie[tiempo]; tieneValor {
					valores[i][j] = valor
				}
			}
		}
	}

	return tipos.ResultadoConsultaRango{
		Series:  seriesOrdenadas,
		Tiempos: tiemposOrdenados,
		Valores: valores,
	}
}

// ConsultarRango consulta datos combinando S3 (histórico) y edge (reciente).
// Esta función funciona incluso si el edge está offline (corte de luz/internet).
// Soporta wildcards en el path de la serie (ej: */temp, sensor_01/*).
// Retorna resultado en formato tabular.
func (m *ManagerDespachador) ConsultarRango(nombreSerie string, tiempoInicio, tiempoFin time.Time) (tipos.ResultadoConsultaRango, error) {
	// Buscar todas las series que coincidan (path exacto o wildcard)
	seriesEncontradas, err := m.buscarSeriesPorPath(nombreSerie)
	if err != nil {
		return tipos.ResultadoConsultaRango{}, err
	}

	inicio := tiempoInicio.UnixNano()
	fin := tiempoFin.UnixNano()

	// Canal para recoger resultados de todas las consultas
	type resultadoSerie struct {
		resultado tipos.ResultadoConsultaRango
		errS3     error
		errEdge   error
		path      string
		nodoID    string
	}
	resultados := make(chan resultadoSerie, len(seriesEncontradas))

	// Consultar cada serie en paralelo (S3 + edge)
	for _, sn := range seriesEncontradas {
		go func(sn serieConNodo) {
			var datosS3 []tipos.Medicion
			var datosEdge tipos.ResultadoConsultaRango
			var errS3, errEdge error

			// Consultar S3
			datosS3, errS3 = m.consultarDatosS3(sn.nodo, sn.serie, inicio, fin)

			// Consultar edge
			datosEdge, errEdge = m.consultarEdgeConTimeout(sn.nodo, sn.path, inicio, fin, 5*time.Second)

			resultados <- resultadoSerie{
				resultado: m.combinarResultadosTabular(datosS3, datosEdge, sn.path),
				errS3:     errS3,
				errEdge:   errEdge,
				path:      sn.path,
				nodoID:    sn.nodo.NodoID,
			}
		}(sn)
	}

	// Recoger todos los resultados
	var todosResultados []tipos.ResultadoConsultaRango
	var erroresS3 []string
	nodosNoDisponibles := make(map[string]struct{}) // Usar mapa para evitar duplicados

	for i := 0; i < len(seriesEncontradas); i++ {
		res := <-resultados

		// Registrar errores de S3 (críticos)
		if res.errS3 != nil {
			erroresS3 = append(erroresS3, fmt.Sprintf("%s: %v", res.path, res.errS3))
		}

		// Los errores de edge se registran como nodos no disponibles
		if res.errEdge != nil {
			log.Printf("Advertencia: error consultando edge para serie %s: %v", res.path, res.errEdge)
			nodosNoDisponibles[res.nodoID] = struct{}{}
		}

		// Agregar resultado si tiene datos
		if len(res.resultado.Series) > 0 {
			todosResultados = append(todosResultados, res.resultado)
		}
	}

	// Si hubo errores de S3 en todas las series, reportar
	if len(erroresS3) == len(seriesEncontradas) {
		return tipos.ResultadoConsultaRango{}, fmt.Errorf("error consultando S3: %v", erroresS3)
	}

	// Combinar todos los resultados en formato tabular final
	resultado := m.combinarResultadosTabulares(todosResultados)

	// Agregar nodos no disponibles al resultado
	for nodoID := range nodosNoDisponibles {
		resultado.NodosNoDisponibles = append(resultado.NodosNoDisponibles, nodoID)
	}
	sort.Strings(resultado.NodosNoDisponibles)

	return resultado, nil
}

// ConsultarUltimoPunto busca el último punto de cada serie combinando S3 y edge.
// Soporta wildcards en el path de la serie (ej: */temp, sensor_01/*).
// Los tiempos son opcionales:
//   - Si ambos son nil: retorna el último punto absoluto de cada serie
//   - Si se especifican: retorna el último punto dentro del rango temporal
//
// Retorna el último punto de CADA serie en formato columnar.
// Las series sin datos son excluidas del resultado.
func (m *ManagerDespachador) ConsultarUltimoPunto(nombreSerie string, tiempoInicio, tiempoFin *time.Time) (tipos.ResultadoConsultaPunto, error) {
	// Buscar todas las series que coincidan (path exacto o wildcard)
	seriesEncontradas, err := m.buscarSeriesPorPath(nombreSerie)
	if err != nil {
		return tipos.ResultadoConsultaPunto{}, err
	}

	// Convertir tiempos para S3
	var inicioNano, finNano int64
	if tiempoInicio != nil && tiempoFin != nil {
		inicioNano = tiempoInicio.UnixNano()
		finNano = tiempoFin.UnixNano()
	} else {
		inicioNano = 0
		finNano = time.Now().UnixNano()
	}

	// Canal para recoger resultados de todas las consultas
	type resultadoSerie struct {
		path      string
		tiempo    int64
		valor     interface{}
		ok        bool
		nodoID    string
		edgeError bool // Indica si hubo error al consultar el edge
	}
	resultados := make(chan resultadoSerie, len(seriesEncontradas))

	// Consultar cada serie en paralelo
	for _, sn := range seriesEncontradas {
		go func(sn serieConNodo) {
			var tiempo int64
			var valor interface{}
			encontrado := false
			edgeError := false

			// Primero intentar con el edge (tiene datos más recientes)
			resEdge, err := m.consultarPuntoEdge(sn.nodo, sn.path, tiempoInicio, tiempoFin, 5*time.Second)
			if err != nil {
				edgeError = true
				log.Printf("Advertencia: error consultando edge para serie %s: %v", sn.path, err)
			} else if len(resEdge.Series) > 0 {
				// El edge retorna formato columnar, buscar nuestra serie
				for i, s := range resEdge.Series {
					if s == sn.path {
						tiempo = resEdge.Tiempos[i]
						valor = resEdge.Valores[i]
						encontrado = true
						break
					}
				}
			}

			// Si el edge no responde o no tiene datos, buscar en S3
			if !encontrado {
				bloques, err := m.listarBloquesEnRango(sn.nodo.NodoID, sn.serie.SerieId, inicioNano, finNano)
				if err == nil && len(bloques) > 0 {
					ultimoBloque := bloques[len(bloques)-1]
					mediciones, err := m.descargarYDescomprimirBloque(ultimoBloque, sn.serie)
					if err == nil && len(mediciones) > 0 {
						// Encontrar la medición más reciente dentro del rango
						var ultimaMed *tipos.Medicion
						for i := range mediciones {
							med := &mediciones[i]
							// Verificar que está en el rango si se especificó
							if med.Tiempo >= inicioNano && med.Tiempo <= finNano {
								if ultimaMed == nil || med.Tiempo > ultimaMed.Tiempo {
									ultimaMed = med
								}
							}
						}
						if ultimaMed != nil {
							tiempo = ultimaMed.Tiempo
							valor = ultimaMed.Valor
							encontrado = true
						}
					}
				}
			}

			resultados <- resultadoSerie{
				path:      sn.path,
				tiempo:    tiempo,
				valor:     valor,
				ok:        encontrado,
				nodoID:    sn.nodo.NodoID,
				edgeError: edgeError,
			}
		}(sn)
	}

	// Recolectar resultados
	type puntoSerie struct {
		path   string
		tiempo int64
		valor  interface{}
	}
	var puntos []puntoSerie
	nodosNoDisponibles := make(map[string]struct{})

	for i := 0; i < len(seriesEncontradas); i++ {
		res := <-resultados
		if res.edgeError {
			nodosNoDisponibles[res.nodoID] = struct{}{}
		}
		if res.ok {
			puntos = append(puntos, puntoSerie{
				path:   res.path,
				tiempo: res.tiempo,
				valor:  res.valor,
			})
		}
	}

	if len(puntos) == 0 {
		return tipos.ResultadoConsultaPunto{}, fmt.Errorf("no se encontraron datos para la serie %s", nombreSerie)
	}

	// Ordenar alfabéticamente por path
	sort.Slice(puntos, func(i, j int) bool {
		return puntos[i].path < puntos[j].path
	})

	// Construir resultado columnar
	resultado := tipos.ResultadoConsultaPunto{
		Series:  make([]string, len(puntos)),
		Tiempos: make([]int64, len(puntos)),
		Valores: make([]interface{}, len(puntos)),
	}

	for i, p := range puntos {
		resultado.Series[i] = p.path
		resultado.Tiempos[i] = p.tiempo
		resultado.Valores[i] = p.valor
	}

	// Agregar nodos no disponibles al resultado
	for nodoID := range nodosNoDisponibles {
		resultado.NodosNoDisponibles = append(resultado.NodosNoDisponibles, nodoID)
	}
	sort.Strings(resultado.NodosNoDisponibles)

	return resultado, nil
}

// ============================================================================
// CONSULTAS DE AGREGACIÓN
// ============================================================================

// calcularAgregacionSimple calcula una agregación sobre un slice de valores float64.
// Función helper interna para calcular agregaciones sobre datos combinados.
func calcularAgregacionSimple(valores []float64, agregacion tipos.TipoAgregacion) (float64, error) {
	if len(valores) == 0 {
		return 0, fmt.Errorf("no hay valores para agregar")
	}

	switch agregacion {
	case tipos.AgregacionPromedio:
		suma := 0.0
		for _, v := range valores {
			suma += v
		}
		return suma / float64(len(valores)), nil

	case tipos.AgregacionMaximo:
		max := valores[0]
		for _, v := range valores[1:] {
			if v > max {
				max = v
			}
		}
		return max, nil

	case tipos.AgregacionMinimo:
		min := valores[0]
		for _, v := range valores[1:] {
			if v < min {
				min = v
			}
		}
		return min, nil

	case tipos.AgregacionSuma:
		suma := 0.0
		for _, v := range valores {
			suma += v
		}
		return suma, nil

	case tipos.AgregacionCount:
		return float64(len(valores)), nil

	default:
		return 0, fmt.Errorf("tipo de agregación no soportado: %s", agregacion)
	}
}

// ============================================================================
// HELPERS PARA BÚSQUEDA DE SERIES
// ============================================================================

// serieConNodo asocia una serie con su nodo para consultas paralelas
type serieConNodo struct {
	nodo  tipos.Nodo
	serie tipos.Serie
	path  string // Path original de la serie
}

// buscarSeriesPorPath busca series que coincidan con el path dado.
// Funciona tanto para paths exactos como para patrones con wildcards.
// Si es un path exacto, retorna la única serie que coincide.
// Si es un patrón wildcard, retorna todas las series que coincidan.
func (m *ManagerDespachador) buscarSeriesPorPath(path string) ([]serieConNodo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var resultados []serieConNodo

	// Si es wildcard, buscar por patrón
	if tipos.EsPatronWildcard(path) {
		for _, nodo := range m.nodos {
			for seriePath, serie := range nodo.Series {
				if tipos.MatchPath(seriePath, path) {
					resultados = append(resultados, serieConNodo{
						nodo:  *nodo,
						serie: serie,
						path:  seriePath,
					})
				}
			}
		}
	} else {
		// Path exacto: buscar directamente
		for _, nodo := range m.nodos {
			if serie, existe := nodo.Series[path]; existe {
				resultados = append(resultados, serieConNodo{
					nodo:  *nodo,
					serie: serie,
					path:  path,
				})
				break // Solo puede estar en un nodo
			}
		}
	}

	if len(resultados) == 0 {
		return nil, fmt.Errorf("serie '%s' no encontrada", path)
	}

	return resultados, nil
}

// ConsultarAgregacion calcula múltiples agregaciones combinando datos de S3 y edge.
// Soporta tipos de agregación: promedio, maximo, minimo, suma, count.
// Soporta wildcards en el path de la serie (ej: */temp, sensor_01/*).
// Retorna una matriz donde Valores[agregacion][serie] contiene el valor agregado.
func (m *ManagerDespachador) ConsultarAgregacion(
	nombreSerie string,
	tiempoInicio, tiempoFin time.Time,
	agregaciones []tipos.TipoAgregacion,
) (tipos.ResultadoAgregacion, error) {
	if len(agregaciones) == 0 {
		return tipos.ResultadoAgregacion{}, fmt.Errorf("debe especificar al menos una agregación")
	}

	// Usar ConsultarRango para obtener datos combinados
	resultado, err := m.ConsultarRango(nombreSerie, tiempoInicio, tiempoFin)
	if err != nil {
		return tipos.ResultadoAgregacion{}, err
	}

	if len(resultado.Series) == 0 || len(resultado.Tiempos) == 0 {
		return tipos.ResultadoAgregacion{}, fmt.Errorf("no se encontraron datos para %s en el rango especificado", nombreSerie)
	}

	// Extraer valores por columna (serie) una sola vez
	valoresPorSerie := make([][]float64, len(resultado.Series))
	for colIdx := range resultado.Series {
		var valoresColumna []float64
		for filaIdx := range resultado.Tiempos {
			if v := resultado.Valores[filaIdx][colIdx]; v != nil {
				switch val := v.(type) {
				case float64:
					valoresColumna = append(valoresColumna, val)
				case int64:
					valoresColumna = append(valoresColumna, float64(val))
				}
			}
		}
		valoresPorSerie[colIdx] = valoresColumna
	}

	// Calcular todas las agregaciones: Valores[agregacion][serie]
	numAgregaciones := len(agregaciones)
	numSeries := len(resultado.Series)
	valores := make([][]float64, numAgregaciones)

	for agIdx, agregacion := range agregaciones {
		valores[agIdx] = make([]float64, numSeries)
		for serieIdx, valoresColumna := range valoresPorSerie {
			if len(valoresColumna) == 0 {
				valores[agIdx][serieIdx] = math.NaN()
				continue
			}
			valor, err := calcularAgregacionSimple(valoresColumna, agregacion)
			if err != nil {
				valores[agIdx][serieIdx] = math.NaN()
			} else {
				valores[agIdx][serieIdx] = valor
			}
		}
	}

	return tipos.ResultadoAgregacion{
		Series:             resultado.Series, // Ya ordenadas alfabéticamente por ConsultarRango
		Agregaciones:       agregaciones,
		Valores:            valores, // [agregacion][serie]
		NodosNoDisponibles: resultado.NodosNoDisponibles,
	}, nil
}

// ConsultarAgregacionTemporal calcula múltiples agregaciones agrupadas por intervalos de tiempo (downsampling).
// Combina datos de S3 y edge, luego agrupa por intervalos del tamaño especificado.
// Soporta wildcards en el path de la serie (ej: */temp, sensor_01/*).
// Retorna una matriz donde Valores[agregacion][bucket][serie] contiene el valor agregado.
// Los valores faltantes (bucket sin datos para una serie) se representan como math.NaN().
func (m *ManagerDespachador) ConsultarAgregacionTemporal(
	nombreSerie string,
	tiempoInicio, tiempoFin time.Time,
	agregaciones []tipos.TipoAgregacion,
	intervalo time.Duration,
) (tipos.ResultadoAgregacionTemporal, error) {
	if len(agregaciones) == 0 {
		return tipos.ResultadoAgregacionTemporal{}, fmt.Errorf("debe especificar al menos una agregación")
	}

	if intervalo <= 0 {
		return tipos.ResultadoAgregacionTemporal{}, fmt.Errorf("intervalo debe ser mayor a cero")
	}

	// Usar ConsultarRango para obtener datos combinados
	resultado, err := m.ConsultarRango(nombreSerie, tiempoInicio, tiempoFin)
	if err != nil {
		return tipos.ResultadoAgregacionTemporal{}, err
	}

	if len(resultado.Series) == 0 || len(resultado.Tiempos) == 0 {
		return tipos.ResultadoAgregacionTemporal{}, fmt.Errorf("no se encontraron datos para la serie %s en el rango especificado", nombreSerie)
	}

	// Generar buckets temporales
	buckets := generarBuckets(tiempoInicio.UnixNano(), tiempoFin.UnixNano(), intervalo.Nanoseconds())
	numBuckets := len(buckets)
	numSeries := len(resultado.Series)
	numAgregaciones := len(agregaciones)

	// Inicializar acumuladores para cada [bucket][serie]
	acumuladores := make([][][]float64, numBuckets)
	for b := 0; b < numBuckets; b++ {
		acumuladores[b] = make([][]float64, numSeries)
		for s := 0; s < numSeries; s++ {
			acumuladores[b][s] = make([]float64, 0)
		}
	}

	// Distribuir valores en acumuladores
	intervaloNano := intervalo.Nanoseconds()
	tiempoInicioNano := tiempoInicio.UnixNano()

	for filaIdx, tiempo := range resultado.Tiempos {
		bucketIdx := calcularBucketIdx(tiempo, tiempoInicioNano, intervaloNano, numBuckets)
		if bucketIdx < 0 || bucketIdx >= numBuckets {
			continue
		}

		for colIdx := 0; colIdx < numSeries; colIdx++ {
			valor := resultado.Valores[filaIdx][colIdx]
			if valor == nil {
				continue
			}

			var valorFloat float64
			switch v := valor.(type) {
			case float64:
				valorFloat = v
			case int64:
				valorFloat = float64(v)
			default:
				continue
			}
			acumuladores[bucketIdx][colIdx] = append(acumuladores[bucketIdx][colIdx], valorFloat)
		}
	}

	// Calcular todas las agregaciones: Valores[agregacion][bucket][serie]
	valores := make([][][]float64, numAgregaciones)
	for agIdx, agregacion := range agregaciones {
		valores[agIdx] = make([][]float64, numBuckets)
		for b := 0; b < numBuckets; b++ {
			valores[agIdx][b] = make([]float64, numSeries)
			for s := 0; s < numSeries; s++ {
				if len(acumuladores[b][s]) == 0 {
					valores[agIdx][b][s] = math.NaN()
				} else {
					valorAgregado, err := calcularAgregacionSimple(acumuladores[b][s], agregacion)
					if err != nil {
						valores[agIdx][b][s] = math.NaN()
					} else {
						valores[agIdx][b][s] = valorAgregado
					}
				}
			}
		}
	}

	return tipos.ResultadoAgregacionTemporal{
		Series:             resultado.Series, // Ya ordenadas alfabéticamente por ConsultarRango
		Tiempos:            buckets,
		Agregaciones:       agregaciones,
		Valores:            valores, // [agregacion][bucket][serie]
		NodosNoDisponibles: resultado.NodosNoDisponibles,
	}, nil
}

// generarBuckets genera los timestamps de inicio de cada bucket temporal
func generarBuckets(tiempoInicio, tiempoFin, intervalo int64) []int64 {
	var buckets []int64
	for t := tiempoInicio; t < tiempoFin; t += intervalo {
		buckets = append(buckets, t)
	}
	return buckets
}

// calcularBucketIdx calcula el índice del bucket para un timestamp dado
func calcularBucketIdx(tiempo, tiempoInicio, intervalo int64, numBuckets int) int {
	if tiempo < tiempoInicio {
		return -1
	}
	idx := int((tiempo - tiempoInicio) / intervalo)
	if idx >= numBuckets {
		return numBuckets - 1 // Último bucket captura valores hasta tiempoFin
	}
	return idx
}
