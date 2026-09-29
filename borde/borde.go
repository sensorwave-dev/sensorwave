package borde

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sensorwave-dev/sensorwave/almacen"
	pebblemotor "github.com/sensorwave-dev/sensorwave/almacen/pebble"
	sqlitemotor "github.com/sensorwave-dev/sensorwave/almacen/sqlite"
	"github.com/sensorwave-dev/sensorwave/compresor"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

type GestorBorde struct {
	nodoID        string            // ID único del nodo borde
	direccion     string            // dirección pública del nodo (registro / ops)
	brokerMQTT    string            // Broker MQTT para federación con la nube
	tags          map[string]string // Metadatos libres del nodo (nombre, ubicación, etc.)
	db            almacen.Almacen   // Almacenamiento local del nodo
	cache         *Cache            // Cache en memoria de configuraciones de series
	coordinadores sync.Map          // Map de coordinadores de series (gestión de compresión)
	mu            sync.RWMutex      // Mutex para proteger el contador
	contador      int               // Contador para generar IDs únicos de series
	motorReglas   *MotorReglas      // Motor de reglas integrado
	finalizado    chan struct{}     // Canal para señalizar cierre del gestor
	federacion    *federacionMQTT   // Worker de federación MQTT (nil si no está activo)
	intervaloS3   time.Duration     // Ciclo de registro, borrados pendientes y migración
	cierre        sync.Once         // La segunda llamada a Cerrar no cierra canales
	muIngesta     sync.Mutex        // Protege cerrado frente a Insertar en curso
	cerrado       bool              // Insertar rechaza datos nuevos
	enVuelo       sync.WaitGroup    // Insertar que ya pasaron el cierre
	fondo         sync.WaitGroup    // Coordinadores de compactación y ciclo de S3
}

type errorValidacion struct {
	mensaje string
}

func (e errorValidacion) Error() string {
	return e.mensaje
}

var (
	errSerieNoEncontrada = errors.New("serie no encontrada")
	errTipoIncompatible  = errors.New("tipo de dato incompatible")
)

type Cache struct {
	datos map[string]tipos.Serie // Mapa para almacenar las configuraciones de series
	mu    sync.RWMutex           // Mutex para proteger el acceso concurrente
}

type CoordinadorSerie struct {
	serie               tipos.Serie   // Configuración de la serie
	mu                  sync.Mutex    // Mutex para proteger el acceso al coordinador
	contador            atomic.Int64  // Contador de puntos pendientes de compresión
	finalizado          chan struct{} // Canal para señalar cierre del hilo
	notificarCompresion chan struct{} // Canal para notificar necesidad de compresión
}

// incrementarContador incrementa el contador de puntos en ingesta
func (cs *CoordinadorSerie) incrementarContador() int {
	return int(cs.contador.Add(1))
}

// decrementarContador decrementa el contador de puntos en ingesta
func (cs *CoordinadorSerie) decrementarContador(n int) {
	if n <= 0 {
		return
	}

	for {
		actual := cs.contador.Load()
		nuevo := actual - int64(n)
		if nuevo < 0 {
			nuevo = 0
		}
		if cs.contador.CompareAndSwap(actual, nuevo) {
			return
		}
	}
}

// escribirPuntoIngesta persiste un punto en la ingesta de una serie.
func (me *GestorBorde) escribirPuntoIngesta(serieId int, medicion tipos.Medicion) error {
	datos, err := tipos.SerializarGob(medicion)
	if err != nil {
		return fmt.Errorf("error al serializar medición: %v", err)
	}
	return me.db.AgregarPunto(almacen.Punto{SerieID: serieId, Tiempo: medicion.Tiempo, Datos: datos})
}

// leerPuntosIngesta lee los N puntos más antiguos de la ingesta de una serie.
// Los valores ilegibles se borran y se cuentan en el segundo resultado.
func (me *GestorBorde) leerPuntosIngesta(serieId int, n int) ([]tipos.Medicion, int, error) {
	puntos, err := me.db.PuntosIngestaAntiguos(serieId, n)
	if err != nil {
		return nil, 0, err
	}

	mediciones := make([]tipos.Medicion, 0, len(puntos))
	var ilegibles []int64
	for _, punto := range puntos {
		var medicion tipos.Medicion
		if err := tipos.DeserializarGob(punto.Datos, &medicion); err != nil {
			log.Printf("Ingesta ilegible serie %d tiempo %d: %v", serieId, punto.Tiempo, err)
			ilegibles = append(ilegibles, punto.Tiempo)
			continue
		}
		mediciones = append(mediciones, medicion)
	}
	if len(ilegibles) > 0 {
		if err := me.db.CerrarBloque(almacen.Bloque{SerieID: serieId}, ilegibles); err != nil {
			log.Printf("Error borrando ingesta ilegible de la serie %d: %v", serieId, err)
		}
	}
	return mediciones, len(ilegibles), nil
}

// leerPuntosIngestaEnRango lee los puntos de ingesta de una serie en un rango inclusivo.
func (me *GestorBorde) leerPuntosIngestaEnRango(serieId int, tiempoInicio, tiempoFin int64) ([]tipos.Medicion, error) {
	puntos, err := me.db.PuntosEnRango(serieId, tiempoInicio, tiempoFin)
	if err != nil {
		return nil, err
	}
	mediciones := make([]tipos.Medicion, 0, len(puntos))
	for _, punto := range puntos {
		var medicion tipos.Medicion
		if err := tipos.DeserializarGob(punto.Datos, &medicion); err != nil {
			continue
		}
		mediciones = append(mediciones, medicion)
	}
	return mediciones, nil
}

// leerUltimoPuntoIngesta retorna la medición más reciente en ingesta para una serie.
func (me *GestorBorde) leerUltimoPuntoIngesta(serieId int) (tipos.Medicion, bool, error) {
	punto, ok, err := me.db.UltimoPunto(serieId)
	if err != nil || !ok {
		return tipos.Medicion{}, false, err
	}
	var medicion tipos.Medicion
	if err := tipos.DeserializarGob(punto.Datos, &medicion); err != nil {
		return tipos.Medicion{}, false, err
	}
	return medicion, true, nil
}

// contarPuntosIngesta cuenta los puntos en la ingesta de una serie.
func (me *GestorBorde) contarPuntosIngesta(serieId int) int {
	puntos, err := me.db.PuntosEnRango(serieId, math.MinInt64, math.MaxInt64)
	if err != nil {
		return 0
	}
	return len(puntos)
}

// reconstruirContadoresIngesta reconstruye los contadores de ingesta al iniciar
func (me *GestorBorde) reconstruirContadoresIngesta() error {
	me.coordinadores.Range(func(clave, valor interface{}) bool {
		cs := valor.(*CoordinadorSerie)
		count := me.contarPuntosIngesta(cs.serie.SerieId)
		if count > 0 {
			cs.contador.Store(int64(count))
			// Si hay suficientes puntos, notificar compresión
			if count >= cs.serie.TamañoBloque {
				select {
				case cs.notificarCompresion <- struct{}{}:
				default:
				}
			}
		}
		return true
	})
	return nil
}

// extraerTimestamps extrae los timestamps de un slice de mediciones
func extraerTimestamps(mediciones []tipos.Medicion) []int64 {
	timestamps := make([]int64, len(mediciones))
	for i, m := range mediciones {
		timestamps[i] = m.Tiempo
	}
	return timestamps
}

// comprimirPuntos comprime un slice de mediciones según la configuración de la serie
func (me *GestorBorde) comprimirPuntos(mediciones []tipos.Medicion, serie tipos.Serie) ([]byte, error) {
	if len(mediciones) == 0 {
		return nil, fmt.Errorf("no hay mediciones para comprimir")
	}

	// NIVEL 1: Compresión específica
	// Tiempo: SIEMPRE usar DeltaDelta
	tiemposComprimidos := compresor.CompresionDeltaDeltaTiempo(mediciones)

	// Valores: usar compresión configurada según tipo de datos
	valores := compresor.ExtraerValores(mediciones)
	var valoresComprimidos []byte
	var err error

	switch serie.TipoDatos {
	case tipos.Integer:
		valoresInt, errConv := compresor.ConvertirAInt64Array(valores)
		if errConv != nil {
			return nil, errConv
		}

		switch serie.CompresionBytes {
		case tipos.DeltaDelta:
			comp := &compresor.CompresorDeltaDeltaGenerico[int64]{}
			valoresComprimidos, err = comp.Comprimir(valoresInt)
		case tipos.RLE:
			comp := &compresor.CompresorRLEGenerico[int64]{}
			valoresComprimidos, err = comp.Comprimir(valoresInt)
		case tipos.Bits:
			comp := &compresor.CompresorBitsGenerico[int64]{}
			valoresComprimidos, err = comp.Comprimir(valoresInt)
		case tipos.SinCompresionBytes:
			comp := &compresor.CompresorNingunoGenerico[int64]{}
			valoresComprimidos, err = comp.Comprimir(valoresInt)
		default:
			return nil, fmt.Errorf("compresión no soportada para tipo Integer: %v", serie.CompresionBytes)
		}

	case tipos.Real:
		valoresFloat, errConv := compresor.ConvertirAFloat64Array(valores)
		if errConv != nil {
			return nil, errConv
		}

		switch serie.CompresionBytes {
		case tipos.DeltaDelta:
			comp := &compresor.CompresorDeltaDeltaGenerico[float64]{}
			valoresComprimidos, err = comp.Comprimir(valoresFloat)
		case tipos.Xor:
			comp := &compresor.CompresorXor{}
			valoresComprimidos, err = comp.Comprimir(valoresFloat)
		case tipos.RLE:
			comp := &compresor.CompresorRLEGenerico[float64]{}
			valoresComprimidos, err = comp.Comprimir(valoresFloat)
		case tipos.SinCompresionBytes:
			comp := &compresor.CompresorNingunoGenerico[float64]{}
			valoresComprimidos, err = comp.Comprimir(valoresFloat)
		default:
			return nil, fmt.Errorf("compresión no soportada para tipo Real: %v", serie.CompresionBytes)
		}

	case tipos.Boolean:
		valoresBool, errConv := compresor.ConvertirABoolArray(valores)
		if errConv != nil {
			return nil, errConv
		}

		switch serie.CompresionBytes {
		case tipos.RLE:
			comp := &compresor.CompresorRLEGenerico[bool]{}
			valoresComprimidos, err = comp.Comprimir(valoresBool)
		case tipos.SinCompresionBytes:
			comp := &compresor.CompresorNingunoGenerico[bool]{}
			valoresComprimidos, err = comp.Comprimir(valoresBool)
		default:
			return nil, fmt.Errorf("compresión no soportada para tipo Boolean: %v", serie.CompresionBytes)
		}

	case tipos.Text:
		valoresStr, errConv := compresor.ConvertirAStringArray(valores)
		if errConv != nil {
			return nil, errConv
		}

		switch serie.CompresionBytes {
		case tipos.Diccionario:
			comp := &compresor.CompresorDiccionario{}
			valoresComprimidos, err = comp.Comprimir(valoresStr)
		case tipos.RLE:
			comp := &compresor.CompresorRLEGenerico[string]{}
			valoresComprimidos, err = comp.Comprimir(valoresStr)
		case tipos.SinCompresionBytes:
			comp := &compresor.CompresorNingunoGenerico[string]{}
			valoresComprimidos, err = comp.Comprimir(valoresStr)
		default:
			return nil, fmt.Errorf("compresión no soportada para tipo Text: %v", serie.CompresionBytes)
		}

	default:
		return nil, fmt.Errorf("tipo de datos no soportado: %v", serie.TipoDatos)
	}

	if err != nil {
		return nil, err
	}

	// Combinar datos del nivel 1
	bloqueNivel1 := compresor.CombinarDatos(tiemposComprimidos, valoresComprimidos)

	// NIVEL 2: Compresión de bloque
	compresorBloque := compresor.ObtenerCompresorBloque(serie.CompresionBloque)
	bloqueFinal, err := compresorBloque.Comprimir(bloqueNivel1)
	if err != nil {
		return nil, err
	}

	return bloqueFinal, nil
}

// Opciones configura la creación de un GestorBorde.
// ConfigS3 es opcional (nil = modo desconectado sin sincronización con nube).
type Opciones struct {
	NombreDB    string                 // Directorio del almacenamiento local (requerido)
	Motor       string                 // Motor de almacenamiento. Vacío o "pebble" abre Pebble. "sqlite" abre SQLite dentro de ese directorio.
	Direccion   string                 // Dirección pública del nodo (registro / ops)
	BrokerMQTT  string                 // Broker MQTT para federación con la nube (requerido solo si ConfigS3 != nil)
	ConfigS3    *tipos.ConfiguracionS3 // nil = modo local sin nube (debe ser explícito si se usa)
	Tags        map[string]string      // Metadatos libres del nodo (nombre, ubicación, etc.)
	IntervaloS3 time.Duration          // Ciclo de registro, borrados pendientes y migración. Cero usa 5 minutos.
}

// abrirMotor abre el almacenamiento indicado en Opciones.
// Motor vacío equivale a Pebble. NombreDB es un directorio en ambos casos.
func abrirMotor(opts Opciones) (almacen.Almacen, error) {
	nombre := opts.Motor
	if nombre == "" {
		nombre = "pebble"
	}
	switch nombre {
	case "pebble":
		return pebblemotor.Abrir(opts.NombreDB)
	case "sqlite":
		return sqlitemotor.Abrir(opts.NombreDB)
	default:
		return nil, fmt.Errorf("motor de almacenamiento desconocido: %s", nombre)
	}
}

// Crear inicializa el GestorBorde con las opciones especificadas.
// ConfigS3 es opcional: si es nil, funciona en modo local sin registro en nube.
// Si se proporciona, debe ser una configuración completa y válida.
func Crear(opts Opciones) (*GestorBorde, error) {

	// Validar opciones requeridas
	if opts.NombreDB == "" {
		return &GestorBorde{}, fmt.Errorf("NombreDB es requerido")
	}

	// Validar opts.Dirección pública no sea vacía
	if opts.Direccion == "" {
		return &GestorBorde{}, fmt.Errorf("Dirección es requerida")
	}

	// Validar conexión federada según configuración de S3
	if opts.ConfigS3 != nil {
		// Con S3: se requiere BrokerMQTT para federación con la nube
		if opts.BrokerMQTT == "" {
			return &GestorBorde{}, fmt.Errorf("BrokerMQTT es requerido cuando ConfigS3 está configurado")
		}
	} else {
		// Sin S3: no se requiere federación
		if opts.BrokerMQTT != "" {
			return &GestorBorde{}, fmt.Errorf("BrokerMQTT no debe especificarse sin ConfigS3")
		}
	}

	// Abrir el motor de almacenamiento. Vacío equivale a Pebble.
	db, err := abrirMotor(opts)
	if err != nil {
		return &GestorBorde{}, err
	}

	// Inicializar el GestorBorde (datos básicos)
	gestor := &GestorBorde{
		db:         db,
		direccion:  opts.Direccion,
		brokerMQTT: opts.BrokerMQTT,
		tags:       opts.Tags,
		cache:      &Cache{datos: make(map[string]tipos.Serie)},
		finalizado: make(chan struct{}),
		contador:   0,
	}

	// Cargar o generar nodoID
	nodoID, existeNodo, err := db.ObtenerNodoID()
	if err != nil {
		return gestor.abortarCreacion(fmt.Errorf("error al leer nodo_id: %v", err))
	}
	if !existeNodo {
		gestor.nodoID = generarNodoID()
		if err = db.GuardarNodoID(gestor.nodoID); err != nil {
			return gestor.abortarCreacion(fmt.Errorf("error al guardar nodo_id: %v", err))
		}
		log.Printf("Nuevo nodoID generado: %s", gestor.nodoID)
	} else {
		gestor.nodoID = nodoID
		log.Printf("NodoID cargado desde DB: %s", gestor.nodoID)
	}

	// Cargar o actualizar tags del nodo.
	// Si se especifican tags en opciones, sobrescriben los guardados.
	if opts.Tags != nil {
		tagsBytes, err := tipos.SerializarGob(opts.Tags)
		if err != nil {
			return gestor.abortarCreacion(fmt.Errorf("error al serializar tags: %v", err))
		}
		if err = db.GuardarTagsNodo(tagsBytes); err != nil {
			return gestor.abortarCreacion(fmt.Errorf("error al guardar tags: %v", err))
		}
		gestor.tags = opts.Tags
		log.Printf("Tags actualizados: %v", opts.Tags)
	} else {
		tagsBytes, ok, err := db.ObtenerTagsNodo()
		if err != nil {
			return gestor.abortarCreacion(fmt.Errorf("error al leer tags: %v", err))
		}
		if ok {
			var tagsGuardados map[string]string
			if err := tipos.DeserializarGob(tagsBytes, &tagsGuardados); err == nil {
				gestor.tags = tagsGuardados
				log.Printf("Tags cargados desde DB: %v", tagsGuardados)
			}
		}
	}

	// Cargar el contador de SerieId del nodo.
	contadorBytes, okContador, err := db.ObtenerContador()
	if err != nil {
		return gestor.abortarCreacion(fmt.Errorf("error al leer contador: %v", err))
	}
	if okContador && len(contadorBytes) >= 4 {
		gestor.contador = int(binary.LittleEndian.Uint32(contadorBytes))
	}

	// Cargar series existentes desde PebbleDB
	err = gestor.cargarSeriesExistentes()
	if err != nil {
		return gestor.abortarCreacion(fmt.Errorf("error al cargar series: %v", err))
	}

	// Reconstruir contadores de ingesta desde Pebble
	err = gestor.reconstruirContadoresIngesta()
	if err != nil {
		return gestor.abortarCreacion(fmt.Errorf("error al reconstruir contadores de ingesta: %v", err))
	}

	// Configurar S3 si se proporciona configuración
	if opts.ConfigS3 != nil {
		// Aplicar defaults y validar
		opts.ConfigS3.AplicarDefaults()
		if err := opts.ConfigS3.Validar(); err != nil {
			return gestor.abortarCreacion(fmt.Errorf("configuración S3 inválida: %w", err))
		}

		err = gestor.configurarS3(*opts.ConfigS3)
		if err != nil {
			log.Printf("Advertencia: error al configurar S3: %v", err)
			log.Printf("El nodo funcionará en modo local")
		}
	} else {
		log.Printf("Modo local: sin registro en nube ni servidor HTTP")
	}

	// Inicializar el motor de reglas integrado
	gestor.motorReglas = nuevoMotorReglasIntegrado(gestor, db)
	// Cargar reglas existentes
	err = gestor.motorReglas.cargarReglasExistentes()
	if err != nil {
		return gestor.abortarCreacion(fmt.Errorf("error al cargar reglas: %v", err))
	}

	// Si quedó un cliente S3, el ciclo registra el nodo, reintenta borrados y migra bloques.
	if clienteS3 != nil {
		gestor.intervaloS3 = opts.IntervaloS3
		if gestor.intervaloS3 <= 0 {
			gestor.intervaloS3 = 5 * time.Minute
		}
		gestor.iniciarCicloS3()
	}

	// Iniciar federación MQTT si hay broker configurado (modo conectado con S3)
	if opts.BrokerMQTT != "" {
		fed, err := gestor.iniciarFederacionMQTT(opts.BrokerMQTT)
		if err != nil {
			return gestor.abortarCreacion(fmt.Errorf("error iniciando federación MQTT: %w", err))
		}
		gestor.federacion = fed
		log.Printf("Federación MQTT iniciada: %s", opts.BrokerMQTT)
	}

	return gestor, nil
}

// abortarCreacion cierra coordinadores y la base cuando Crear falla después de abrirla.
func (me *GestorBorde) abortarCreacion(err error) (*GestorBorde, error) {
	me.muIngesta.Lock()
	me.cerrado = true
	me.muIngesta.Unlock()
	close(me.finalizado)
	me.fondo.Wait()
	if me.federacion != nil {
		me.federacion.cerrar()
	}
	me.db.Cerrar()
	return &GestorBorde{}, err
}

// ActivarFederacionMQTT inicia el plano de control MQTT si aún no está activo.
// Pensado para pruebas y escenarios donde el nodo ya opera en modo local.
// DesconectarFederacion cierra la sesión MQTT del borde. El almacenamiento
// local y el cliente de objetos siguen abiertos.
func (me *GestorBorde) DesconectarFederacion() {
	if me.federacion == nil {
		return
	}
	me.federacion.cerrar()
	me.federacion = nil
}

func (me *GestorBorde) ActivarFederacionMQTT(broker string) error {
	if broker == "" {
		return fmt.Errorf("broker MQTT vacío")
	}
	if me.federacion != nil {
		return nil
	}
	fed, err := me.iniciarFederacionMQTT(broker)
	if err != nil {
		return err
	}
	me.federacion = fed
	me.brokerMQTT = broker
	log.Printf("Federación MQTT iniciada: %s", broker)
	return nil
}

// ObtenerNodeID retorna el ID del nodo borde
func (me *GestorBorde) ObtenerNodoID() string {
	return me.nodoID
}

// ObtenerTags retorna los tags del nodo borde
func (me *GestorBorde) ObtenerTags() map[string]string {
	return me.tags
}

// ActualizarTags actualiza los tags del nodo y los persiste.
// Si tags es nil, elimina todos los tags guardados.
func (me *GestorBorde) ActualizarTags(tags map[string]string) error {
	if tags == nil {
		if err := me.db.GuardarTagsNodo(nil); err != nil {
			return fmt.Errorf("error al eliminar tags: %v", err)
		}
		me.tags = nil
		log.Printf("Tags eliminados")
	} else {
		tagsBytes, err := tipos.SerializarGob(tags)
		if err != nil {
			return fmt.Errorf("error al serializar tags: %v", err)
		}
		if err = me.db.GuardarTagsNodo(tagsBytes); err != nil {
			return fmt.Errorf("error al guardar tags: %v", err)
		}
		me.tags = tags
		log.Printf("Tags actualizados: %v", tags)
	}

	me.informarCatalogoS3("tags")
	return nil
}

// cargarSeriesExistentes carga todas las series al cache.
func (me *GestorBorde) cargarSeriesExistentes() error {
	iter, err := me.db.ListarSeries()
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Siguiente() {
		seriesPath := iter.Clave()
		if seriesPath == "" {
			continue
		}

		var config tipos.Serie
		if err := tipos.DeserializarGob(iter.Valor(), &config); err != nil {
			continue
		}

		// Inicializar Tags si es nil
		if config.Tags == nil {
			config.Tags = make(map[string]string)
		}

		// Agregar al cache usando Path como clave
		me.cache.mu.Lock()
		me.cache.datos[seriesPath] = config
		me.cache.mu.Unlock()

		// Crear coordinador y goroutine para cada serie
		coordinador := &CoordinadorSerie{
			serie:               config,
			finalizado:          make(chan struct{}),
			notificarCompresion: make(chan struct{}, 1),
		}

		me.coordinadores.Store(seriesPath, coordinador)
		me.lanzarCoordinador(coordinador)
	}

	return iter.Error()
}

// Cerrar deja de aceptar inserts, compacta los bloques completos y cierra la base.
// Una segunda llamada no vuelve a cerrar canales.
func (me *GestorBorde) Cerrar() {
	me.cierre.Do(func() {
		me.muIngesta.Lock()
		me.cerrado = true
		me.muIngesta.Unlock()
		me.enVuelo.Wait()

		close(me.finalizado)

		if me.federacion != nil {
			me.federacion.cerrar()
		}

		me.fondo.Wait()

		me.coordinadores.Range(func(_, valor any) bool {
			cs := valor.(*CoordinadorSerie)
			close(cs.finalizado)
			return true
		})

		me.db.Cerrar()
	})
}

// CrearSerie crea una nueva serie si no existe. Si ya existe, no hace nada.
func (me *GestorBorde) CrearSerie(config tipos.Serie) error {
	// Validar campos obligatorios
	if config.Path == "" {
		return errorValidacion{"el path de la serie no puede estar vacío"}
	}

	// Validar que el path sea correcto
	if !esPathValido(config.Path) {
		return errorValidacion{fmt.Sprintf("el path de la serie tiene un formato inválido: %s", config.Path)}
	}

	// Validar TipoDatos
	tiposValidos := []tipos.TipoDatos{tipos.Boolean, tipos.Integer, tipos.Real, tipos.Text}
	tipoDatosValido := false
	for _, tipo := range tiposValidos {
		if config.TipoDatos == tipo {
			tipoDatosValido = true
			break
		}
	}
	if !tipoDatosValido {
		return errorValidacion{fmt.Sprintf("tipo de datos inválido: %s, debe ser Boolean, Integer, Real o Text", config.TipoDatos)}
	}

	// Validar TamañoBloque
	// se encuentra en el rango de (0, 10000]
	if config.TamañoBloque <= 0 || config.TamañoBloque > 10000 {
		return errorValidacion{fmt.Sprintf("el tamaño del bloque debe ubicarse en el rango de(0, 10000], recibido: %d", config.TamañoBloque)}
	}

	// Validar CompresionBloque
	compresionBloqueValida := false
	for _, compresion := range []tipos.TipoCompresionBloque{
		tipos.SinCompresionBloque, tipos.LZ4, tipos.ZSTD, tipos.Snappy, tipos.Gzip} {
		if config.CompresionBloque == compresion {
			compresionBloqueValida = true
			break
		}
	}
	if !compresionBloqueValida {
		return errorValidacion{fmt.Sprintf("tipo de compresión de bloque inválido: %s", config.CompresionBloque)}
	}

	// Validar CompresionBytes - usar validación automática del sistema de tipos
	if err := config.TipoDatos.ValidarCompresion(config.CompresionBytes); err != nil {
		return errorValidacion{fmt.Sprintf("error en compresión de bytes: %v", err)}
	}

	// Generar clave única basada en Path
	serieClave := config.Path

	// Inicializar Tags si es nil
	if config.Tags == nil {
		config.Tags = make(map[string]string)
	}

	// El lock cubre la existencia, el alta y la publicación del coordinador.
	me.mu.Lock()
	defer me.mu.Unlock()

	me.cache.mu.RLock()
	_, existe := me.cache.datos[serieClave]
	me.cache.mu.RUnlock()
	if existe {
		return nil
	}

	_, existeEnDisco, err := me.db.ObtenerSerie(serieClave)
	if err != nil {
		return fmt.Errorf("error al verificar serie: %v", err)
	}
	if existeEnDisco {
		return nil
	}

	me.contador++
	config.SerieId = me.contador

	contadorBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(contadorBytes, uint32(me.contador))
	if err = me.db.GuardarContador(contadorBytes); err != nil {
		return fmt.Errorf("error al actualizar contador: %v", err)
	}

	serieBytes, err := tipos.SerializarGob(config)
	if err != nil {
		return fmt.Errorf("error al serializar serie: %v", err)
	}
	if err = me.db.GuardarSerie(serieClave, serieBytes); err != nil {
		return fmt.Errorf("error al guardar serie: %v", err)
	}

	// Actualizar cache usando Path como clave
	me.cache.mu.Lock()
	me.cache.datos[string(serieClave)] = config
	me.cache.mu.Unlock()

	// Crear coordinador y goroutine para la nueva serie
	coordinador := &CoordinadorSerie{
		serie:               config,
		finalizado:          make(chan struct{}),
		notificarCompresion: make(chan struct{}, 1),
	}

	me.coordinadores.Store(serieClave, coordinador)
	me.lanzarCoordinador(coordinador)

	me.informarCatalogoS3("serie nueva")
	return nil
}

func (me *GestorBorde) lanzarCoordinador(cs *CoordinadorSerie) {
	me.fondo.Add(1)
	go me.coordinarCompresion(cs)
}

// coordinarCompresion compacta bloques completos. Si después de uno el contador
// sigue en TamañoBloque o más, deja otro aviso. Al cerrar el gestor drena esos
// bloques y, si uno falla, no lo reintenta.
func (me *GestorBorde) coordinarCompresion(cs *CoordinadorSerie) {
	defer me.fondo.Done()
	for {
		select {
		case <-cs.finalizado:
			return
		case <-me.finalizado:
			for cs.contador.Load() >= int64(cs.serie.TamañoBloque) {
				if !me.compactarUnBloque(cs) {
					return
				}
			}
			return
		case <-cs.notificarCompresion:
			if !me.compactarUnBloque(cs) {
				continue
			}
			if cs.contador.Load() >= int64(cs.serie.TamañoBloque) {
				select {
				case cs.notificarCompresion <- struct{}{}:
				default:
				}
			}
		}
	}
}

// compactarUnBloque lee, comprime y escribe un bloque. Devuelve false si la
// lectura, la compresión o la escritura fallan.
func (me *GestorBorde) compactarUnBloque(cs *CoordinadorSerie) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	puntos, descartados, err := me.leerPuntosIngesta(cs.serie.SerieId, cs.serie.TamañoBloque)
	if descartados > 0 {
		cs.decrementarContador(descartados)
	}
	if err != nil || len(puntos) == 0 {
		return false
	}

	bloqueComprimido, err := me.comprimirPuntos(puntos, cs.serie)
	if err != nil {
		fmt.Printf("Error al comprimir puntos para serie %s: %v\n", cs.serie.Path, err)
		return false
	}

	tiempoInicio := puntos[0].Tiempo
	tiempoFinal := puntos[len(puntos)-1].Tiempo
	timestamps := extraerTimestamps(puntos)
	if err = me.db.CerrarBloque(almacen.Bloque{
		SerieID: cs.serie.SerieId,
		Inicio:  tiempoInicio,
		Fin:     tiempoFinal,
		Datos:   bloqueComprimido,
	}, timestamps); err != nil {
		fmt.Printf("Error al cerrar bloque para serie %s: %v\n", cs.serie.Path, err)
		return false
	}

	fmt.Println("Almacenando bloque para serie:", cs.serie.Path,
		"Tiempo inicio:", tiempoInicio,
		"Tiempo final:", tiempoFinal,
		"Mediciones:", len(puntos),
		"Tamaño comprimido:", len(bloqueComprimido))

	cs.decrementarContador(len(puntos))
	return true
}

// Insertar agrega un nuevo dato a la serie especificada
func (me *GestorBorde) Insertar(path string, tiempo int64, dato any) error {
	me.muIngesta.Lock()
	if me.cerrado {
		me.muIngesta.Unlock()
		return fmt.Errorf("el nodo está cerrado")
	}
	me.enVuelo.Add(1)
	me.muIngesta.Unlock()
	defer me.enVuelo.Done()

	csInterface, ok := me.coordinadores.Load(path)
	if !ok {
		return fmt.Errorf("%w: %s", errSerieNoEncontrada, path)
	}

	cs := csInterface.(*CoordinadorSerie)

	if !esCompatibleConTipo(dato, cs.serie.TipoDatos) {
		return fmt.Errorf("%w: esperado %s, recibido %T",
			errTipoIncompatible, cs.serie.TipoDatos, dato)
	}

	// Crear la medición con tiempo y valor
	medicion := tipos.Medicion{
		Tiempo: tiempo,
		Valor:  dato,
	}

	// 1. Escribir a ingesta (persistencia inmediata)
	if err := me.escribirPuntoIngesta(cs.serie.SerieId, medicion); err != nil {
		return fmt.Errorf("error al escribir punto a ingesta: %v", err)
	}

	// 2. Incrementar contador
	contador := cs.incrementarContador()

	// 3. Si contador >= TamañoBloque, notificar para comprimir
	if contador >= cs.serie.TamañoBloque {
		select {
		case cs.notificarCompresion <- struct{}{}:
		default: // ya hay notificación pendiente
		}
	}

	// 4. Evaluar reglas
	me.motorReglas.evaluarReglas(time.Unix(0, tiempo))

	return nil
}

// descomprimirBloque descomprime un bloque de datos usando la función pública del compresor
func (me *GestorBorde) descomprimirBloque(datosComprimidos []byte, serie tipos.Serie) ([]tipos.Medicion, error) {
	return compresor.DescomprimirBloqueSerie(datosComprimidos, serie.TipoDatos, serie.CompresionBytes, serie.CompresionBloque)
}

func (me *GestorBorde) AgregarRegla(regla *Regla) error {
	return me.motorReglas.AgregarRegla(regla)
}

func (me *GestorBorde) EliminarRegla(id string) error {
	return me.motorReglas.EliminarRegla(id)
}

func (me *GestorBorde) ActualizarRegla(regla *Regla) error {
	return me.motorReglas.ActualizarRegla(regla)
}

func (me *GestorBorde) HabilitarRegla(id string, habilitada bool) error {
	return me.motorReglas.HabilitarRegla(id, habilitada)
}

func (me *GestorBorde) RegistrarEjecutor(tipoAccion string, ejecutor EjecutorAccion) error {
	return me.motorReglas.RegistrarEjecutor(tipoAccion, ejecutor)
}

func (me *GestorBorde) ListarReglas() map[string]*Regla {
	return me.motorReglas.ListarReglas()
}

// ObtenerRegla obtiene una regla por su ID
func (me *GestorBorde) ObtenerRegla(id string) (*Regla, error) {
	return me.motorReglas.ObtenerRegla(id)
}

func (me *GestorBorde) HabilitarMotorReglas(habilitado bool) {
	me.motorReglas.Habilitar(habilitado)
}

// ObtenerEstadoMotorReglas devuelve el estado actual del motor de reglas
func (me *GestorBorde) ObtenerEstadoMotorReglas() EstadoMotorReglas {
	return me.motorReglas.ObtenerEstado()
}

// EliminarSerie elimina una serie y todos sus datos asociados.
// Elimina: metadatos, bloques de datos locales y cache.
// Si S3 está configurado, registra la eliminación pendiente, sube el catálogo
// y procesa el borrado en el acto. La eliminación local siempre se completa;
// si S3 falla, el ciclo de IntervaloS3 reintenta.
func (me *GestorBorde) EliminarSerie(path string) error {
	// Verificar que la serie existe
	me.cache.mu.RLock()
	serie, existe := me.cache.datos[path]
	me.cache.mu.RUnlock()

	if !existe {
		return fmt.Errorf("serie no encontrada: %s", path)
	}

	serieId := serie.SerieId

	// 1. Si S3 está configurado, guardar eliminación pendiente ANTES de eliminar localmente
	// Esto garantiza que si el sistema falla, la eliminación de S3 se reintentará
	if clienteS3 != nil {
		if err := me.guardarEliminacionPendiente(serieId, path); err != nil {
			log.Printf("Advertencia: error guardando eliminación pendiente: %v", err)
			// Continuamos con la eliminación local de todas formas
		}
	}

	// 2. Cerrar el coordinador y su goroutine
	if csInterface, ok := me.coordinadores.Load(path); ok {
		cs := csInterface.(*CoordinadorSerie)

		// Señalar al goroutine que termine
		close(cs.finalizado)

		// Eliminar del mapa de coordinadores
		me.coordinadores.Delete(path)
	}

	if err := me.db.BorrarSerie(path, serieId); err != nil {
		return fmt.Errorf("error al eliminar serie: %v", err)
	}

	// 5. Eliminar del cache
	me.cache.mu.Lock()
	delete(me.cache.datos, path)
	me.cache.mu.Unlock()

	log.Printf("Serie eliminada localmente: %s (ID: %d)", path, serieId)

	me.informarCatalogoS3("eliminación de serie")
	if err := me.procesarEliminacionesPendientes(); err != nil {
		log.Printf("Advertencia: error eliminando serie de S3: %v", err)
	}

	return nil
}
