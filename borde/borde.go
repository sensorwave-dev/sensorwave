package borde

import (
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/sensorwave-dev/sensorwave/compresor"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

type GestorBorde struct {
	nodoID        string            // ID único del nodo borde
	direccion     string            // dirección pública para uso de API REST
	puertoHTTP    string            // Puerto HTTP para la API REST
	tags          map[string]string // Metadatos libres del nodo (nombre, ubicación, etc.)
	db            *pebble.DB        // Base de datos Pebble local
	cache         *Cache            // Cache en memoria de configuraciones de series
	coordinadores sync.Map          // Map de coordinadores de series (gestión de compresión)
	mu            sync.RWMutex      // Mutex para proteger el contador
	contador      int               // Contador para generar IDs únicos de series
	motorReglas   *MotorReglas      // Motor de reglas integrado
	finalizado    chan struct{}     // Canal para señalizar cierre del gestor
}

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

// escribirPuntoIngesta escribe un punto al espacio de nombres de ingesta de una serie
func (me *GestorBorde) escribirPuntoIngesta(serieId int, medicion tipos.Medicion) error {
	clave := fmt.Sprintf("ingesta/%010d/%020d", serieId, medicion.Tiempo)
	datos, err := tipos.SerializarGob(medicion)
	if err != nil {
		return fmt.Errorf("error al serializar medición: %v", err)
	}
	return me.db.Set([]byte(clave), datos, pebble.Sync)
}

// leerPuntosIngesta lee los N puntos más antiguos del espacio de nombres de ingesta de una serie
func (me *GestorBorde) leerPuntosIngesta(serieId int, n int) ([]tipos.Medicion, error) {
	prefijo := fmt.Sprintf("ingesta/%010d/", serieId)
	iter, err := me.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefijo),
		UpperBound: []byte(fmt.Sprintf("ingesta/%010d0", serieId)),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var mediciones []tipos.Medicion
	for iter.First(); iter.Valid() && len(mediciones) < n; iter.Next() {
		var medicion tipos.Medicion
		if err := tipos.DeserializarGob(iter.Value(), &medicion); err != nil {
			continue // Skip mediciones con error
		}
		mediciones = append(mediciones, medicion)
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	return mediciones, nil
}

// leerPuntosIngestaEnRango lee todos los puntos de ingesta de una serie en un rango temporal inclusivo.
func (me *GestorBorde) leerPuntosIngestaEnRango(serieId int, tiempoInicio, tiempoFin int64) ([]tipos.Medicion, error) {
	if tiempoInicio > tiempoFin {
		return []tipos.Medicion{}, nil
	}

	lowerBound := []byte(fmt.Sprintf("ingesta/%010d/%020d", serieId, tiempoInicio))
	upperBound := []byte(fmt.Sprintf("ingesta/%010d/%020d~", serieId, tiempoFin))

	iter, err := me.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var mediciones []tipos.Medicion
	for iter.First(); iter.Valid(); iter.Next() {
		var medicion tipos.Medicion
		if err := tipos.DeserializarGob(iter.Value(), &medicion); err != nil {
			continue
		}
		mediciones = append(mediciones, medicion)
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	return mediciones, nil
}

// leerUltimoPuntoIngesta retorna la medicion mas reciente en ingesta para una serie.
func (me *GestorBorde) leerUltimoPuntoIngesta(serieId int) (tipos.Medicion, bool, error) {
	prefijo := fmt.Sprintf("ingesta/%010d/", serieId)
	iter, err := me.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefijo),
		UpperBound: []byte(fmt.Sprintf("ingesta/%010d0", serieId)),
	})
	if err != nil {
		return tipos.Medicion{}, false, err
	}
	defer iter.Close()

	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return tipos.Medicion{}, false, err
		}
		return tipos.Medicion{}, false, nil
	}

	var medicion tipos.Medicion
	if err := tipos.DeserializarGob(iter.Value(), &medicion); err != nil {
		return tipos.Medicion{}, false, err
	}

	return medicion, true, nil
}

// eliminarPuntosIngesta elimina un conjunto de puntos del espacio de nombres de ingesta usando un batch
func (me *GestorBorde) eliminarPuntosIngesta(serieId int, timestamps []int64) error {
	batch := me.db.NewBatch()
	defer batch.Close()

	for _, timestamp := range timestamps {
		clave := []byte(fmt.Sprintf("ingesta/%010d/%020d", serieId, timestamp))
		if err := batch.Delete(clave, nil); err != nil {
			return err
		}
	}

	return me.db.Apply(batch, pebble.Sync)
}

// contarPuntosIngesta cuenta los puntos en el espacio de nombres de ingesta de una serie
func (me *GestorBorde) contarPuntosIngesta(serieId int) int {
	prefijo := fmt.Sprintf("ingesta/%010d/", serieId)
	iter, err := me.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefijo),
		UpperBound: []byte(fmt.Sprintf("ingesta/%010d0", serieId)),
	})
	if err != nil {
		return 0
	}
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	return count
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
		case tipos.SinCompresion:
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
		case tipos.SinCompresion:
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
		case tipos.SinCompresion:
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
		case tipos.SinCompresion:
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
	bloqueFinal, _ := compresorBloque.Comprimir(bloqueNivel1)

	return bloqueFinal, nil
}

// Opciones configura la creación de un GestorBorde.
// ConfigS3 es opcional (nil = modo desconectado sin sincronización con nube).
type Opciones struct {
	NombreDB   string                 // Nombre de la base de datos Pebble (requerido)
	Direccion  string                 // Dirección pública para API REST (se debe pasar, SensorWave es agnóstico en cuanto a que se usa)
	PuertoHTTP string                 // Puerto HTTP para API REST (requerido solo si ConfigS3 != nil)
	ConfigS3   *tipos.ConfiguracionS3 // nil = modo local sin nube (debe ser explícito si se usa)
	Tags       map[string]string      // Metadatos libres del nodo (nombre, ubicación, etc.)
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

	// Validar puerto HTTP según configuración de S3
	var puertoHTTP string
	var err error
	if opts.ConfigS3 != nil {
		// Con S3: puerto HTTP es requerido
		if opts.PuertoHTTP == "" {
			return &GestorBorde{}, fmt.Errorf("PuertoHTTP es requerido cuando ConfigS3 está configurado")
		}
		puertoHTTP, err = validarPuertoHTTP(opts.PuertoHTTP)
		if err != nil {
			return &GestorBorde{}, err
		}
	} else {
		// Sin S3: puerto HTTP no debe especificarse
		if opts.PuertoHTTP != "" {
			return &GestorBorde{}, fmt.Errorf("PuertoHTTP no debe especificarse sin ConfigS3 (no tiene sentido exponer HTTP sin registro en nube)")
		}
	}

	// Abrir o crear la base de datos Pebble local
	db, err := pebble.Open(opts.NombreDB, &pebble.Options{})
	if err != nil {
		return &GestorBorde{}, err
	}

	// Inicializar el GestorBorde (datos básicos)
	gestor := &GestorBorde{
		db:         db,
		direccion:  opts.Direccion,
		puertoHTTP: puertoHTTP,
		tags:       opts.Tags,
		cache:      &Cache{datos: make(map[string]tipos.Serie)},
		finalizado: make(chan struct{}),
		contador:   0,
	}

	// Cargar o generar nodoID
	nodoIDBytes, closer, err := db.Get([]byte("metadatos/nodo_id"))
	// Si no existe, generar uno nuevo
	if err == pebble.ErrNotFound {
		gestor.nodoID = generarNodoID()
		err = db.Set([]byte("metadatos/nodo_id"), []byte(gestor.nodoID), pebble.Sync)
		if err != nil {
			return &GestorBorde{}, fmt.Errorf("error al guardar nodo_id: %v", err)
		}
		log.Printf("Nuevo nodoID generado: %s", gestor.nodoID)
	} else if err != nil {
		// Error al leer nodoID existente
		return &GestorBorde{}, fmt.Errorf("error al leer nodo_id: %v", err)
	} else {
		// Cargar nodoID existente
		gestor.nodoID = string(nodoIDBytes)
		closer.Close()
		log.Printf("NodoID cargado desde DB: %s", gestor.nodoID)
	}

	// Cargar o actualizar tags del nodo
	// Opción A: Si se especifican tags en opciones, sobrescriben los guardados
	if opts.Tags != nil {
		// Guardar los nuevos tags
		tagsBytes, err := tipos.SerializarGob(opts.Tags)
		if err != nil {
			return &GestorBorde{}, fmt.Errorf("error al serializar tags: %v", err)
		}
		err = db.Set([]byte("metadatos/tags"), tagsBytes, pebble.Sync)
		if err != nil {
			return &GestorBorde{}, fmt.Errorf("error al guardar tags: %v", err)
		}
		gestor.tags = opts.Tags
		log.Printf("Tags actualizados: %v", opts.Tags)
	} else {
		// Cargar tags existentes desde PebbleDB
		tagsBytes, closer, err := db.Get([]byte("metadatos/tags"))
		if err == nil {
			var tagsGuardados map[string]string
			if err := tipos.DeserializarGob(tagsBytes, &tagsGuardados); err == nil {
				gestor.tags = tagsGuardados
				log.Printf("Tags cargados desde DB: %v", tagsGuardados)
			}
			closer.Close()
		} else if err != pebble.ErrNotFound {
			return &GestorBorde{}, fmt.Errorf("error al leer tags: %v", err)
		}
		// Si no hay tags guardados, gestor.tags queda nil (ya asignado arriba)
	}

	// Cargar contador de series desde PebbleDB
	contadorBytes, closer, err := db.Get([]byte("metadatos/contador"))
	if err != nil && err != pebble.ErrNotFound {
		return &GestorBorde{}, fmt.Errorf("error al leer contador: %v", err)
	}
	if err == nil {
		// Cargar contador existente
		if len(contadorBytes) >= 4 {
			gestor.contador = int(binary.LittleEndian.Uint32(contadorBytes))
		}
		closer.Close()
	}

	// Cargar series existentes desde PebbleDB
	err = gestor.cargarSeriesExistentes()
	if err != nil {
		return &GestorBorde{}, fmt.Errorf("error al cargar series: %v", err)
	}

	// Reconstruir contadores de ingesta desde Pebble
	err = gestor.reconstruirContadoresIngesta()
	if err != nil {
		return &GestorBorde{}, fmt.Errorf("error al reconstruir contadores de ingesta: %v", err)
	}

	// Configurar S3 si se proporciona configuración
	if opts.ConfigS3 != nil {
		// Aplicar defaults y validar
		opts.ConfigS3.AplicarDefaults()
		if err := opts.ConfigS3.Validar(); err != nil {
			return &GestorBorde{}, fmt.Errorf("configuración S3 inválida: %w", err)
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
		return &GestorBorde{}, fmt.Errorf("error al cargar reglas: %v", err)
	}

	// Si S3 está configurado y se pudo conectar, registrar el nodo (incluye reglas)
	if clienteS3 != nil {
		if err := gestor.registrarEnS3(); err != nil {
			log.Printf("Advertencia: error registrando nodo en S3: %v", err)
		}
		// Iniciar limpieza automática de S3 (eliminaciones pendientes)
		gestor.iniciarLimpiezaS3Automatica()
	}

	// Iniciar servidor HTTP solo si hay puerto configurado (modo conectado con S3)
	if puertoHTTP != "" {
		listoHTTP, err := gestor.iniciarServidorHTTP()
		if err != nil {
    		db.Close()
    		return nil, fmt.Errorf("modo borde-nube requiere servidor HTTP: %w", err)
		}
		<-listoHTTP
	}
	
	return gestor, nil
}

// ObtenerNodeID retorna el ID del nodo borde
func (me *GestorBorde) ObtenerNodoID() string {
	return me.nodoID
}

// ObtenerTags retorna los tags del nodo borde
func (me *GestorBorde) ObtenerTags() map[string]string {
	return me.tags
}

// ActualizarTags actualiza los tags del nodo y los persiste en PebbleDB.
// Si tags es nil, elimina todos los tags guardados.
func (me *GestorBorde) ActualizarTags(tags map[string]string) error {
	if tags == nil {
		// Eliminar tags de PebbleDB
		err := me.db.Delete([]byte("metadatos/tags"), pebble.Sync)
		if err != nil && err != pebble.ErrNotFound {
			return fmt.Errorf("error al eliminar tags: %v", err)
		}
		me.tags = nil
		log.Printf("Tags eliminados")
	} else {
		// Guardar nuevos tags
		tagsBytes, err := tipos.SerializarGob(tags)
		if err != nil {
			return fmt.Errorf("error al serializar tags: %v", err)
		}
		err = me.db.Set([]byte("metadatos/tags"), tagsBytes, pebble.Sync)
		if err != nil {
			return fmt.Errorf("error al guardar tags: %v", err)
		}
		me.tags = tags
		log.Printf("Tags actualizados: %v", tags)
	}

	// Sincronizar registro en S3 en modo best-effort
	if clienteS3 != nil {
		if err := me.registrarEnS3(); err != nil {
			log.Printf("Advertencia: error actualizando tags en registro S3: %v", err)
		}
	}

	return nil
}

// cargarSeriesExistentes carga todas las series desde PebbleDB al cache
func (me *GestorBorde) cargarSeriesExistentes() error {
	iter, err := me.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("series/"),
		UpperBound: []byte("series0"), // Rango que incluye todas las claves "series/*"
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		clave := string(iter.Key())
		if !strings.HasPrefix(clave, "series/") {
			continue
		}

		// Extraer path de la clave
		seriesPath := strings.TrimPrefix(clave, "series/")
		if seriesPath == "" {
			continue
		}

		// Deserializar configuración de serie
		var config tipos.Serie
		if err := tipos.DeserializarGob(iter.Value(), &config); err != nil {
			continue // Skip series con error de deserialización
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
		go me.coordinarCompresion(coordinador)
	}

	return iter.Error()
}

// Cerrar cierra la conexión a PebbleDB y todos los goroutines asociados
func (me *GestorBorde) Cerrar() {
	// Señalar a todos los goroutines que deben terminar
	close(me.finalizado)

	// Cerrar todos los coordinadores individuales
	me.coordinadores.Range(func(clave, valor interface{}) bool {
		cs := valor.(*CoordinadorSerie)
		close(cs.finalizado)
		return true
	})

	// Cerrar PebbleDB
	me.db.Close()
}

// CrearSerie crea una nueva serie si no existe. Si ya existe, no hace nada.
func (me *GestorBorde) CrearSerie(config tipos.Serie) error {
	// Validar campos obligatorios
	if config.Path == "" {
		return fmt.Errorf("el path de la serie no puede estar vacío")
	}

	// Validar que el path sea correcto
	if !esPathValido(config.Path) {
		return fmt.Errorf("el path de la serie tiene un formato inválido: %s", config.Path)
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
		return fmt.Errorf("tipo de datos inválido: %s, debe ser Boolean, Integer, Real o Text", config.TipoDatos)
	}

	// Validar TamañoBloque
	// se encuentra en el rango de (0, 10000]
	if config.TamañoBloque <= 0 || config.TamañoBloque > 10000 {
		return fmt.Errorf("el tamaño del bloque debe ubicarse en el rango de(0, 10000], recibido: %d", config.TamañoBloque)
	}

	// Validar CompresionBloque
	compresionBloqueValida := false
	for _, compresion := range []tipos.TipoCompresionBloque{
		tipos.Ninguna, tipos.LZ4, tipos.ZSTD, tipos.Snappy, tipos.Gzip} {
		if config.CompresionBloque == compresion {
			compresionBloqueValida = true
			break
		}
	}
	if !compresionBloqueValida {
		return fmt.Errorf("tipo de compresión de bloque inválido: %s", config.CompresionBloque)
	}

	// Validar CompresionBytes - usar validación automática del sistema de tipos
	if err := config.TipoDatos.ValidarCompresion(config.CompresionBytes); err != nil {
		return fmt.Errorf("error en compresión de bytes: %v", err)
	}

	// Generar clave única basada en Path
	serieClave := config.Path

	// Inicializar Tags si es nil
	if config.Tags == nil {
		config.Tags = make(map[string]string)
	}

	// Verificar si la serie ya existe en cache
	me.cache.mu.RLock()
	if _, existe := me.cache.datos[serieClave]; existe {
		me.cache.mu.RUnlock()
		return nil
	}
	me.cache.mu.RUnlock()

	// Verificar si existe en PebbleDB
	clave := []byte("series/" + serieClave)
	_, closer, err := me.db.Get(clave)
	if err == nil {
		closer.Close()
		return nil // Serie ya existe
	}
	if err != pebble.ErrNotFound {
		return fmt.Errorf("error al verificar serie: %v", err)
	}

	// Generar nuevo ID para la serie
	me.mu.Lock()
	me.contador++
	config.SerieId = me.contador

	// Actualizar contador en PebbleDB
	contadorBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(contadorBytes, uint32(me.contador))
	err = me.db.Set([]byte("metadatos/contador"), contadorBytes, pebble.Sync)
	me.mu.Unlock()

	if err != nil {
		return fmt.Errorf("error al actualizar contador: %v", err)
	}

	// Serializar y guardar configuración de serie
	serieBytes, err := tipos.SerializarGob(config)
	if err != nil {
		return fmt.Errorf("error al serializar serie: %v", err)
	}

	err = me.db.Set(clave, serieBytes, pebble.Sync)
	if err != nil {
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
	go me.coordinarCompresion(coordinador)

	// Registrar nodo actualizado en S3 si está configurado
	if clienteS3 != nil {
		err = me.registrarEnS3()
		if err != nil {
			log.Printf("Error registrando serie nueva en S3: %v", err)
		}
	}

	return nil
}

// coordinarCompresion coordina la compresión asíncrona de datos desde el WAL
func (me *GestorBorde) coordinarCompresion(cs *CoordinadorSerie) {
	for {
		select {
		case <-cs.finalizado:
			return
		case <-me.finalizado:
			return
		case <-cs.notificarCompresion:
			cs.mu.Lock()

			// Leer puntos del WAL
			puntos, err := me.leerPuntosIngesta(cs.serie.SerieId, cs.serie.TamañoBloque)
			if err != nil || len(puntos) == 0 {
				cs.mu.Unlock()
				continue
			}

			// Comprimir puntos
			bloqueComprimido, err := me.comprimirPuntos(puntos, cs.serie)
			if err != nil {
				fmt.Printf("Error al comprimir puntos para serie %s: %v\n", cs.serie.Path, err)
				cs.mu.Unlock()
				continue
			}

			// Escribir bloque comprimido
			tiempoInicio := puntos[0].Tiempo
			tiempoFinal := puntos[len(puntos)-1].Tiempo
			clave := generarClaveDatos(cs.serie.SerieId, tiempoInicio, tiempoFinal)
			err = me.db.Set(clave, bloqueComprimido, pebble.Sync)
			if err != nil {
				fmt.Printf("Error al escribir bloque para serie %s: %v\n", cs.serie.Path, err)
				cs.mu.Unlock()
				continue
			}

			fmt.Println("Almacenando bloque para serie:", cs.serie.Path,
				"Tiempo inicio:", tiempoInicio,
				"Tiempo final:", tiempoFinal,
				"Mediciones:", len(puntos),
				"Tamaño comprimido:", len(bloqueComprimido))

			// Eliminar puntos del WAL
			timestamps := extraerTimestamps(puntos)
			if err := me.eliminarPuntosIngesta(cs.serie.SerieId, timestamps); err != nil {
				fmt.Printf("Error al eliminar puntos del WAL para serie %s: %v\n", cs.serie.Path, err)
			}

			// Decrementar contador
			cs.decrementarContador(len(puntos))

			cs.mu.Unlock()
		}
	}
}

// Insertar agrega un nuevo dato a la serie especificada
func (me *GestorBorde) Insertar(path string, tiempo int64, dato interface{}) error {
	// Obtener el coordinador para la serie
	csInterface, ok := me.coordinadores.Load(path)
	if !ok {
		return fmt.Errorf("serie no encontrada: %s", path)
	}

	cs := csInterface.(*CoordinadorSerie)

	// Validar compatibilidad de tipo
	if !esCompatibleConTipo(dato, cs.serie.TipoDatos) {
		return fmt.Errorf("tipo de dato incompatible: esperado %s, recibido %T",
			cs.serie.TipoDatos, dato)
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
// Si S3 está configurado, registra la eliminación pendiente y la procesa (best-effort).
// La eliminación local siempre se completa; la eliminación de S3 se reintentará automáticamente.
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

	// 3. Eliminar puntos del WAL
	prefijoWAL := fmt.Sprintf("ingesta/%010d/", serieId)
	iterWAL, err := me.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefijoWAL),
		UpperBound: []byte(fmt.Sprintf("ingesta/%010d0", serieId)),
	})
	if err != nil {
		return fmt.Errorf("error al crear iterador para WAL: %v", err)
	}

	var clavesWAL [][]byte
	for iterWAL.First(); iterWAL.Valid(); iterWAL.Next() {
		clave := make([]byte, len(iterWAL.Key()))
		copy(clave, iterWAL.Key())
		clavesWAL = append(clavesWAL, clave)
	}
	iterWAL.Close()

	for _, clave := range clavesWAL {
		if err := me.db.Delete(clave, pebble.Sync); err != nil {
			log.Printf("Advertencia: error al eliminar punto WAL %s: %v", string(clave), err)
		}
	}

	// 3. Eliminar todos los bloques de datos de PebbleDB
	prefijoDatos := fmt.Sprintf("datos/%010d/", serieId)

	iter, err := me.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefijoDatos),
		UpperBound: []byte(fmt.Sprintf("datos/%010d0", serieId)), // Siguiente serie
	})
	if err != nil {
		return fmt.Errorf("error al crear iterador para datos: %v", err)
	}

	// Recolectar claves a eliminar
	var clavesAEliminar [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		clave := make([]byte, len(iter.Key()))
		copy(clave, iter.Key())
		clavesAEliminar = append(clavesAEliminar, clave)
	}
	iter.Close()

	// Eliminar bloques de datos
	for _, clave := range clavesAEliminar {
		if err := me.db.Delete(clave, pebble.Sync); err != nil {
			log.Printf("Advertencia: error al eliminar bloque %s: %v", string(clave), err)
		}
	}

	// 4. Eliminar metadatos de la serie
	claveSerie := []byte("series/" + path)
	if err := me.db.Delete(claveSerie, pebble.Sync); err != nil {
		return fmt.Errorf("error al eliminar metadatos de serie: %v", err)
	}

	// 5. Eliminar del cache
	me.cache.mu.Lock()
	delete(me.cache.datos, path)
	me.cache.mu.Unlock()

	log.Printf("Serie eliminada localmente: %s (ID: %d, bloques eliminados: %d)", path, serieId, len(clavesAEliminar))

	// La eliminación de S3 se procesa automáticamente vía iniciarLimpiezaS3Automatica()
	// que ejecuta procesarEliminacionesPendientes() cada 5 minutos.
	// La eliminación pendiente ya fue registrada en el paso 1 (si S3 estaba configurado).

	return nil
}
