package edge

import (
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/sensorwave-dev/sensorwave/compresor"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

type ManagerEdge struct {
	nodoID        string            // ID único del nodo edge
	direccion     string            // dirección pública para uso de API REST
	puertoHTTP    string            // Puerto HTTP para la API REST
	tags          map[string]string // Metadatos libres del nodo (nombre, ubicación, etc.)
	db            *pebble.DB        // Base de datos Pebble local
	cache         *Cache            // Cache en memoria de configuraciones de series
	buffers       sync.Map          // Map de buffers de series en memoria
	mu            sync.RWMutex      // Mutex para proteger el contador
	contador      int               // Contador para generar IDs únicos de series
	MotorReglas   *MotorReglas      // Motor de reglas integrado
	tamañoBuffer  int               // Tamaño del buffer de canales (default 1000)
	timeoutBuffer int64             // Timeout para inserción en nanosegundos (default 100ms)
	done          chan struct{}     // Canal para señalizar cierre del manager
}

type Cache struct {
	datos map[string]tipos.Serie // Mapa para almacenar las configuraciones de series
	mu    sync.RWMutex           // Mutex para proteger el acceso concurrente
}

type SerieBuffer struct {
	datos      []tipos.Medicion    // Arreglo con TamañoBloque elementos
	serie      tipos.Serie         // Configuración de la serie
	indice     int                 // Índice actual en el buffer
	mu         sync.Mutex          // Mutex para proteger el buffer
	done       chan struct{}       // Canal para señalar cierre del hilo
	datosCanal chan tipos.Medicion // Canal para recibir nuevos datos
}

// Opciones configura la creación de un ManagerEdge.
// ConfigS3 es opcional (nil = modo desconectado sin sincronización con nube).
type Opciones struct {
	NombreDB      string                 // Nombre de la base de datos Pebble (requerido)
	Direccion     string                 // Dirección pública para API REST (se debe pasar, SensorWave es agnóstico en cuanto a que se usa)
	PuertoHTTP    string                 // Puerto HTTP para API REST (requerido solo si ConfigS3 != nil)
	ConfigS3      *tipos.ConfiguracionS3 // nil = modo local sin nube (debe ser explícito si se usa)
	TamañoBuffer  int                    // Tamaño del canal de buffer por serie (default: 1000)
	TimeoutBuffer int64                  // Timeout en nanosegundos para inserción (default: 100ms)
	Tags          map[string]string      // Metadatos libres del nodo (nombre, ubicación, etc.)
}

// Crear inicializa el ManagerEdge con las opciones especificadas.
// ConfigS3 es opcional: si es nil, funciona en modo local sin registro en nube.
// Si se proporciona, debe ser una configuración completa y válida.
func Crear(opts Opciones) (*ManagerEdge, error) {

	// Validar opciones requeridas
	if opts.NombreDB == "" {
		return &ManagerEdge{}, fmt.Errorf("NombreDB es requerido")
	}

	// Validar opts.Dirección pública no sea vacía
	if opts.Direccion == "" {
		return &ManagerEdge{}, fmt.Errorf("Dirección es requerida")
	}

	// Validar puerto HTTP según configuración de S3
	var puertoHTTP string
	var err error
	if opts.ConfigS3 != nil {
		// Con S3: puerto HTTP es requerido
		if opts.PuertoHTTP == "" {
			return &ManagerEdge{}, fmt.Errorf("PuertoHTTP es requerido cuando ConfigS3 está configurado")
		}
		puertoHTTP, err = validarPuertoHTTP(opts.PuertoHTTP)
		if err != nil {
			return &ManagerEdge{}, err
		}
	} else {
		// Sin S3: puerto HTTP no debe especificarse
		if opts.PuertoHTTP != "" {
			return &ManagerEdge{}, fmt.Errorf("PuertoHTTP no debe especificarse sin ConfigS3 (no tiene sentido exponer HTTP sin registro en nube)")
		}
	}

	// Aplicar defaults para buffer
	tamañoBuffer := opts.TamañoBuffer
	if tamañoBuffer <= 0 {
		tamañoBuffer = 1000 // Default: 1000 mediciones por serie
	}

	timeoutBuffer := opts.TimeoutBuffer
	if timeoutBuffer <= 0 {
		timeoutBuffer = 100 * 1000 * 1000 // Default: 100ms en nanosegundos
	}

	// Abrir o crear la base de datos Pebble local
	db, err := pebble.Open(opts.NombreDB, &pebble.Options{})
	if err != nil {
		return &ManagerEdge{}, err
	}

	// Inicializar el ManagerEdge (datos básicos)
	manager := &ManagerEdge{
		db:            db,
		direccion:     opts.Direccion,
		puertoHTTP:    puertoHTTP,
		tags:          opts.Tags,
		cache:         &Cache{datos: make(map[string]tipos.Serie)},
		done:          make(chan struct{}),
		contador:      0,
		tamañoBuffer:  tamañoBuffer,
		timeoutBuffer: timeoutBuffer,
	}

	// Cargar o generar nodoID
	nodoIDBytes, closer, err := db.Get([]byte("meta/nodo_id"))
	// Si no existe, generar uno nuevo
	if err == pebble.ErrNotFound {
		manager.nodoID = generarNodoID()
		err = db.Set([]byte("meta/nodo_id"), []byte(manager.nodoID), pebble.Sync)
		if err != nil {
			return &ManagerEdge{}, fmt.Errorf("error al guardar nodo_id: %v", err)
		}
		log.Printf("Nuevo nodoID generado: %s", manager.nodoID)
	} else if err != nil {
		// Error al leer nodoID existente
		return &ManagerEdge{}, fmt.Errorf("error al leer nodo_id: %v", err)
	} else {
		// Cargar nodoID existente
		manager.nodoID = string(nodoIDBytes)
		closer.Close()
		log.Printf("NodoID cargado desde DB: %s", manager.nodoID)
	}

	// Cargar o actualizar tags del nodo
	// Opción A: Si se especifican tags en opciones, sobrescriben los guardados
	if opts.Tags != nil {
		// Guardar los nuevos tags
		tagsBytes, err := tipos.SerializarGob(opts.Tags)
		if err != nil {
			return &ManagerEdge{}, fmt.Errorf("error al serializar tags: %v", err)
		}
		err = db.Set([]byte("meta/tags"), tagsBytes, pebble.Sync)
		if err != nil {
			return &ManagerEdge{}, fmt.Errorf("error al guardar tags: %v", err)
		}
		manager.tags = opts.Tags
		log.Printf("Tags actualizados: %v", opts.Tags)
	} else {
		// Cargar tags existentes desde PebbleDB
		tagsBytes, closer, err := db.Get([]byte("meta/tags"))
		if err == nil {
			var tagsGuardados map[string]string
			if err := tipos.DeserializarGob(tagsBytes, &tagsGuardados); err == nil {
				manager.tags = tagsGuardados
				log.Printf("Tags cargados desde DB: %v", tagsGuardados)
			}
			closer.Close()
		} else if err != pebble.ErrNotFound {
			return &ManagerEdge{}, fmt.Errorf("error al leer tags: %v", err)
		}
		// Si no hay tags guardados, manager.tags queda nil (ya asignado arriba)
	}

	// Cargar contador de series desde PebbleDB
	contadorBytes, closer, err := db.Get([]byte("meta/contador"))
	if err != nil && err != pebble.ErrNotFound {
		return &ManagerEdge{}, fmt.Errorf("error al leer contador: %v", err)
	}
	if err == nil {
		// Cargar contador existente
		if len(contadorBytes) >= 4 {
			manager.contador = int(binary.LittleEndian.Uint32(contadorBytes))
		}
		closer.Close()
	}

	// Cargar series existentes desde PebbleDB
	err = manager.cargarSeriesExistentes()
	if err != nil {
		return &ManagerEdge{}, fmt.Errorf("error al cargar series: %v", err)
	}

	// Configurar S3 si se proporciona configuración
	if opts.ConfigS3 != nil {
		// Aplicar defaults y validar
		opts.ConfigS3.AplicarDefaults()
		if err := opts.ConfigS3.Validar(); err != nil {
			return &ManagerEdge{}, fmt.Errorf("configuración S3 inválida: %w", err)
		}

		err = manager.ConfigurarS3(*opts.ConfigS3)
		if err != nil {
			log.Printf("Advertencia: error al configurar S3: %v", err)
			log.Printf("El nodo funcionará en modo local")
		}
	} else {
		log.Printf("Modo local: sin registro en nube ni servidor HTTP")
	}

	// Inicializar el motor de reglas integrado
	manager.MotorReglas = nuevoMotorReglasIntegrado(manager, db)
	// Cargar reglas existentes
	err = manager.MotorReglas.cargarReglasExistentes()
	if err != nil {
		return &ManagerEdge{}, fmt.Errorf("error al cargar reglas: %v", err)
	}

	// Si S3 está configurado y se pudo conectar, registrar el nodo (incluye reglas)
	if clienteS3 != nil {
		if err := manager.RegistrarEnS3(); err != nil {
			log.Printf("Advertencia: error registrando nodo en S3: %v", err)
		}
		// Iniciar limpieza automática de S3 (eliminaciones pendientes)
		manager.IniciarLimpiezaS3Automatica()
	}

	// Iniciar servidor HTTP solo si hay puerto configurado (modo conectado con S3)
	if puertoHTTP != "" {
		listoHTTP := manager.iniciarServidorHTTP()
		<-listoHTTP
	}

	return manager, nil
}

// ObtenerNodeID retorna el ID del nodo edge
func (me *ManagerEdge) ObtenerNodoID() string {
	return me.nodoID
}

// ObtenerTags retorna los tags del nodo edge
func (me *ManagerEdge) ObtenerTags() map[string]string {
	return me.tags
}

// ActualizarTags actualiza los tags del nodo y los persiste en PebbleDB.
// Si tags es nil, elimina todos los tags guardados.
func (me *ManagerEdge) ActualizarTags(tags map[string]string) error {
	if tags == nil {
		// Eliminar tags de PebbleDB
		err := me.db.Delete([]byte("meta/tags"), pebble.Sync)
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
		err = me.db.Set([]byte("meta/tags"), tagsBytes, pebble.Sync)
		if err != nil {
			return fmt.Errorf("error al guardar tags: %v", err)
		}
		me.tags = tags
		log.Printf("Tags actualizados: %v", tags)
	}

	// Actualizar registro en S3 si está configurado
	if clienteS3 != nil {
		if err := me.RegistrarEnS3(); err != nil {
			log.Printf("Advertencia: error actualizando tags en S3: %v", err)
		}
	}

	return nil
}

// cargarSeriesExistentes carga todas las series desde PebbleDB al cache
func (me *ManagerEdge) cargarSeriesExistentes() error {
	iter, err := me.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("series/"),
		UpperBound: []byte("series0"), // Rango que incluye todas las claves "series/*"
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if !strings.HasPrefix(key, "series/") {
			continue
		}

		// Extraer path de la clave
		seriesPath := strings.TrimPrefix(key, "series/")
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

		// Crear buffer y goroutine para cada serie
		serieBuffer := &SerieBuffer{
			datos:      make([]tipos.Medicion, config.TamañoBloque),
			serie:      config,
			indice:     0,
			done:       make(chan struct{}),
			datosCanal: make(chan tipos.Medicion, me.tamañoBuffer),
		}

		me.buffers.Store(seriesPath, serieBuffer)
		go me.manejarBuffer(serieBuffer)
	}

	return iter.Error()
}

// Cerrar cierra la conexión a PebbleDB y todos los goroutines asociados
func (me *ManagerEdge) Cerrar() {
	// Señalar a todos los goroutines que deben terminar
	close(me.done)

	// Cerrar todos los buffers individuales
	me.buffers.Range(func(key, value interface{}) bool {
		buffer := value.(*SerieBuffer)
		close(buffer.done)
		return true
	})

	// Cerrar PebbleDB
	me.db.Close()
}

// CrearSerie crea una nueva serie si no existe. Si ya existe, no hace nada.
func (me *ManagerEdge) CrearSerie(config tipos.Serie) error {
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
	err = me.db.Set([]byte("meta/contador"), contadorBytes, pebble.Sync)
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

	// Crear buffer y goroutine para la nueva serie
	buffer := &SerieBuffer{
		datos:      make([]tipos.Medicion, config.TamañoBloque),
		serie:      config,
		indice:     0,
		done:       make(chan struct{}),
		datosCanal: make(chan tipos.Medicion, me.tamañoBuffer),
	}

	me.buffers.Store(serieClave, buffer)
	go me.manejarBuffer(buffer)

	// Registrar nodo actualizado en S3 si está configurado
	if clienteS3 != nil {
		err = me.RegistrarEnS3()
		if err != nil {
			log.Printf("Error registrando serie nueva en S3: %v", err)
		}
	}

	return nil
}

// manejarBuffer maneja la inserción y almacenamiento de datos en el buffer de una serie
func (me *ManagerEdge) manejarBuffer(buffer *SerieBuffer) {
	for {
		select {
		case <-buffer.done:
			return
		case <-me.done:
			return
		case medicion := <-buffer.datosCanal:
			buffer.mu.Lock()
			// Agregar la medición al buffer
			buffer.datos[buffer.indice] = medicion
			buffer.indice++

			// Verificar si el buffer está lleno
			if buffer.indice >= buffer.serie.TamañoBloque {
				// Comprimir y almacenar el buffer
				me.comprimirYAlmacenar(buffer)
				// Limpiar el buffer
				buffer.indice = 0
				// Limpiar los datos (opcional, se sobreescribirán)
				for i := range buffer.datos {
					buffer.datos[i] = tipos.Medicion{}
				}
			}
			buffer.mu.Unlock()
		}
	}
}

// Insertar agrega un nuevo dato a la serie especificada
func (me *ManagerEdge) Insertar(path string, tiempo int64, dato interface{}) error {
	// Obtener el buffer para la serie
	bufferInterface, ok := me.buffers.Load(path)
	if !ok {
		return fmt.Errorf("serie no encontrada: %s", path)
	}

	buffer := bufferInterface.(*SerieBuffer)

	// Validar compatibilidad de tipo
	if !esCompatibleConTipo(dato, buffer.serie.TipoDatos) {
		return fmt.Errorf("tipo de dato incompatible: esperado %s, recibido %T",
			buffer.serie.TipoDatos, dato)
	}

	// Crear la medición con tiempo y valor
	medicion := tipos.Medicion{
		Tiempo: tiempo,
		Valor:  dato,
	}

	// Enviar la medición al canal del buffer con timeout
	select {
	case buffer.datosCanal <- medicion:
		// Evaluar reglas de forma síncrona después de inserción exitosa
		me.MotorReglas.evaluarReglas(time.Unix(0, tiempo))
		return nil
	case <-time.After(time.Duration(me.timeoutBuffer)):
		return fmt.Errorf("timeout (%v): buffer saturado para serie %s",
			time.Duration(me.timeoutBuffer), path)
	}
}

// descomprimirBloque descomprime un bloque de datos usando la función pública del compresor
func (me *ManagerEdge) descomprimirBloque(datosComprimidos []byte, serie tipos.Serie) ([]tipos.Medicion, error) {
	return compresor.DescomprimirBloqueSerie(datosComprimidos, serie.TipoDatos, serie.CompresionBytes, serie.CompresionBloque)
}

func (me *ManagerEdge) comprimirYAlmacenar(buffer *SerieBuffer) {
	// Obtener mediciones válidas del buffer
	mediciones := buffer.datos[:buffer.indice]
	if len(mediciones) == 0 {
		return
	}

	// NIVEL 1: Compresión específica
	// Tiempo: SIEMPRE usar DeltaDelta
	tiemposComprimidos := compresor.CompresionDeltaDeltaTiempo(mediciones)

	// Valores: usar compresión configurada según tipo de datos
	valores := compresor.ExtraerValores(mediciones)
	var valoresComprimidos []byte
	var err error

	switch buffer.serie.TipoDatos {
	case tipos.Integer:
		// Convertir valores a int64
		valoresInt, errConv := compresor.ConvertirAInt64Array(valores)
		if errConv != nil {
			fmt.Printf("Error al convertir valores a int64: %v\n", errConv)
			return
		}

		// Comprimir según algoritmo configurado
		switch buffer.serie.CompresionBytes {
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
			fmt.Printf("Compresión no soportada para tipo Integer: %v\n", buffer.serie.CompresionBytes)
			return
		}

	case tipos.Real:
		// Convertir valores a float64
		valoresFloat, errConv := compresor.ConvertirAFloat64Array(valores)
		if errConv != nil {
			fmt.Printf("Error al convertir valores a float64: %v\n", errConv)
			return
		}

		// Comprimir según algoritmo configurado
		switch buffer.serie.CompresionBytes {
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
			fmt.Printf("Compresión no soportada para tipo Real: %v\n", buffer.serie.CompresionBytes)
			return
		}

	case tipos.Boolean:
		// Convertir valores a bool
		valoresBool, errConv := compresor.ConvertirABoolArray(valores)
		if errConv != nil {
			fmt.Printf("Error al convertir valores a bool: %v\n", errConv)
			return
		}

		// Comprimir según algoritmo configurado
		switch buffer.serie.CompresionBytes {
		case tipos.RLE:
			comp := &compresor.CompresorRLEGenerico[bool]{}
			valoresComprimidos, err = comp.Comprimir(valoresBool)
		case tipos.SinCompresion:
			comp := &compresor.CompresorNingunoGenerico[bool]{}
			valoresComprimidos, err = comp.Comprimir(valoresBool)
		default:
			fmt.Printf("Compresión no soportada para tipo Boolean: %v\n", buffer.serie.CompresionBytes)
			return
		}

	case tipos.Text:
		// Convertir valores a string
		valoresStr, errConv := compresor.ConvertirAStringArray(valores)
		if errConv != nil {
			fmt.Printf("Error al convertir valores a string: %v\n", errConv)
			return
		}

		// Comprimir según algoritmo configurado
		switch buffer.serie.CompresionBytes {
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
			fmt.Printf("Compresión no soportada para tipo Text: %v\n", buffer.serie.CompresionBytes)
			return
		}

	default:
		fmt.Printf("Tipo de datos no soportado: %v\n", buffer.serie.TipoDatos)
		return
	}

	if err != nil {
		fmt.Printf("Error al comprimir valores: %v\n", err)
		return
	}

	// Combinar datos del nivel 1
	bloqueNivel1 := compresor.CombinarDatos(tiemposComprimidos, valoresComprimidos)

	// NIVEL 2: Compresión de bloque
	compresorBloque := compresor.ObtenerCompresorBloque(buffer.serie.CompresionBloque)
	bloqueFinal, _ := compresorBloque.Comprimir(bloqueNivel1)

	// Calcular timestamps de inicio y fin
	tiempoInicio := mediciones[0].Tiempo
	tiempoFinal := mediciones[len(mediciones)-1].Tiempo

	fmt.Println("Almacenando bloque para serie:", buffer.serie.Path,
		"Tiempo inicio:", tiempoInicio,
		"Tiempo final:", tiempoFinal,
		"Mediciones:", len(mediciones),
		"Tamaño comprimido:", len(bloqueFinal))

	// Escribir directamente a PebbleDB (sin serialización)
	key := generarClaveDatos(buffer.serie.SerieId, tiempoInicio, tiempoFinal)
	err = me.db.Set(key, bloqueFinal, pebble.Sync)
	if err != nil {
		fmt.Printf("Error al escribir datos para serie %s: %v\n", buffer.serie.Path, err)
	}
}

func (me *ManagerEdge) AgregarRegla(regla *Regla) error {
	return me.MotorReglas.AgregarRegla(regla)
}

func (me *ManagerEdge) EliminarRegla(id string) error {
	return me.MotorReglas.EliminarRegla(id)
}

func (me *ManagerEdge) ActualizarRegla(regla *Regla) error {
	return me.MotorReglas.ActualizarRegla(regla)
}

func (me *ManagerEdge) RegistrarEjecutor(tipoAccion string, ejecutor EjecutorAccion) error {
	return me.MotorReglas.RegistrarEjecutor(tipoAccion, ejecutor)
}

func (me *ManagerEdge) ListarReglas() map[string]*Regla {
	return me.MotorReglas.ListarReglas()
}

func (me *ManagerEdge) HabilitarMotorReglas(habilitado bool) {
	me.MotorReglas.Habilitar(habilitado)
}

// ObtenerEstadoMotorReglas devuelve el estado actual del motor de reglas
func (me *ManagerEdge) ObtenerEstadoMotorReglas() EstadoMotorReglas {
	return me.MotorReglas.ObtenerEstado()
}

// EliminarSerie elimina una serie y todos sus datos asociados.
// Elimina: metadatos, bloques de datos locales, cache y buffer.
// Si S3 está configurado, registra la eliminación pendiente y la procesa (best-effort).
// La eliminación local siempre se completa; la eliminación de S3 se reintentará automáticamente.
func (me *ManagerEdge) EliminarSerie(path string) error {
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

	// 2. Cerrar el buffer y su goroutine
	if bufferInterface, ok := me.buffers.Load(path); ok {
		buffer := bufferInterface.(*SerieBuffer)

		// Señalar al goroutine que termine
		close(buffer.done)

		// Vaciar el canal para evitar bloqueos
		close(buffer.datosCanal)
		for range buffer.datosCanal {
			// Drenar el canal
		}

		// Eliminar del mapa de buffers
		me.buffers.Delete(path)
	}

	// 3. Eliminar todos los bloques de datos de PebbleDB
	prefijoDatos := fmt.Sprintf("data/%010d/", serieId)

	iter, err := me.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefijoDatos),
		UpperBound: []byte(fmt.Sprintf("data/%010d0", serieId)), // Siguiente serie
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

	// La eliminación de S3 se procesa automáticamente vía IniciarLimpiezaS3Automatica()
	// que ejecuta ProcesarEliminacionesPendientes() cada 5 minutos.
	// La eliminación pendiente ya fue registrada en el paso 1 (si S3 estaba configurado).

	return nil
}
