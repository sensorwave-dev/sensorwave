package borde

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sensorwave-dev/sensorwave/almacen"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

// clienteS3 es el cliente S3 para la migración
// Usa la interfaz tipos.ClienteS3 para permitir inyección de mocks en tests
var clienteS3 tipos.ClienteS3
var configuracionS3 tipos.ConfiguracionS3

// configurarS3 configura la conexión a almacenamiento S3-compatible
func (me *GestorBorde) configurarS3(cfg tipos.ConfiguracionS3) error {
	configuracionS3 = cfg

	// Crear cliente S3 usando la función centralizada
	cliente, err := tipos.CrearClienteS3(cfg)
	if err != nil {
		return err
	}
	clienteS3 = cliente

	// Verificar que el bucket existe, si no, intentar crearlo
	ctx := context.TODO()
	_, err = clienteS3.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	if err != nil && !tipos.BucketNoExiste(err) {
		return fmt.Errorf("error al verificar bucket: %v", err)
	}
	if err != nil {
		log.Printf("El bucket %s no existe, intentando crearlo...", cfg.Bucket)
		_, err = clienteS3.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(cfg.Bucket),
		})
		if err != nil {
			return fmt.Errorf("error al crear bucket: %v", err)
		}
		log.Printf("Bucket %s creado exitosamente", cfg.Bucket)
	}

	log.Printf("Conexión a S3 configurada exitosamente (endpoint: %s, bucket: %s)", cfg.Endpoint, cfg.Bucket)
	return nil
}

// migrarPorTiempoAlmacenamiento migra bloques de datos que excedan el tiempo de almacenamiento configurado
// para cada serie. Solo migra series que tengan TiempoAlmacenamiento > 0.
func (me *GestorBorde) migrarPorTiempoAlmacenamiento() error {
	// Verificar que S3 esté configurado
	if clienteS3 == nil {
		return fmt.Errorf("S3 no está configurado")
	}

	ctx := context.TODO()
	ahora := time.Now().UnixNano()
	contadorMigrados := 0
	contadorEliminados := 0

	// Obtener todas las series del cache
	me.cache.mu.RLock()
	seriesConTiempo := make([]tipos.Serie, 0)
	for _, serie := range me.cache.datos {
		if serie.TiempoAlmacenamiento > 0 {
			seriesConTiempo = append(seriesConTiempo, serie)
		}
	}
	me.cache.mu.RUnlock()

	if len(seriesConTiempo) == 0 {
		log.Printf("No hay series con tiempo de almacenamiento configurado")
		return nil
	}

	log.Printf("Iniciando migración por tiempo de almacenamiento para %d series", len(seriesConTiempo))

	// Procesar cada serie con tiempo de almacenamiento configurado
	for _, serie := range seriesConTiempo {
		tiempoLimite := ahora - serie.TiempoAlmacenamiento

		iter, err := me.db.BloquesEnRango(serie.SerieId, math.MinInt64, math.MaxInt64)
		if err != nil {
			log.Printf("Error creando iterador para serie %s: %v", serie.Path, err)
			continue
		}

		var aMigrar []almacen.Bloque
		for iter.Siguiente() {
			bloque := iter.Bloque()
			if bloque.Fin < tiempoLimite {
				aMigrar = append(aMigrar, bloque)
			}
		}
		iter.Close()

		for _, bloque := range aMigrar {
			nombreArchivo := tipos.GenerarClaveS3Datos(me.nodoID, bloque.SerieID, bloque.Inicio, bloque.Fin)

			_, err = clienteS3.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(configuracionS3.Bucket),
				Key:    aws.String(nombreArchivo),
				Body:   bytes.NewReader(bloque.Datos),
			})
			if err != nil {
				log.Printf("Error subiendo bloque a S3 (serie %d, %d-%d): %v", bloque.SerieID, bloque.Inicio, bloque.Fin, err)
				continue
			}
			contadorMigrados++

			err = me.db.BorrarBloque(bloque.SerieID, bloque.Inicio, bloque.Fin)
			if err != nil {
				log.Printf("Error eliminando bloque migrado: %v", err)
				continue
			}
			contadorEliminados++
		}

		if len(aMigrar) > 0 {
			log.Printf("Serie '%s': %d bloques migrados a S3", serie.Path, len(aMigrar))
		}
	}

	log.Printf("Migración por tiempo completada: %d bloques migrados, %d eliminados localmente",
		contadorMigrados, contadorEliminados)
	return nil
}

// parsearTiempoFinDeClave extrae el tiempoFin de una clave con formato "datos/{serieId}/{tiempoInicio}_{tiempoFin}"
func parsearTiempoFinDeClave(clave string) (int64, error) {
	// Formato: datos/0000000001/00000000000000000001_00000000000000000002
	partes := strings.Split(clave, "/")
	if len(partes) != 3 {
		return 0, fmt.Errorf("formato de clave inválido: %s", clave)
	}

	tiempos := strings.Split(partes[2], "_")
	if len(tiempos) != 2 {
		return 0, fmt.Errorf("formato de tiempos inválido: %s", partes[2])
	}

	tiempoFin, err := strconv.ParseInt(tiempos[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parseando tiempoFin: %v", err)
	}

	return tiempoFin, nil
}

// iniciarCicloS3 arranca el reintento de S3 cuando Crear dejó un cliente.
// La primera vuelta y cada IntervaloS3 vuelven a subir el catálogo por si un
// aviso inmediato no llegó, y procesan borrados pendientes y migración.
// Se detiene al cerrar el gestor.
func (me *GestorBorde) iniciarCicloS3() {
	me.fondo.Add(1)
	go func() {
		defer me.fondo.Done()
		me.ejecutarCicloS3()

		ticker := time.NewTicker(me.intervaloS3)
		defer ticker.Stop()

		for {
			select {
			case <-me.finalizado:
				log.Printf("Deteniendo ciclo de S3")
				return
			case <-ticker.C:
				me.ejecutarCicloS3()
			}
		}
	}()

	log.Printf("Ciclo de S3 iniciado (intervalo: %v)", me.intervaloS3)
}

// EjecutarCicloS3 corre una vuelta del ciclo: registro del nodo, borrados
// pendientes y migración de bloques vencidos.
func (me *GestorBorde) EjecutarCicloS3() {
	me.ejecutarCicloS3()
}

// ResumenAlmacenLocal cuenta los bloques de datos/, sus bytes y los puntos
// que siguen en ingesta.
func (me *GestorBorde) ResumenAlmacenLocal() (bloques int, nbytes int64, ingesta int, err error) {
	me.cache.mu.RLock()
	defer me.cache.mu.RUnlock()
	for _, serie := range me.cache.datos {
		iter, errIter := me.db.BloquesEnRango(serie.SerieId, math.MinInt64, math.MaxInt64)
		if errIter != nil {
			return 0, 0, 0, errIter
		}
		for iter.Siguiente() {
			bloques++
			nbytes += int64(len(iter.Bloque().Datos))
		}
		if errIter = iter.Error(); errIter != nil {
			iter.Close()
			return 0, 0, 0, errIter
		}
		iter.Close()
		ingesta += me.contarPuntosIngesta(serie.SerieId)
	}
	return bloques, nbytes, ingesta, nil
}

// ejecutarCicloS3 reintenta el catálogo, los borrados pendientes y la migración.
// El aviso principal ocurre al cambiar series, tags o reglas. Esta vuelta cubre
// las subidas que fallaron por conectividad.
func (me *GestorBorde) ejecutarCicloS3() {
	select {
	case <-me.finalizado:
		return
	default:
	}

	if clienteS3 == nil {
		return
	}

	if err := me.registrarEnS3(); err != nil {
		log.Printf("Advertencia: error registrando nodo en S3: %v", err)
	}
	if err := me.procesarEliminacionesPendientes(); err != nil {
		log.Printf("Error procesando eliminaciones pendientes de S3: %v", err)
	}
	if err := me.migrarPorTiempoAlmacenamiento(); err != nil {
		log.Printf("Error en migración automática: %v", err)
	}
}

// ============================================================================
// ELIMINACIÓN DE SERIES EN S3 (OFFLINE-FIRST)
// ============================================================================

// EliminacionPendiente representa una serie pendiente de eliminar de S3
type EliminacionPendiente struct {
	SerieId     int    // ID de la serie eliminada
	Path        string // Path original (para logging)
	MarcaTiempo int64  // Cuándo se solicitó la eliminación (UnixNano)
	Intentos    int    // Cantidad de intentos fallidos
}

// generarClaveEliminacionPendiente genera la clave para almacenar una eliminación pendiente
func generarClaveEliminacionPendiente(serieId int) []byte {
	return []byte(fmt.Sprintf("pendientes/eliminar/%010d", serieId))
}

// guardarEliminacionPendiente guarda una eliminación pendiente en PebbleDB
func (me *GestorBorde) guardarEliminacionPendiente(serieId int, path string) error {
	pendiente := EliminacionPendiente{
		SerieId:     serieId,
		Path:        path,
		MarcaTiempo: time.Now().UnixNano(),
		Intentos:    0,
	}

	datos, err := tipos.SerializarGob(pendiente)
	if err != nil {
		return fmt.Errorf("error serializando eliminación pendiente: %v", err)
	}

	err = me.db.GuardarPendiente(serieId, datos)
	if err != nil {
		return fmt.Errorf("error guardando eliminación pendiente: %v", err)
	}

	log.Printf("Eliminación pendiente registrada para serie %s (ID: %d)", path, serieId)
	return nil
}

// cargarEliminacionesPendientes carga todas las eliminaciones pendientes de PebbleDB
func (me *GestorBorde) cargarEliminacionesPendientes() ([]EliminacionPendiente, error) {
	var pendientes []EliminacionPendiente

	iter, err := me.db.ListarPendientes()
	if err != nil {
		return nil, fmt.Errorf("error creando iterador para pendientes: %v", err)
	}
	defer iter.Close()

	for iter.Siguiente() {
		var pendiente EliminacionPendiente
		if err := tipos.DeserializarGob(iter.Valor(), &pendiente); err != nil {
			log.Printf("Advertencia: error deserializando pendiente %d: %v", iter.SerieID(), err)
			continue
		}
		pendientes = append(pendientes, pendiente)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("error iterando pendientes: %v", err)
	}

	return pendientes, nil
}

// actualizarEliminacionPendiente actualiza el contador de intentos de una eliminación pendiente
func (me *GestorBorde) actualizarEliminacionPendiente(pendiente EliminacionPendiente) error {
	datos, err := tipos.SerializarGob(pendiente)
	if err != nil {
		return fmt.Errorf("error serializando eliminación pendiente: %v", err)
	}

	return me.db.GuardarPendiente(pendiente.SerieId, datos)
}

// eliminarPendienteCompletado elimina una eliminación pendiente de PebbleDB (cuando se completó)
func (me *GestorBorde) eliminarPendienteCompletado(serieId int) error {
	return me.db.BorrarPendiente(serieId)
}

// eliminarSerieDeS3 elimina todos los objetos de una serie en S3
// Retorna el número de objetos eliminados y un error si falla
func (me *GestorBorde) eliminarSerieDeS3(serieId int) (int, error) {
	if clienteS3 == nil {
		return 0, fmt.Errorf("S3 no está configurado")
	}

	ctx := context.TODO()
	prefijo := tipos.GenerarPrefijoS3Serie(me.nodoID, serieId)
	objetosEliminados := 0

	// Listar objetos con el prefijo de la serie
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(configuracionS3.Bucket),
		Prefix: aws.String(prefijo),
	}

	resultado, err := clienteS3.ListObjectsV2(ctx, listInput)
	if err != nil {
		return 0, fmt.Errorf("error listando objetos en S3: %v", err)
	}

	// Eliminar cada objeto encontrado
	for _, objeto := range resultado.Contents {
		_, err := clienteS3.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(configuracionS3.Bucket),
			Key:    objeto.Key,
		})
		if err != nil {
			return objetosEliminados, fmt.Errorf("error eliminando objeto %s: %v", *objeto.Key, err)
		}
		objetosEliminados++
	}

	// Si hay más objetos (paginación), continuar
	for resultado.IsTruncated != nil && *resultado.IsTruncated {
		listInput.ContinuationToken = resultado.NextContinuationToken
		resultado, err = clienteS3.ListObjectsV2(ctx, listInput)
		if err != nil {
			return objetosEliminados, fmt.Errorf("error listando más objetos en S3: %v", err)
		}

		for _, objeto := range resultado.Contents {
			_, err := clienteS3.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(configuracionS3.Bucket),
				Key:    objeto.Key,
			})
			if err != nil {
				return objetosEliminados, fmt.Errorf("error eliminando objeto %s: %v", *objeto.Key, err)
			}
			objetosEliminados++
		}
	}

	return objetosEliminados, nil
}

// procesarEliminacionesPendientes procesa todas las eliminaciones pendientes de S3.
// El catálogo lo sube quien llama: el aviso inmediato o el ciclo de reintento.
func (me *GestorBorde) procesarEliminacionesPendientes() error {
	// Verificar si el gestor está cerrando
	select {
	case <-me.finalizado:
		return nil // Gestor cerrando, no procesar
	default:
	}

	if clienteS3 == nil {
		return nil // Sin S3, nada que procesar
	}

	pendientes, err := me.cargarEliminacionesPendientes()
	if err != nil {
		return fmt.Errorf("error cargando pendientes: %v", err)
	}

	if len(pendientes) == 0 {
		return nil // Nada que procesar
	}

	log.Printf("Procesando %d eliminaciones pendientes de S3", len(pendientes))

	exitosos := 0
	fallidos := 0

	for _, pendiente := range pendientes {
		// Intentar eliminar de S3
		objetosEliminados, err := me.eliminarSerieDeS3(pendiente.SerieId)

		if err != nil {
			// Falló: incrementar intentos y actualizar
			pendiente.Intentos++
			if updateErr := me.actualizarEliminacionPendiente(pendiente); updateErr != nil {
				log.Printf("Error actualizando pendiente: %v", updateErr)
			}
			log.Printf("Intento %d fallido para eliminar serie %s (ID: %d) de S3: %v",
				pendiente.Intentos, pendiente.Path, pendiente.SerieId, err)
			fallidos++
		} else {
			// Éxito: eliminar de la cola de pendientes
			if err := me.eliminarPendienteCompletado(pendiente.SerieId); err != nil {
				log.Printf("Error eliminando pendiente completado: %v", err)
			}
			log.Printf("Serie %s (ID: %d) eliminada de S3: %d objetos eliminados",
				pendiente.Path, pendiente.SerieId, objetosEliminados)
			exitosos++
		}
	}

	log.Printf("Eliminaciones pendientes procesadas: %d exitosas, %d fallidas", exitosos, fallidos)
	return nil
}
