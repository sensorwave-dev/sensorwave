package edge

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cockroachdb/pebble"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

// clienteS3 es el cliente S3 para la migración
// Usa la interfaz tipos.ClienteS3 para permitir inyección de mocks en tests
var clienteS3 tipos.ClienteS3
var configuracionS3 tipos.ConfiguracionS3

// ConfigurarS3 configura la conexión a almacenamiento S3-compatible
func (me *ManagerEdge) ConfigurarS3(cfg tipos.ConfiguracionS3) error {
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

// MigrarAS3 migra todas las series y datos a almacenamiento S3 como archivos
func (me *ManagerEdge) MigrarAS3() error {
	// Verificar que S3 esté configurado
	if clienteS3 == nil {
		// Intentar configurar desde variables de entorno
		cfg := tipos.ConfiguracionS3{
			Endpoint:        os.Getenv("S3_ENDPOINT"),
			AccessKeyID:     os.Getenv("S3_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"),
			Bucket:          os.Getenv("S3_BUCKET"),
			Region:          os.Getenv("S3_REGION"),
		}

		if cfg.Endpoint == "" {
			cfg.Endpoint = "http://localhost:3900" // Valor por defecto
		}
		if cfg.Bucket == "" {
			cfg.Bucket = "sensorwave-data" // Valor por defecto
		}
		if cfg.Region == "" {
			cfg.Region = "us-east-1"
		}

		if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
			return fmt.Errorf("S3 no está configurado. Use ConfigurarS3() o configure las variables de entorno")
		}

		err := me.ConfigurarS3(cfg)
		if err != nil {
			return fmt.Errorf("error al configurar S3: %v", err)
		}
	}

	ctx := context.TODO()
	contadorMigrados := 0

	// Iterar sobre los datos en PebbleDB y migrar a S3
	iter, err := me.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("data/"),
		UpperBound: []byte("data0"),
	})
	if err != nil {
		return fmt.Errorf("error al crear iterador para migración: %v", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		// Obtener clave y valor
		clave := string(iter.Key())
		valor := iter.Value()

		// Extraer serieId y tiempos de la clave local para generar clave S3
		serieId, tiempoInicio, tiempoFin, err := parsearClaveLocalDatos(clave)
		if err != nil {
			log.Printf("Advertencia: clave con formato inválido %s: %v", clave, err)
			continue
		}

		// Crear nombre de archivo en S3 con formato optimizado
		// Formato: nodoID/{serieId}_{tiempoInicio}_{tiempoFin}
		nombreArchivo := tipos.GenerarClaveS3Datos(me.nodoID, serieId, tiempoInicio, tiempoFin)

		// Subir archivo a S3
		_, err = clienteS3.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(configuracionS3.Bucket),
			Key:    aws.String(nombreArchivo),
			Body:   bytes.NewReader(valor),
		})
		if err != nil {
			return fmt.Errorf("error al subir dato a S3 (clave: %s): %v", clave, err)
		}

		contadorMigrados++

		// Borrar la entrada de PebbleDB después de migrar
		err = me.db.Delete(iter.Key(), pebble.Sync)
		if err != nil {
			return fmt.Errorf("error al borrar dato migrado de PebbleDB: %v", err)
		}

		// Log cada 100 registros migrados
		if contadorMigrados%100 == 0 {
			log.Printf("Migrados %d registros a S3...", contadorMigrados)
		}
	}

	// Verificar errores del iterador
	if err := iter.Error(); err != nil {
		return fmt.Errorf("error durante la iteración para migración: %v", err)
	}

	log.Printf("Migración a S3 completada exitosamente. Total de registros migrados: %d", contadorMigrados)
	return nil
}

// MigrarPorTiempoAlmacenamiento migra bloques de datos que excedan el tiempo de almacenamiento configurado
// para cada serie. Solo migra series que tengan TiempoAlmacenamiento > 0.
func (me *ManagerEdge) MigrarPorTiempoAlmacenamiento() error {
	// Verificar que S3 esté configurado
	if clienteS3 == nil {
		return fmt.Errorf("S3 no está configurado. Use ConfigurarS3() primero")
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

		// Construir prefijo para buscar bloques de esta serie
		prefijo := fmt.Sprintf("data/%010d/", serie.SerieId)

		iter, err := me.db.NewIter(&pebble.IterOptions{
			LowerBound: []byte(prefijo),
			UpperBound: []byte(prefijo[:len(prefijo)-1] + "0"), // Incrementar último caracter
		})
		if err != nil {
			log.Printf("Error creando iterador para serie %s: %v", serie.Path, err)
			continue
		}

		// Recolectar claves a migrar (no podemos modificar durante iteración)
		clavesAMigrar := make([][]byte, 0)
		valoresAMigrar := make([][]byte, 0)

		for iter.First(); iter.Valid(); iter.Next() {
			clave := string(iter.Key())

			// Parsear tiempoFin de la clave
			tiempoFin, err := parsearTiempoFinDeClave(clave)
			if err != nil {
				continue
			}

			// Si el bloque es más antiguo que el límite, marcarlo para migración
			if tiempoFin < tiempoLimite {
				// Copiar clave y valor
				claveBytes := make([]byte, len(iter.Key()))
				copy(claveBytes, iter.Key())
				valorBytes := make([]byte, len(iter.Value()))
				copy(valorBytes, iter.Value())

				clavesAMigrar = append(clavesAMigrar, claveBytes)
				valoresAMigrar = append(valoresAMigrar, valorBytes)
			}
		}
		iter.Close()

		// Migrar bloques recolectados
		for i, clave := range clavesAMigrar {
			valor := valoresAMigrar[i]

			// Extraer serieId y tiempos de la clave local para generar clave S3
			serieId, tiempoInicio, tiempoFin, err := parsearClaveLocalDatos(string(clave))
			if err != nil {
				log.Printf("Advertencia: clave con formato inválido %s: %v", string(clave), err)
				continue
			}

			// Crear nombre de archivo en S3 con formato optimizado
			nombreArchivo := tipos.GenerarClaveS3Datos(me.nodoID, serieId, tiempoInicio, tiempoFin)

			// Subir a S3
			_, err = clienteS3.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(configuracionS3.Bucket),
				Key:    aws.String(nombreArchivo),
				Body:   bytes.NewReader(valor),
			})
			if err != nil {
				log.Printf("Error subiendo bloque a S3 (clave: %s): %v", string(clave), err)
				continue
			}
			contadorMigrados++

			// Eliminar de PebbleDB después de migrar exitosamente
			err = me.db.Delete(clave, pebble.Sync)
			if err != nil {
				log.Printf("Error eliminando bloque migrado de PebbleDB: %v", err)
				continue
			}
			contadorEliminados++
		}

		if len(clavesAMigrar) > 0 {
			log.Printf("Serie '%s': %d bloques migrados a S3", serie.Path, len(clavesAMigrar))
		}
	}

	log.Printf("Migración por tiempo completada: %d bloques migrados, %d eliminados localmente",
		contadorMigrados, contadorEliminados)
	return nil
}

// parsearTiempoFinDeClave extrae el tiempoFin de una clave con formato "data/{serieId}/{tiempoInicio}_{tiempoFin}"
func parsearTiempoFinDeClave(clave string) (int64, error) {
	// Formato: data/0000000001/00000000000000000001_00000000000000000002
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

// parsearClaveLocalDatos extrae serieId, tiempoInicio y tiempoFin de una clave local de PebbleDB
// Formato: data/{serieId}/{tiempoInicio}_{tiempoFin}
func parsearClaveLocalDatos(clave string) (serieId int, tiempoInicio, tiempoFin int64, err error) {
	partes := strings.Split(clave, "/")
	if len(partes) != 3 || partes[0] != "data" {
		return 0, 0, 0, fmt.Errorf("formato de clave local inválido: %s", clave)
	}

	serieId64, err := strconv.ParseInt(strings.TrimLeft(partes[1], "0"), 10, 32)
	if err != nil && partes[1] != "0000000000" {
		return 0, 0, 0, fmt.Errorf("error parseando serieId: %v", err)
	}

	tiempos := strings.Split(partes[2], "_")
	if len(tiempos) != 2 {
		return 0, 0, 0, fmt.Errorf("formato de tiempos inválido: %s", partes[2])
	}

	tiempoInicio, err = strconv.ParseInt(strings.TrimLeft(tiempos[0], "0"), 10, 64)
	if err != nil && tiempos[0] != strings.Repeat("0", 20) {
		return 0, 0, 0, fmt.Errorf("error parseando tiempoInicio: %v", err)
	}

	tiempoFin, err = strconv.ParseInt(strings.TrimLeft(tiempos[1], "0"), 10, 64)
	if err != nil && tiempos[1] != strings.Repeat("0", 20) {
		return 0, 0, 0, fmt.Errorf("error parseando tiempoFin: %v", err)
	}

	return int(serieId64), tiempoInicio, tiempoFin, nil
}

// IniciarMigracionAutomatica inicia un goroutine que ejecuta la migración por tiempo
// de almacenamiento periódicamente según el intervalo especificado.
func (me *ManagerEdge) IniciarMigracionAutomatica(intervalo time.Duration) {
	go func() {
		ticker := time.NewTicker(intervalo)
		defer ticker.Stop()

		for {
			select {
			case <-me.done:
				log.Printf("Deteniendo migración automática")
				return
			case <-ticker.C:
				if clienteS3 == nil {
					continue // S3 no configurado, saltar
				}

				err := me.MigrarPorTiempoAlmacenamiento()
				if err != nil {
					log.Printf("Error en migración automática: %v", err)
				}
			}
		}
	}()

	log.Printf("Migración automática iniciada (intervalo: %v)", intervalo)
}

// ============================================================================
// ELIMINACIÓN DE SERIES EN S3 (OFFLINE-FIRST)
// ============================================================================

// EliminacionPendiente representa una serie pendiente de eliminar de S3
type EliminacionPendiente struct {
	SerieId   int    // ID de la serie eliminada
	Path      string // Path original (para logging)
	Timestamp int64  // Cuándo se solicitó la eliminación (UnixNano)
	Intentos  int    // Cantidad de intentos fallidos
}

// generarClaveEliminacionPendiente genera la clave para almacenar una eliminación pendiente
func generarClaveEliminacionPendiente(serieId int) []byte {
	return []byte(fmt.Sprintf("pendientes/eliminar/%010d", serieId))
}

// guardarEliminacionPendiente guarda una eliminación pendiente en PebbleDB
func (me *ManagerEdge) guardarEliminacionPendiente(serieId int, path string) error {
	pendiente := EliminacionPendiente{
		SerieId:   serieId,
		Path:      path,
		Timestamp: time.Now().UnixNano(),
		Intentos:  0,
	}

	datos, err := tipos.SerializarGob(pendiente)
	if err != nil {
		return fmt.Errorf("error serializando eliminación pendiente: %v", err)
	}

	clave := generarClaveEliminacionPendiente(serieId)
	err = me.db.Set(clave, datos, pebble.Sync)
	if err != nil {
		return fmt.Errorf("error guardando eliminación pendiente: %v", err)
	}

	log.Printf("Eliminación pendiente registrada para serie %s (ID: %d)", path, serieId)
	return nil
}

// cargarEliminacionesPendientes carga todas las eliminaciones pendientes de PebbleDB
func (me *ManagerEdge) cargarEliminacionesPendientes() ([]EliminacionPendiente, error) {
	var pendientes []EliminacionPendiente

	iter, err := me.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("pendientes/eliminar/"),
		UpperBound: []byte("pendientes/eliminar0"), // Rango que incluye todas las claves
	})
	if err != nil {
		return nil, fmt.Errorf("error creando iterador para pendientes: %v", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var pendiente EliminacionPendiente
		if err := tipos.DeserializarGob(iter.Value(), &pendiente); err != nil {
			log.Printf("Advertencia: error deserializando pendiente %s: %v", string(iter.Key()), err)
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
func (me *ManagerEdge) actualizarEliminacionPendiente(pendiente EliminacionPendiente) error {
	datos, err := tipos.SerializarGob(pendiente)
	if err != nil {
		return fmt.Errorf("error serializando eliminación pendiente: %v", err)
	}

	clave := generarClaveEliminacionPendiente(pendiente.SerieId)
	return me.db.Set(clave, datos, pebble.Sync)
}

// eliminarPendienteCompletado elimina una eliminación pendiente de PebbleDB (cuando se completó)
func (me *ManagerEdge) eliminarPendienteCompletado(serieId int) error {
	clave := generarClaveEliminacionPendiente(serieId)
	return me.db.Delete(clave, pebble.Sync)
}

// eliminarSerieDeS3 elimina todos los objetos de una serie en S3
// Retorna el número de objetos eliminados y un error si falla
func (me *ManagerEdge) eliminarSerieDeS3(serieId int) (int, error) {
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

// ProcesarEliminacionesPendientes procesa todas las eliminaciones pendientes de S3
// Intenta eliminar los datos de cada serie en S3 y actualiza el registro del nodo
func (me *ManagerEdge) ProcesarEliminacionesPendientes() error {
	// Verificar si el manager está cerrando
	select {
	case <-me.done:
		return nil // Manager cerrando, no procesar
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

	// Actualizar registro del nodo en S3 si hubo eliminaciones exitosas
	if exitosos > 0 {
		if err := me.RegistrarEnS3(); err != nil {
			log.Printf("Advertencia: error actualizando registro del nodo en S3: %v", err)
		}
	}

	log.Printf("Eliminaciones pendientes procesadas: %d exitosas, %d fallidas", exitosos, fallidos)
	return nil
}

// IniciarLimpiezaS3Automatica inicia un goroutine que procesa eliminaciones pendientes
// cada 5 minutos (mismo intervalo que limpieza de reglas)
func (me *ManagerEdge) IniciarLimpiezaS3Automatica() {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-me.done:
				log.Printf("Deteniendo limpieza automática de S3")
				return
			case <-ticker.C:
				if clienteS3 == nil {
					continue // S3 no configurado, saltar
				}

				if err := me.ProcesarEliminacionesPendientes(); err != nil {
					log.Printf("Error en limpieza automática de S3: %v", err)
				}
			}
		}
	}()

	log.Printf("Limpieza automática de S3 iniciada (intervalo: 5 minutos)")
}
