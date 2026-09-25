package borde

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

// registrarEnS3 sube el nodo, sus series y sus reglas a S3.
// Lo llama el ciclo de S3 al arrancar y en cada vuelta.
func (me *GestorBorde) registrarEnS3() error {
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
	reglasMap := me.motorReglas.ListarReglas()
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
				Tipo:       a.Tipo,
				Destino:    a.Destino,
				Parametros: a.Parametros,
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
