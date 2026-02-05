package edge

import (
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/sensorwave-dev/sensorwave/tipos"
)

type TipoOperador string

const (
	OperadorMayorIgual TipoOperador = ">="
	OperadorMenorIgual TipoOperador = "<="
	OperadorIgual      TipoOperador = "=="
	OperadorDistinto   TipoOperador = "!="
	OperadorMayor      TipoOperador = ">"
	OperadorMenor      TipoOperador = "<"
)

// Alias para mantener compatibilidad con código existente en reglas
// Los tipos canónicos están en tipos/agregacion.go
type TipoAgregacion = tipos.TipoAgregacion

const (
	AgregacionPromedio = tipos.AgregacionPromedio
	AgregacionMaximo   = tipos.AgregacionMaximo
	AgregacionMinimo   = tipos.AgregacionMinimo
	AgregacionSuma     = tipos.AgregacionSuma
	AgregacionCount    = tipos.AgregacionCount
)

type TipoLogica string

const (
	LogicaAND TipoLogica = "AND"
	LogicaOR  TipoLogica = "OR"
)

// Condicion define una condición para evaluar reglas.
// La condición usa las funciones de consulta del sistema (ConsultarUltimoPunto, ConsultarAgregacion)
// para obtener datos, lo que unifica la lógica de obtención de datos entre consultas y reglas.
type Condicion struct {
	// Path especifica la serie o patrón de series a evaluar.
	// Puede ser un path exacto ("sensor_01/temp") o un patrón con wildcards ("sensor_*/temp", "*/temp").
	Path string

	// VentanaT define la ventana temporal relativa hacia atrás desde el momento de evaluación.
	// Por ejemplo, 5*time.Minute evalúa los datos de los últimos 5 minutos.
	VentanaT time.Duration

	// Agregacion especifica el tipo de agregación a aplicar sobre los datos.
	// Si está vacía ("") o es "last", se usa el último valor (ConsultarUltimoPunto).
	// Valores soportados: promedio, maximo, minimo, suma, count
	Agregacion TipoAgregacion

	// Operador de comparación para evaluar la condición.
	// Valores: >=, <=, ==, !=, >, <
	Operador TipoOperador

	// Valor umbral para la comparación.
	// Tipos soportados: bool, int64, float64, string
	Valor interface{}

	// AgregarSeries controla cómo se evalúan múltiples series (cuando Path es un patrón wildcard):
	//   - false (default): Modo "any" - la condición es verdadera si ALGUNA serie cumple
	//   - true: Modo "all" - primero agrega los valores de todas las series, luego evalúa
	AgregarSeries bool
}

type Accion struct {
	Tipo    string
	Destino string
	Params  map[string]string
}

type Regla struct {
	ID          string
	Nombre      string
	Condiciones []Condicion
	Acciones    []Accion
	Logica      TipoLogica
	Activa      bool
	UltimaEval  time.Time
}

type EjecutorAccion func(accion Accion, regla *Regla, valores map[string]interface{}) error

type MotorReglas struct {
	reglas     map[string]*Regla
	ejecutores map[string]EjecutorAccion
	habilitado bool
	mu         sync.RWMutex
	manager    *ManagerEdge // Referencia al manager padre (para acceso a datos)
	db         *pebble.DB   // Conexión a PebbleDB para persistencia de reglas
}

// EstadoMotorReglas contiene información sobre el estado actual del motor de reglas
type EstadoMotorReglas struct {
	Habilitado    bool // Si el motor está habilitado
	ReglasActivas int  // Cantidad de reglas activas
	ReglasTotal   int  // Cantidad total de reglas
}

// ObtenerEstado devuelve el estado actual del motor de reglas
func (mr *MotorReglas) ObtenerEstado() EstadoMotorReglas {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	activas := 0
	for _, regla := range mr.reglas {
		if regla.Activa {
			activas++
		}
	}

	return EstadoMotorReglas{
		Habilitado:    mr.habilitado,
		ReglasActivas: activas,
		ReglasTotal:   len(mr.reglas),
	}
}

func nuevoMotorReglasIntegrado(manager *ManagerEdge, db *pebble.DB) *MotorReglas {
	motor := &MotorReglas{
		reglas:     make(map[string]*Regla),
		ejecutores: make(map[string]EjecutorAccion),
		habilitado: true,
		manager:    manager,
		db:         db,
	}

	motor.registrarEjecutoresPorDefecto()
	return motor
}

func (mr *MotorReglas) registrarEjecutoresPorDefecto() {
	// Ejecutor log: útil para debugging y desarrollo
	mr.ejecutores["log"] = func(accion Accion, regla *Regla, valores map[string]interface{}) error {
		log.Printf("Regla '%s' activada - Acción: %s, Destino: %s, Valores: %v",
			regla.ID, accion.Tipo, accion.Destino, valores)
		return nil
	}
	// Nota: Para publicar a actuadores usar PUBLICAR_MQTT, PUBLICAR_NATS, PUBLICAR_HTTP o PUBLICAR_COAP
	// registrados via RegistrarEjecutorMQTT(), RegistrarEjecutorNATS(), etc. en ejecutores.go
}

func (mr *MotorReglas) evaluarReglas(timestamp time.Time) error {
	if !mr.habilitado {
		return nil
	}

	mr.mu.RLock()
	defer mr.mu.RUnlock()

	for _, regla := range mr.reglas {
		if !regla.Activa {
			continue
		}

		if mr.evaluarCondicionesRegla(regla, timestamp) {
			if err := mr.ejecutarAcciones(regla, timestamp); err != nil {
				log.Printf("Error ejecutando acciones de regla '%s': %v", regla.ID, err)
			}
		}

		regla.UltimaEval = timestamp
	}

	return nil
}

func (mr *MotorReglas) evaluarCondicionesRegla(regla *Regla, timestamp time.Time) bool {
	if len(regla.Condiciones) == 0 {
		return false
	}

	resultados := make([]bool, len(regla.Condiciones))

	for i, condicion := range regla.Condiciones {
		resultados[i] = mr.evaluarCondicion(&condicion, timestamp)
	}

	if regla.Logica == LogicaOR {
		for _, resultado := range resultados {
			if resultado {
				return true
			}
		}
		return false
	}

	for _, resultado := range resultados {
		if !resultado {
			return false
		}
	}
	return true
}

// CalcularAgregacionSimple calcula una agregación sobre un slice de valores.
// Función pública para ser usada por consultas y reglas.
// Solo soporta agregaciones numéricas: promedio, maximo, minimo, suma, count.
func CalcularAgregacionSimple(valores []float64, agregacion TipoAgregacion) (float64, error) {
	if len(valores) == 0 {
		return 0, fmt.Errorf("no hay valores para agregar")
	}

	switch agregacion {
	case AgregacionPromedio:
		suma := 0.0
		for _, v := range valores {
			suma += v
		}
		return suma / float64(len(valores)), nil

	case AgregacionMaximo:
		max := valores[0]
		for _, v := range valores[1:] {
			if v > max {
				max = v
			}
		}
		return max, nil

	case AgregacionMinimo:
		min := valores[0]
		for _, v := range valores[1:] {
			if v < min {
				min = v
			}
		}
		return min, nil

	case AgregacionSuma:
		suma := 0.0
		for _, v := range valores {
			suma += v
		}
		return suma, nil

	case AgregacionCount:
		return float64(len(valores)), nil

	default:
		return 0, fmt.Errorf("tipo de agregación no soportado: %s", agregacion)
	}
}

func (mr *MotorReglas) evaluarCondicion(condicion *Condicion, timestamp time.Time) bool {
	tiempoInicio := timestamp.Add(-condicion.VentanaT)

	// Determinar si es agregación que requiere último valor o agregación completa
	// Mantenemos compatibilidad con "last" como string para indicar último valor
	agregacionVacia := condicion.Agregacion == "" || condicion.Agregacion == "last"

	if agregacionVacia {
		// Sin agregación (o "last") = obtener último valor en ventana
		resultado, err := mr.manager.ConsultarUltimoPunto(condicion.Path, &tiempoInicio, &timestamp)
		if err != nil || len(resultado.Valores) == 0 {
			return false
		}

		if condicion.AgregarSeries {
			// Modo "all": usar el primer valor encontrado (no tiene mucho sentido sin agregación)
			return mr.aplicarOperador(resultado.Valores[0], condicion.Operador, condicion.Valor)
		}

		// Modo "any": si alguna serie cumple, retornar true
		for _, valor := range resultado.Valores {
			if mr.aplicarOperador(valor, condicion.Operador, condicion.Valor) {
				return true
			}
		}
		return false
	}

	// Con agregación: usar ConsultarAgregacion
	resultado, err := mr.manager.ConsultarAgregacion(condicion.Path, tiempoInicio, timestamp, []tipos.TipoAgregacion{condicion.Agregacion})
	if err != nil || len(resultado.Valores) == 0 || len(resultado.Valores[0]) == 0 {
		return false
	}

	// resultado.Valores[0] contiene los valores de la primera (y única) agregación
	valoresAgregacion := resultado.Valores[0]

	if condicion.AgregarSeries {
		// Modo "all": agregar todos los valores de las series
		valorFinal, err := CalcularAgregacionSimple(valoresAgregacion, condicion.Agregacion)
		if err != nil {
			return false
		}
		return mr.aplicarOperador(valorFinal, condicion.Operador, condicion.Valor)
	}

	// Modo "any": si alguna serie cumple
	for _, valor := range valoresAgregacion {
		if mr.aplicarOperador(valor, condicion.Operador, condicion.Valor) {
			return true
		}
	}
	return false
}

func (mr *MotorReglas) aplicarOperador(valor1 interface{}, operador TipoOperador, valor2 interface{}) bool {
	const epsilon = 1e-9

	// Caso 1: Comparación Boolean
	if v1, ok1 := valor1.(bool); ok1 {
		if v2, ok2 := valor2.(bool); ok2 {
			switch operador {
			case OperadorIgual:
				return v1 == v2
			case OperadorDistinto:
				return v1 != v2
			default:
				log.Printf("Operador %s no soportado para boolean", operador)
				return false
			}
		}
		// Tipos incompatibles
		log.Printf("No se puede comparar bool con %T", valor2)
		return false
	}

	// Caso 2: Comparación String (case-insensitive)
	if v1, ok1 := valor1.(string); ok1 {
		if v2, ok2 := valor2.(string); ok2 {
			v1Lower := strings.ToLower(v1)
			v2Lower := strings.ToLower(v2)

			switch operador {
			case OperadorIgual:
				return v1Lower == v2Lower
			case OperadorDistinto:
				return v1Lower != v2Lower
			default:
				log.Printf("Operador %s no soportado para string", operador)
				return false
			}
		}
		// Tipos incompatibles
		log.Printf("No se puede comparar string con %T", valor2)
		return false
	}

	// Caso 3: Comparación Numérica (int64 y float64 son intercambiables)
	var v1Float, v2Float float64
	var ok1, ok2 bool

	// Convertir valor1 a float64
	switch v := valor1.(type) {
	case int64:
		v1Float = float64(v)
		ok1 = true
	case float64:
		v1Float = v
		ok1 = true
	}

	// Convertir valor2 a float64
	switch v := valor2.(type) {
	case int64:
		v2Float = float64(v)
		ok2 = true
	case float64:
		v2Float = v
		ok2 = true
	}

	if ok1 && ok2 {
		// Comparación numérica
		switch operador {
		case OperadorMayorIgual:
			return v1Float >= v2Float
		case OperadorMenorIgual:
			return v1Float <= v2Float
		case OperadorIgual:
			return math.Abs(v1Float-v2Float) < epsilon
		case OperadorDistinto:
			return math.Abs(v1Float-v2Float) >= epsilon
		case OperadorMayor:
			return v1Float > v2Float
		case OperadorMenor:
			return v1Float < v2Float
		default:
			return false
		}
	}

	// Tipos incompatibles (ej: número vs string)
	log.Printf("No se puede comparar %T con %T", valor1, valor2)
	return false
}

func (mr *MotorReglas) ejecutarAcciones(regla *Regla, timestamp time.Time) error {
	valores := make(map[string]interface{})
	var seriePrincipal string

	// Recolectar valores de las condiciones para pasarlos a las acciones
	for _, condicion := range regla.Condiciones {
		tiempoInicio := timestamp.Add(-condicion.VentanaT)

		// Usar ConsultarUltimoPunto para obtener los valores actuales
		resultado, err := mr.manager.ConsultarUltimoPunto(condicion.Path, &tiempoInicio, &timestamp)
		if err != nil {
			continue
		}

		// Agregar todos los valores encontrados al mapa
		for i, serie := range resultado.Series {
			if i < len(resultado.Valores) {
				valores[serie] = resultado.Valores[i]
			}
			// Guardar la primera serie como principal (para variables de contexto)
			if seriePrincipal == "" {
				seriePrincipal = serie
			}
		}
	}

	// Agregar metadatos de contexto para resolución de plantillas
	if seriePrincipal != "" {
		valores["_serie"] = seriePrincipal
		partes := strings.Split(seriePrincipal, "/")
		for i, parte := range partes {
			valores[fmt.Sprintf("_serie_%d", i)] = parte
		}
	}
	valores["_regla_id"] = regla.ID
	valores["_regla_nombre"] = regla.Nombre
	valores["_timestamp"] = timestamp

	for _, accion := range regla.Acciones {
		ejecutor, exists := mr.ejecutores[accion.Tipo]
		if !exists {
			log.Printf("Ejecutor no encontrado para tipo de acción: %s", accion.Tipo)
			continue
		}

		if err := ejecutor(accion, regla, valores); err != nil {
			return fmt.Errorf("error ejecutando acción %s: %v", accion.Tipo, err)
		}
	}

	return nil
}

func (mr *MotorReglas) RegistrarEjecutor(tipoAccion string, ejecutor EjecutorAccion) error {
	if tipoAccion == "" {
		return fmt.Errorf("tipo de acción no puede estar vacío")
	}
	if ejecutor == nil {
		return fmt.Errorf("ejecutor no puede ser nil")
	}

	mr.mu.Lock()
	defer mr.mu.Unlock()

	mr.ejecutores[tipoAccion] = ejecutor
	log.Printf("Ejecutor registrado para tipo de acción: %s", tipoAccion)
	return nil
}

func (mr *MotorReglas) Habilitar(habilitado bool) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	mr.habilitado = habilitado
	estado := "deshabilitado"
	if habilitado {
		estado = "habilitado"
	}
	log.Printf("Motor de reglas %s", estado)
}

func (mr *MotorReglas) validarRegla(regla *Regla) error {
	if regla.ID == "" {
		return fmt.Errorf("ID de regla no puede estar vacío")
	}

	if len(regla.Condiciones) == 0 {
		return fmt.Errorf("regla debe tener al menos una condición")
	}

	if len(regla.Acciones) == 0 {
		return fmt.Errorf("regla debe tener al menos una acción")
	}

	for i, condicion := range regla.Condiciones {
		if err := mr.validarCondicion(&condicion); err != nil {
			return fmt.Errorf("condición %d inválida: %v", i, err)
		}
	}

	for i, accion := range regla.Acciones {
		if err := mr.validarAccion(&accion); err != nil {
			return fmt.Errorf("acción %d inválida: %v", i, err)
		}
	}

	if regla.Logica != LogicaAND && regla.Logica != LogicaOR {
		regla.Logica = LogicaAND
	}

	return nil
}

func (mr *MotorReglas) validarCondicion(condicion *Condicion) error {
	// VALIDACIÓN 1: Path no puede estar vacío
	if condicion.Path == "" {
		return fmt.Errorf("Path de condición no puede estar vacío")
	}

	// VALIDACIÓN 2: Ventana temporal debe ser positiva
	if condicion.VentanaT <= 0 {
		return fmt.Errorf("ventana temporal debe ser mayor a cero")
	}

	// VALIDACIÓN 3: Operador válido
	operadoresValidos := []TipoOperador{OperadorMayorIgual, OperadorMenorIgual, OperadorIgual, OperadorDistinto, OperadorMayor, OperadorMenor}
	operadorValido := false
	for _, op := range operadoresValidos {
		if condicion.Operador == op {
			operadorValido = true
			break
		}
	}
	if !operadorValido {
		return fmt.Errorf("operador inválido: %s", condicion.Operador)
	}

	// VALIDACIÓN 4: Valor no puede ser nil
	if condicion.Valor == nil {
		return fmt.Errorf("valor de condición no puede ser nil")
	}

	// VALIDACIÓN 5: Solo tipos de valor soportados
	switch condicion.Valor.(type) {
	case bool, int64, float64, string:
		// OK
	default:
		return fmt.Errorf("tipo de valor no soportado: %T (use bool, int64, float64 o string)", condicion.Valor)
	}

	// VALIDACIÓN 6: Operadores restringidos para bool/string
	switch condicion.Valor.(type) {
	case bool, string:
		if condicion.Operador != OperadorIgual && condicion.Operador != OperadorDistinto {
			return fmt.Errorf("tipo %T solo soporta operadores == y != (recibido: %s)",
				condicion.Valor, condicion.Operador)
		}
	}

	// VALIDACIÓN 7: Agregación válida (si se especifica)
	// Nota: "last" es aceptado pero se maneja como caso especial (usa ConsultarUltimoPunto)
	if condicion.Agregacion != "" && condicion.Agregacion != "last" {
		agregacionesValidas := []TipoAgregacion{
			AgregacionPromedio, AgregacionMaximo, AgregacionMinimo,
			AgregacionSuma, AgregacionCount,
		}
		agregacionValida := false
		for _, agg := range agregacionesValidas {
			if condicion.Agregacion == agg {
				agregacionValida = true
				break
			}
		}
		if !agregacionValida {
			return fmt.Errorf("agregación inválida: %s (use: promedio, maximo, minimo, suma, count)", condicion.Agregacion)
		}
	}

	// VALIDACIÓN 8: Verificar compatibilidad de tipos con agregación
	// Solo validar para agregaciones numéricas (no para "" ni "last" ni "count")
	if condicion.Agregacion != "" && condicion.Agregacion != "last" && condicion.Agregacion != AgregacionCount {
		if err := mr.validarAgregacionCompatible(condicion); err != nil {
			return err
		}
	}

	return nil
}

// validarAgregacionCompatible verifica que la agregación sea compatible con los tipos de las series.
// Usa el Path para resolver las series (puede incluir wildcards).
func (mr *MotorReglas) validarAgregacionCompatible(condicion *Condicion) error {
	if mr.manager == nil {
		return nil // No podemos validar sin manager
	}

	// Resolver series por Path (puede ser exacto o patrón wildcard)
	series, err := mr.manager.ListarSeriesPorPath(condicion.Path)
	if err != nil {
		// Si no se pueden resolver las series, no validar estrictamente
		return nil
	}

	// Validar cada serie
	tiposEncontrados := make(map[tipos.TipoDatos]bool)

	for _, serie := range series {
		tiposEncontrados[serie.TipoDatos] = true

		// Text y Boolean NO permiten agregaciones numéricas (excepto count)
		if serie.TipoDatos == tipos.Text || serie.TipoDatos == tipos.Boolean {
			return fmt.Errorf("agregación '%s' no soportada para serie '%s' de tipo %s (solo 'count' es válido)",
				condicion.Agregacion, serie.Path, serie.TipoDatos)
		}
	}

	// Grupos heterogéneos no permiten agregaciones numéricas
	if len(tiposEncontrados) > 1 {
		return fmt.Errorf("no se puede agregar series de tipos diferentes (encontrados: %v)",
			getTiposKeys(tiposEncontrados))
	}

	return nil
}

// Helper para extraer keys de map
func getTiposKeys(m map[tipos.TipoDatos]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k.String())
	}
	return keys
}

func (mr *MotorReglas) validarAccion(accion *Accion) error {
	if accion.Tipo == "" {
		return fmt.Errorf("tipo de acción no puede estar vacío")
	}

	if accion.Destino == "" {
		return fmt.Errorf("destino de acción no puede estar vacío")
	}

	// Validar sintaxis de plantilla en Destino
	if err := ValidarPlantilla(accion.Destino); err != nil {
		return fmt.Errorf("plantilla de destino inválida: %v", err)
	}

	// Validar que variables requeridas estén en Params (excepto las de contexto)
	if err := ValidarVariablesRequeridas(accion.Destino, accion.Params); err != nil {
		return fmt.Errorf("variables faltantes en params: %v", err)
	}

	return nil
}

func (mr *MotorReglas) ListarReglas() map[string]*Regla {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	copia := make(map[string]*Regla)
	for id, regla := range mr.reglas {
		copia[id] = regla
	}
	return copia
}

func (mr *MotorReglas) ObtenerRegla(id string) (*Regla, error) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	regla, exists := mr.reglas[id]
	if !exists {
		return nil, fmt.Errorf("regla '%s' no encontrada", id)
	}

	return regla, nil
}

// AgregarReglaEnMemoria agrega una regla al motor sin persistencia
func (mr *MotorReglas) AgregarReglaEnMemoria(regla *Regla) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	mr.reglas[regla.ID] = regla
	return nil
}

// EliminarReglaEnMemoria elimina una regla del motor sin persistencia
func (mr *MotorReglas) EliminarReglaEnMemoria(id string) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if _, exists := mr.reglas[id]; !exists {
		return fmt.Errorf("regla '%s' no encontrada", id)
	}

	delete(mr.reglas, id)
	return nil
}

// ActualizarReglaEnMemoria actualiza una regla en el motor sin persistencia
func (mr *MotorReglas) ActualizarReglaEnMemoria(regla *Regla) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if _, exists := mr.reglas[regla.ID]; !exists {
		return fmt.Errorf("regla '%s' no encontrada", regla.ID)
	}

	mr.reglas[regla.ID] = regla
	return nil
}

func generarClaveRegla(id string) []byte {
	return []byte("reglas/" + id)
}

func serializarRegla(regla *Regla) ([]byte, error) {
	return tipos.SerializarGob(regla)
}

func deserializarRegla(data []byte) (*Regla, error) {
	var regla Regla
	if err := tipos.DeserializarGob(data, &regla); err != nil {
		return nil, err
	}
	return &regla, nil
}

func (mr *MotorReglas) cargarReglasExistentes() error {
	if mr.db == nil {
		return nil
	}

	iter, err := mr.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("reglas/"),
		UpperBound: []byte("reglas0"),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		regla, err := deserializarRegla(iter.Value())
		if err != nil {
			continue
		}

		mr.AgregarReglaEnMemoria(regla)
	}

	return iter.Error()
}

func (mr *MotorReglas) AgregarRegla(regla *Regla) error {
	if regla == nil {
		return fmt.Errorf("regla no puede ser nil")
	}

	if err := mr.validarRegla(regla); err != nil {
		return fmt.Errorf("regla inválida: %v", err)
	}

	if mr.db != nil {
		key := generarClaveRegla(regla.ID)
		reglaBytes, err := serializarRegla(regla)
		if err != nil {
			return fmt.Errorf("error al serializar regla: %v", err)
		}

		err = mr.db.Set(key, reglaBytes, pebble.Sync)
		if err != nil {
			return fmt.Errorf("error al guardar regla: %v", err)
		}
	}

	regla.Activa = true
	if err := mr.AgregarReglaEnMemoria(regla); err != nil {
		return err
	}

	log.Printf("Regla '%s' agregada exitosamente", regla.ID)

	// Actualizar S3 (best-effort, igual que CrearSerie)
	if mr.manager != nil && clienteS3 != nil {
		if err := mr.manager.RegistrarEnS3(); err != nil {
			log.Printf("Error registrando regla nueva en S3: %v", err)
			// No retornar error - la regla ya fue guardada localmente
		}
	}

	return nil
}

func (mr *MotorReglas) EliminarRegla(id string) error {
	if mr.db != nil {
		key := generarClaveRegla(id)
		err := mr.db.Delete(key, pebble.Sync)
		if err != nil {
			return fmt.Errorf("error al eliminar regla de DB: %v", err)
		}
	}

	if err := mr.EliminarReglaEnMemoria(id); err != nil {
		return err
	}

	log.Printf("Regla '%s' eliminada", id)

	// Actualizar S3 (best-effort)
	if mr.manager != nil && clienteS3 != nil {
		if err := mr.manager.RegistrarEnS3(); err != nil {
			log.Printf("Error registrando eliminación de regla en S3: %v", err)
		}
	}

	return nil
}

func (mr *MotorReglas) ActualizarRegla(regla *Regla) error {
	if regla == nil {
		return fmt.Errorf("regla no puede ser nil")
	}

	if err := mr.validarRegla(regla); err != nil {
		return fmt.Errorf("regla inválida: %v", err)
	}

	if mr.db != nil {
		key := generarClaveRegla(regla.ID)
		reglaBytes, err := serializarRegla(regla)
		if err != nil {
			return fmt.Errorf("error al serializar regla: %v", err)
		}

		err = mr.db.Set(key, reglaBytes, pebble.Sync)
		if err != nil {
			return fmt.Errorf("error al actualizar regla en DB: %v", err)
		}
	}

	if err := mr.ActualizarReglaEnMemoria(regla); err != nil {
		return err
	}

	log.Printf("Regla '%s' actualizada", regla.ID)

	// Actualizar S3 (best-effort)
	if mr.manager != nil && clienteS3 != nil {
		if err := mr.manager.RegistrarEnS3(); err != nil {
			log.Printf("Error registrando regla actualizada en S3: %v", err)
		}
	}

	return nil
}
