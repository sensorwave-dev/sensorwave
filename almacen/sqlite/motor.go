// Package sqlite implementa almacen.Almacen sobre un archivo SQLite
// dentro del directorio que recibe Abrir.
package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	_ "modernc.org/sqlite"

	"github.com/sensorwave-dev/sensorwave/almacen"
)

var _ almacen.Almacen = (*motor)(nil)

const (
	claveNodoID   = "nodo_id"
	claveTagsNodo = "tags"
	claveContador = "contador"
	nombreArchivo = "sensorwave.db"
)

const esquema = `
CREATE TABLE IF NOT EXISTS ingesta (
	serie_id INTEGER NOT NULL,
	tiempo INTEGER NOT NULL,
	datos BLOB NOT NULL,
	PRIMARY KEY (serie_id, tiempo)
);
CREATE TABLE IF NOT EXISTS bloques (
	serie_id INTEGER NOT NULL,
	inicio INTEGER NOT NULL,
	fin INTEGER NOT NULL,
	datos BLOB NOT NULL,
	PRIMARY KEY (serie_id, inicio, fin)
);
CREATE TABLE IF NOT EXISTS series (
	path TEXT PRIMARY KEY,
	valor BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS reglas (
	id TEXT PRIMARY KEY,
	valor BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS nodo (
	clave TEXT PRIMARY KEY,
	valor BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS pendientes (
	serie_id INTEGER PRIMARY KEY,
	valor BLOB NOT NULL
);
`

type motor struct {
	db *sql.DB
	mu sync.Mutex
}

// Abrir crea el directorio y abre sensorwave.db adentro.
// journal_mode=WAL y synchronous=FULL.
func Abrir(ruta string) (almacen.Almacen, error) {
	if err := os.MkdirAll(ruta, 0o755); err != nil {
		return nil, err
	}
	archivo := filepath.Join(ruta, nombreArchivo)
	dsn := "file:" + archivo + "?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := preparar(db); err != nil {
		db.Close()
		return nil, err
	}
	return &motor{db: db}, nil
}

func preparar(db *sql.DB) error {
	var modo string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&modo); err != nil {
		return err
	}
	if modo != "wal" {
		return fmt.Errorf("journal_mode=%s", modo)
	}
	var sincro int
	if err := db.QueryRow("PRAGMA synchronous").Scan(&sincro); err != nil {
		return err
	}
	if sincro != 2 {
		return fmt.Errorf("synchronous=%d", sincro)
	}
	_, err := db.Exec(esquema)
	return err
}

func (m *motor) AgregarPunto(p almacen.Punto) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(
		`INSERT OR REPLACE INTO ingesta (serie_id, tiempo, datos) VALUES (?, ?, ?)`,
		p.SerieID, p.Tiempo, p.Datos,
	)
	return err
}

func (m *motor) PuntosIngestaAntiguos(serieID, n int) ([]almacen.Punto, error) {
	if n <= 0 {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	filas, err := m.db.Query(
		`SELECT tiempo, datos FROM ingesta WHERE serie_id = ? ORDER BY tiempo LIMIT ?`,
		serieID, n,
	)
	if err != nil {
		return nil, err
	}
	defer filas.Close()
	return leerPuntos(filas, serieID)
}

func (m *motor) PuntosEnRango(serieID int, desde, hasta int64) ([]almacen.Punto, error) {
	if desde > hasta {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	filas, err := m.db.Query(
		`SELECT tiempo, datos FROM ingesta WHERE serie_id = ? AND tiempo >= ? AND tiempo <= ? ORDER BY tiempo`,
		serieID, desde, hasta,
	)
	if err != nil {
		return nil, err
	}
	defer filas.Close()
	return leerPuntos(filas, serieID)
}

func (m *motor) UltimoPunto(serieID int) (almacen.Punto, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var tiempo int64
	var datos []byte
	err := m.db.QueryRow(
		`SELECT tiempo, datos FROM ingesta WHERE serie_id = ? ORDER BY tiempo DESC LIMIT 1`,
		serieID,
	).Scan(&tiempo, &datos)
	if errors.Is(err, sql.ErrNoRows) {
		return almacen.Punto{}, false, nil
	}
	if err != nil {
		return almacen.Punto{}, false, err
	}
	return almacen.Punto{SerieID: serieID, Tiempo: tiempo, Datos: slices.Clone(datos)}, true, nil
}

func (m *motor) CerrarBloque(b almacen.Bloque, tiempos []int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.db.Exec("BEGIN IMMEDIATE"); err != nil {
		return err
	}
	cerrado := false
	defer func() {
		if !cerrado {
			m.db.Exec("ROLLBACK")
		}
	}()
	if len(b.Datos) > 0 {
		_, err := m.db.Exec(
			`INSERT OR REPLACE INTO bloques (serie_id, inicio, fin, datos) VALUES (?, ?, ?, ?)`,
			b.SerieID, b.Inicio, b.Fin, b.Datos,
		)
		if err != nil {
			return err
		}
	}
	if len(tiempos) > 0 {
		args := make([]any, 0, 1+len(tiempos))
		args = append(args, b.SerieID)
		for _, tiempo := range tiempos {
			args = append(args, tiempo)
		}
		if _, err := m.db.Exec(borrarTiempos(len(tiempos)), args...); err != nil {
			return err
		}
	}
	if _, err := m.db.Exec("COMMIT"); err != nil {
		return err
	}
	cerrado = true
	return nil
}

func (m *motor) BloquesEnRango(serieID int, desde, hasta int64) (almacen.IterBloques, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	filas, err := m.db.Query(
		`SELECT inicio, fin, datos FROM bloques WHERE serie_id = ? AND inicio <= ? AND fin >= ? ORDER BY inicio`,
		serieID, hasta, desde,
	)
	if err != nil {
		return nil, err
	}
	defer filas.Close()
	var bloques []almacen.Bloque
	for filas.Next() {
		var inicio, fin int64
		var datos []byte
		if err := filas.Scan(&inicio, &fin, &datos); err != nil {
			return nil, err
		}
		bloques = append(bloques, almacen.Bloque{
			SerieID: serieID,
			Inicio:  inicio,
			Fin:     fin,
			Datos:   slices.Clone(datos),
		})
	}
	if err := filas.Err(); err != nil {
		return nil, err
	}
	return almacen.NuevosBloques(bloques), nil
}

func (m *motor) BorrarBloque(serieID int, inicio, fin int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(
		`DELETE FROM bloques WHERE serie_id = ? AND inicio = ? AND fin = ?`,
		serieID, inicio, fin,
	)
	return err
}

func (m *motor) BorrarDatosDeSerie(serieID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.enTransaccion(func() error {
		if _, err := m.db.Exec(`DELETE FROM ingesta WHERE serie_id = ?`, serieID); err != nil {
			return err
		}
		_, err := m.db.Exec(`DELETE FROM bloques WHERE serie_id = ?`, serieID)
		return err
	})
}

func (m *motor) GuardarSerie(path string, valor []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(
		`INSERT OR REPLACE INTO series (path, valor) VALUES (?, ?)`,
		path, valor,
	)
	return err
}

func (m *motor) ObtenerSerie(path string) ([]byte, bool, error) {
	return m.obtenerBlob(`SELECT valor FROM series WHERE path = ?`, path)
}

func (m *motor) ListarSeries() (almacen.IterClave, error) {
	return m.listar(`SELECT path, valor FROM series`)
}

func (m *motor) BorrarSerie(path string, serieID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.enTransaccion(func() error {
		if _, err := m.db.Exec(`DELETE FROM ingesta WHERE serie_id = ?`, serieID); err != nil {
			return err
		}
		if _, err := m.db.Exec(`DELETE FROM bloques WHERE serie_id = ?`, serieID); err != nil {
			return err
		}
		_, err := m.db.Exec(`DELETE FROM series WHERE path = ?`, path)
		return err
	})
}

func (m *motor) GuardarRegla(id string, valor []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(
		`INSERT OR REPLACE INTO reglas (id, valor) VALUES (?, ?)`,
		id, valor,
	)
	return err
}

func (m *motor) ObtenerRegla(id string) ([]byte, bool, error) {
	return m.obtenerBlob(`SELECT valor FROM reglas WHERE id = ?`, id)
}

func (m *motor) ListarReglas() (almacen.IterClave, error) {
	return m.listar(`SELECT id, valor FROM reglas`)
}

func (m *motor) BorrarRegla(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(`DELETE FROM reglas WHERE id = ?`, id)
	return err
}

func (m *motor) GuardarNodoID(id string) error {
	return m.guardarNodo(claveNodoID, []byte(id))
}

func (m *motor) ObtenerNodoID() (string, bool, error) {
	valor, ok, err := m.obtenerNodo(claveNodoID)
	if err != nil || !ok {
		return "", ok, err
	}
	return string(valor), true, nil
}

func (m *motor) GuardarTagsNodo(valor []byte) error {
	if valor == nil {
		m.mu.Lock()
		defer m.mu.Unlock()
		_, err := m.db.Exec(`DELETE FROM nodo WHERE clave = ?`, claveTagsNodo)
		return err
	}
	return m.guardarNodo(claveTagsNodo, valor)
}

func (m *motor) ObtenerTagsNodo() ([]byte, bool, error) {
	return m.obtenerNodo(claveTagsNodo)
}

func (m *motor) GuardarContador(valor []byte) error {
	return m.guardarNodo(claveContador, valor)
}

func (m *motor) ObtenerContador() ([]byte, bool, error) {
	return m.obtenerNodo(claveContador)
}

func (m *motor) GuardarPendiente(serieID int, valor []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(
		`INSERT OR REPLACE INTO pendientes (serie_id, valor) VALUES (?, ?)`,
		serieID, valor,
	)
	return err
}

func (m *motor) ListarPendientes() (almacen.IterPendiente, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	filas, err := m.db.Query(`SELECT serie_id, valor FROM pendientes`)
	if err != nil {
		return nil, err
	}
	defer filas.Close()
	var ids []int
	var vals [][]byte
	for filas.Next() {
		var id int
		var valor []byte
		if err := filas.Scan(&id, &valor); err != nil {
			return nil, err
		}
		ids = append(ids, id)
		vals = append(vals, slices.Clone(valor))
	}
	if err := filas.Err(); err != nil {
		return nil, err
	}
	return almacen.NuevosPendientes(ids, vals), nil
}

func (m *motor) BorrarPendiente(serieID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(`DELETE FROM pendientes WHERE serie_id = ?`, serieID)
	return err
}

func (m *motor) Cerrar() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.db.Close()
}

func (m *motor) guardarNodo(clave string, valor []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(
		`INSERT OR REPLACE INTO nodo (clave, valor) VALUES (?, ?)`,
		clave, valor,
	)
	return err
}

func (m *motor) obtenerNodo(clave string) ([]byte, bool, error) {
	return m.obtenerBlob(`SELECT valor FROM nodo WHERE clave = ?`, clave)
}

func (m *motor) obtenerBlob(consulta string, arg any) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var valor []byte
	err := m.db.QueryRow(consulta, arg).Scan(&valor)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return slices.Clone(valor), true, nil
}

func (m *motor) listar(consulta string) (almacen.IterClave, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	filas, err := m.db.Query(consulta)
	if err != nil {
		return nil, err
	}
	defer filas.Close()
	var claves []string
	var vals [][]byte
	for filas.Next() {
		var clave string
		var valor []byte
		if err := filas.Scan(&clave, &valor); err != nil {
			return nil, err
		}
		claves = append(claves, clave)
		vals = append(vals, slices.Clone(valor))
	}
	if err := filas.Err(); err != nil {
		return nil, err
	}
	return almacen.NuevasClaves(claves, vals), nil
}

func (m *motor) enTransaccion(fn func() error) error {
	if _, err := m.db.Exec("BEGIN IMMEDIATE"); err != nil {
		return err
	}
	if err := fn(); err != nil {
		m.db.Exec("ROLLBACK")
		return err
	}
	if _, err := m.db.Exec("COMMIT"); err != nil {
		m.db.Exec("ROLLBACK")
		return err
	}
	return nil
}

func leerPuntos(filas *sql.Rows, serieID int) ([]almacen.Punto, error) {
	var puntos []almacen.Punto
	for filas.Next() {
		var tiempo int64
		var datos []byte
		if err := filas.Scan(&tiempo, &datos); err != nil {
			return nil, err
		}
		puntos = append(puntos, almacen.Punto{
			SerieID: serieID,
			Tiempo:  tiempo,
			Datos:   slices.Clone(datos),
		})
	}
	if err := filas.Err(); err != nil {
		return nil, err
	}
	return puntos, nil
}

func borrarTiempos(n int) string {
	var b strings.Builder
	b.WriteString("DELETE FROM ingesta WHERE serie_id = ? AND tiempo IN (")
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('?')
	}
	b.WriteByte(')')
	return b.String()
}
