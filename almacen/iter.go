package almacen

type iterBloques struct {
	items []Bloque
	i     int
	err   error
}

// NuevosBloques arma un iterador sobre una lista ya leída.
func NuevosBloques(items []Bloque) IterBloques {
	return &iterBloques{items: items, i: -1}
}

func (it *iterBloques) Siguiente() bool {
	if it.err != nil || it.i+1 >= len(it.items) {
		return false
	}
	it.i++
	return true
}

func (it *iterBloques) Bloque() Bloque {
	return it.items[it.i]
}

func (it *iterBloques) Error() error { return it.err }

func (it *iterBloques) Close() error { return nil }

type iterClave struct {
	claves []string
	vals   [][]byte
	i      int
	err    error
}

// NuevasClaves arma un iterador de fichas.
func NuevasClaves(claves []string, vals [][]byte) IterClave {
	return &iterClave{claves: claves, vals: vals, i: -1}
}

func (it *iterClave) Siguiente() bool {
	if it.err != nil || it.i+1 >= len(it.claves) {
		return false
	}
	it.i++
	return true
}

func (it *iterClave) Clave() string { return it.claves[it.i] }

func (it *iterClave) Valor() []byte { return it.vals[it.i] }

func (it *iterClave) Error() error { return it.err }

func (it *iterClave) Close() error { return nil }

type iterPendiente struct {
	ids  []int
	vals [][]byte
	i    int
	err  error
}

// NuevosPendientes arma un iterador de eliminaciones pendientes.
func NuevosPendientes(ids []int, vals [][]byte) IterPendiente {
	return &iterPendiente{ids: ids, vals: vals, i: -1}
}

func (it *iterPendiente) Siguiente() bool {
	if it.err != nil || it.i+1 >= len(it.ids) {
		return false
	}
	it.i++
	return true
}

func (it *iterPendiente) SerieID() int { return it.ids[it.i] }

func (it *iterPendiente) Valor() []byte { return it.vals[it.i] }

func (it *iterPendiente) Error() error { return it.err }

func (it *iterPendiente) Close() error { return nil }
