package tipos

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

// EnviarJSON envía una respuesta JSON con Content-Type application/json
func EnviarJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Error encodificando JSON: %v", err)
	}
}

// EnviarError envía una respuesta de error en formato JSON
func EnviarError(w http.ResponseWriter, codigo int, mensaje string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(codigo)
	json.NewEncoder(w).Encode(map[string]string{"error": mensaje})
}

// LeerJSON lee y parsea el body de una request como JSON
func LeerJSON(r *http.Request, v interface{}) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("error leyendo body: %v", err)
	}
	defer r.Body.Close()

	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("error parseando JSON: %v", err)
	}
	return nil
}
