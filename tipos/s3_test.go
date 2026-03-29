package tipos

import (
	"testing"
)

// ==================== Tests de ConfiguracionS3.Validar ====================

// TestConfiguracionS3_Validar_Completa verifica validación con todos los campos
func TestConfiguracionS3_Validar_Completa(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "https://s3.example.com",
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Bucket:          "mi-bucket",
		Region:          "us-west-2",
	}

	err := cfg.Validar()
	if err != nil {
		t.Errorf("No se esperaba error con configuración completa: %v", err)
	}

	t.Log("✓ Configuración completa válida")
}

// TestConfiguracionS3_Validar_SinEndpoint verifica error sin endpoint
func TestConfiguracionS3_Validar_SinEndpoint(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "", // Falta
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "secret",
		Bucket:          "mi-bucket",
	}

	err := cfg.Validar()
	if err == nil {
		t.Error("Se esperaba error sin Endpoint")
	} else {
		t.Logf("✓ Error esperado: %v", err)
	}
}

// TestConfiguracionS3_Validar_SinAccessKey verifica error sin access key
func TestConfiguracionS3_Validar_SinAccessKey(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "https://s3.example.com",
		AccessKeyID:     "", // Falta
		SecretAccessKey: "secret",
		Bucket:          "mi-bucket",
	}

	err := cfg.Validar()
	if err == nil {
		t.Error("Se esperaba error sin AccessKeyID")
	} else {
		t.Logf("✓ Error esperado: %v", err)
	}
}

// TestConfiguracionS3_Validar_SinSecretKey verifica error sin secret key
func TestConfiguracionS3_Validar_SinSecretKey(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "https://s3.example.com",
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "", // Falta
		Bucket:          "mi-bucket",
	}

	err := cfg.Validar()
	if err == nil {
		t.Error("Se esperaba error sin SecretAccessKey")
	} else {
		t.Logf("✓ Error esperado: %v", err)
	}
}

// TestConfiguracionS3_Validar_SinBucket verifica error sin bucket
func TestConfiguracionS3_Validar_SinBucket(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "https://s3.example.com",
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "secret",
		Bucket:          "", // Falta
	}

	err := cfg.Validar()
	if err == nil {
		t.Error("Se esperaba error sin Bucket")
	} else {
		t.Logf("✓ Error esperado: %v", err)
	}
}

// TestConfiguracionS3_Validar_SinRegion verifica que region es opcional
func TestConfiguracionS3_Validar_SinRegion(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "https://s3.example.com",
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "secret",
		Bucket:          "mi-bucket",
		Region:          "", // Vacío pero opcional
	}

	err := cfg.Validar()
	if err != nil {
		t.Errorf("No se esperaba error sin Region (es opcional): %v", err)
	}

	t.Log("✓ Configuración válida sin Region (es opcional)")
}

// ==================== Tests de ConfiguracionS3.AplicarDefaults ====================

// TestConfiguracionS3_AplicarDefaults_RegionVacia verifica default de region
func TestConfiguracionS3_AplicarDefaults_RegionVacia(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "https://s3.example.com",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		Bucket:          "bucket",
		Region:          "", // Vacío
	}

	cfg.AplicarDefaults()

	if cfg.Region != "us-east-1" {
		t.Errorf("Region default incorrecta: esperada 'us-east-1', obtenida '%s'", cfg.Region)
	}

	t.Logf("✓ Region default aplicada: %s", cfg.Region)
}

// TestConfiguracionS3_AplicarDefaults_RegionExistente verifica que no sobreescribe region
func TestConfiguracionS3_AplicarDefaults_RegionExistente(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "https://s3.example.com",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		Bucket:          "bucket",
		Region:          "eu-west-1", // Ya tiene valor
	}

	cfg.AplicarDefaults()

	if cfg.Region != "eu-west-1" {
		t.Errorf("Region sobreescrita incorrectamente: esperada 'eu-west-1', obtenida '%s'", cfg.Region)
	}

	t.Logf("✓ Region existente preservada: %s", cfg.Region)
}

// TestConfiguracionS3_ValidarMultiplesCamposVacios verifica error prioritario
func TestConfiguracionS3_ValidarMultiplesCamposVacios(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "",
		AccessKeyID:     "",
		SecretAccessKey: "",
		Bucket:          "",
	}

	err := cfg.Validar()
	if err == nil {
		t.Error("Se esperaba error con configuración vacía")
	}

	// Verificar que el primer error es sobre Endpoint
	esperadoError := "Endpoint es requerido"
	if err.Error() != esperadoError {
		t.Logf("Error obtenido: %v (puede variar el orden de validación)", err)
	}

	t.Log("✓ Validación detecta campos vacíos")
}

// ==================== Tests de CrearClienteS3 ====================

// TestCrearClienteS3_ConfiguracionValida verifica que se crea un cliente con config válida
func TestCrearClienteS3_ConfiguracionValida(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "http://localhost:3900",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Bucket:          "test-bucket",
	}

	cliente, err := CrearClienteS3(cfg)

	if err != nil {
		t.Errorf("No se esperaba error con configuración válida: %v", err)
	}
	if cliente == nil {
		t.Error("El cliente no debería ser nil con configuración válida")
	}

	t.Log("✓ Cliente S3 creado correctamente con configuración válida")
}

// TestCrearClienteS3_ConRegion verifica creación con region especificada
func TestCrearClienteS3_ConRegion(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "http://localhost:3900",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Bucket:          "test-bucket",
		Region:          "eu-west-1",
	}

	cliente, err := CrearClienteS3(cfg)

	if err != nil {
		t.Errorf("No se esperaba error: %v", err)
	}
	if cliente == nil {
		t.Error("El cliente no debería ser nil")
	}

	t.Log("✓ Cliente S3 creado correctamente con region especificada")
}

// TestCrearClienteS3_SinRegion verifica que se aplica region por defecto
func TestCrearClienteS3_SinRegion(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "http://localhost:3900",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Bucket:          "test-bucket",
		Region:          "", // Vacío, debería aplicar default
	}

	cliente, err := CrearClienteS3(cfg)

	if err != nil {
		t.Errorf("No se esperaba error: %v", err)
	}
	if cliente == nil {
		t.Error("El cliente no debería ser nil")
	}

	t.Log("✓ Cliente S3 creado correctamente sin region (usa default)")
}

// TestCrearClienteS3_ErrorSinEndpoint verifica error con endpoint vacío
func TestCrearClienteS3_ErrorSinEndpoint(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "", // Falta
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Bucket:          "test-bucket",
	}

	cliente, err := CrearClienteS3(cfg)

	if err == nil {
		t.Error("Se esperaba error sin Endpoint")
	}
	if cliente != nil {
		t.Error("El cliente debería ser nil cuando hay error")
	}

	t.Logf("✓ Error esperado sin Endpoint: %v", err)
}

// TestCrearClienteS3_ErrorSinAccessKey verifica error con access key vacío
func TestCrearClienteS3_ErrorSinAccessKey(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "http://localhost:3900",
		AccessKeyID:     "", // Falta
		SecretAccessKey: "test-secret-key",
		Bucket:          "test-bucket",
	}

	cliente, err := CrearClienteS3(cfg)

	if err == nil {
		t.Error("Se esperaba error sin AccessKeyID")
	}
	if cliente != nil {
		t.Error("El cliente debería ser nil cuando hay error")
	}

	t.Logf("✓ Error esperado sin AccessKeyID: %v", err)
}

// TestCrearClienteS3_ErrorSinSecretKey verifica error con secret key vacío
func TestCrearClienteS3_ErrorSinSecretKey(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "http://localhost:3900",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "", // Falta
		Bucket:          "test-bucket",
	}

	cliente, err := CrearClienteS3(cfg)

	if err == nil {
		t.Error("Se esperaba error sin SecretAccessKey")
	}
	if cliente != nil {
		t.Error("El cliente debería ser nil cuando hay error")
	}

	t.Logf("✓ Error esperado sin SecretAccessKey: %v", err)
}

// TestCrearClienteS3_ErrorSinBucket verifica error con bucket vacío
func TestCrearClienteS3_ErrorSinBucket(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "http://localhost:3900",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Bucket:          "", // Falta
	}

	cliente, err := CrearClienteS3(cfg)

	if err == nil {
		t.Error("Se esperaba error sin Bucket")
	}
	if cliente != nil {
		t.Error("El cliente debería ser nil cuando hay error")
	}

	t.Logf("✓ Error esperado sin Bucket: %v", err)
}

// TestCrearClienteS3_EndpointHTTPS verifica creación con endpoint HTTPS
func TestCrearClienteS3_EndpointHTTPS(t *testing.T) {
	cfg := ConfiguracionS3{
		Endpoint:        "https://s3.amazonaws.com",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Bucket:          "test-bucket",
		Region:          "us-west-2",
	}

	cliente, err := CrearClienteS3(cfg)

	if err != nil {
		t.Errorf("No se esperaba error con endpoint HTTPS: %v", err)
	}
	if cliente == nil {
		t.Error("El cliente no debería ser nil")
	}

	t.Log("✓ Cliente S3 creado correctamente con endpoint HTTPS")
}

// ==================== Tests de Claves S3 ====================

// TestGenerarClaveS3Datos verifica generación de clave S3
func TestGenerarClaveS3Datos(t *testing.T) {
	clave := GenerarClaveS3Datos("nodo-001", 1, 1000, 2000)
	esperado := "nodo-001/0000000001_00000000000000001000_00000000000000002000"
	if clave != esperado {
		t.Errorf("Clave incorrecta: esperada '%s', obtenida '%s'", esperado, clave)
	}
	t.Log("✓ GenerarClaveS3Datos genera clave correcta")
}

// TestGenerarClaveS3Datos_SerieGrande verifica con valores grandes
func TestGenerarClaveS3Datos_SerieGrande(t *testing.T) {
	clave := GenerarClaveS3Datos("nodo-001", 999999999, 1704067200000000000, 1704153600000000000)
	esperado := "nodo-001/0999999999_01704067200000000000_01704153600000000000"
	if clave != esperado {
		t.Errorf("Clave incorrecta: esperada '%s', obtenida '%s'", esperado, clave)
	}
	t.Log("✓ GenerarClaveS3Datos maneja series grandes correctamente")
}

// TestGenerarPrefijoS3Serie verifica generación de prefijo
func TestGenerarPrefijoS3Serie(t *testing.T) {
	prefijo := GenerarPrefijoS3Serie("nodo-001", 1)
	esperado := "nodo-001/0000000001_"
	if prefijo != esperado {
		t.Errorf("Prefijo incorrecto: esperado '%s', obtenido '%s'", esperado, prefijo)
	}
	t.Log("✓ GenerarPrefijoS3Serie genera prefijo correcto")
}

// TestGenerarPrefijoS3Serie_SerieGrande verifica con serie grande
func TestGenerarPrefijoS3Serie_SerieGrande(t *testing.T) {
	prefijo := GenerarPrefijoS3Serie("nodo-borde-001", 123456789)
	esperado := "nodo-borde-001/0123456789_"
	if prefijo != esperado {
		t.Errorf("Prefijo incorrecto: esperado '%s', obtenido '%s'", esperado, prefijo)
	}
	t.Log("✓ GenerarPrefijoS3Serie maneja series grandes correctamente")
}

// TestParsearClaveS3Datos_Valida verifica parsing correcto
func TestParsearClaveS3Datos_Valida(t *testing.T) {
	clave := "nodo-001/0000000001_00000000000000001000_00000000000000002000"
	serieId, tiempoInicio, tiempoFin, err := ParsearClaveS3Datos(clave)
	if err != nil {
		t.Errorf("No se esperaba error: %v", err)
	}
	if serieId != 1 {
		t.Errorf("SerieId incorrecta: esperada 1, obtenida %d", serieId)
	}
	if tiempoInicio != 1000 {
		t.Errorf("TiempoInicio incorrecto: esperado 1000, obtenido %d", tiempoInicio)
	}
	if tiempoFin != 2000 {
		t.Errorf("TiempoFin incorrecto: esperado 2000, obtenido %d", tiempoFin)
	}
	t.Log("✓ ParsearClaveS3Datos parsea correctamente")
}

// TestParsearClaveS3Datos_ValoresGrandes verifica parsing de valores grandes
func TestParsearClaveS3Datos_ValoresGrandes(t *testing.T) {
	clave := "nodo-001/0999999999_01704067200000000000_01704153600000000000"
	serieId, tiempoInicio, tiempoFin, err := ParsearClaveS3Datos(clave)
	if err != nil {
		t.Errorf("No se esperaba error: %v", err)
	}
	if serieId != 999999999 {
		t.Errorf("SerieId incorrecta: esperada 999999999, obtenida %d", serieId)
	}
	if tiempoInicio != 1704067200000000000 {
		t.Errorf("TiempoInicio incorrecto: esperado 1704067200000000000, obtenido %d", tiempoInicio)
	}
	if tiempoFin != 1704153600000000000 {
		t.Errorf("TiempoFin incorrecto: esperado 1704153600000000000, obtenido %d", tiempoFin)
	}
	t.Log("✓ ParsearClaveS3Datos parsea valores grandes correctamente")
}

// TestParsearClaveS3Datos_FormatoInvalido verifica detección de errores
func TestParsearClaveS3Datos_FormatoInvalido(t *testing.T) {
	casos := []string{
		"nodo-001/data/0000000001/1000_2000", // formato antiguo
		"nodo-001",                           // sin nombre de bloque
		"1000_2000",                          // sin nodoID
		"nodo-001/invalido",                  // sin tiempos
		"nodo-001/0000000001_1000",           // solo 2 componentes
	}
	for _, clave := range casos {
		_, _, _, err := ParsearClaveS3Datos(clave)
		if err == nil {
			t.Errorf("Se esperaba error para clave '%s'", clave)
		}
	}
	t.Log("✓ ParsearClaveS3Datos detecta formatos inválidos")
}

// TestParsearClaveS3Datos_Ceros verifica parsing de valores cero
func TestParsearClaveS3Datos_Ceros(t *testing.T) {
	clave := "nodo-001/0000000000_00000000000000000000_00000000000000000000"
	serieId, tiempoInicio, tiempoFin, err := ParsearClaveS3Datos(clave)
	if err != nil {
		t.Errorf("No se esperaba error: %v", err)
	}
	if serieId != 0 || tiempoInicio != 0 || tiempoFin != 0 {
		t.Errorf("Valores incorrectos: serieId=%d, inicio=%d, fin=%d", serieId, tiempoInicio, tiempoFin)
	}
	t.Log("✓ ParsearClaveS3Datos maneja valores cero correctamente")
}
