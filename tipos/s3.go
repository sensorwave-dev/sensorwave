package tipos

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ConfiguracionS3 contiene la configuración para conectar con almacenamiento S3-compatible
// (Garage, AWS S3, Cloudflare R2, MinIO, etc.).
// Esta estructura es compartida entre:
// - borde: para migración de datos de series a la nube
// - despachador: para registro y descubrimiento de nodos
type ConfiguracionS3 struct {
	Endpoint        string // URL del servidor S3, ej: http://localhost:3900
	AccessKeyID     string // Access Key ID de S3
	SecretAccessKey string // Secret Access Key de S3
	Bucket          string // Nombre del bucket (borde: datos, despachador: nodos)
	Region          string // Región (puede ser cualquier valor para implementaciones como Garage)
}

// Validar verifica que todos los campos requeridos estén presentes
func (cfg ConfiguracionS3) Validar() error {
	if cfg.Endpoint == "" {
		return fmt.Errorf("Endpoint es requerido")
	}
	if cfg.AccessKeyID == "" {
		return fmt.Errorf("AccessKeyID es requerido")
	}
	if cfg.SecretAccessKey == "" {
		return fmt.Errorf("SecretAccessKey es requerido")
	}
	if cfg.Bucket == "" {
		return fmt.Errorf("Bucket es requerido")
	}
	// Region es opcional, se aplica default "us-east-1" si está vacío
	return nil
}

// AplicarDefaults establece valores por defecto en campos opcionales
func (cfg *ConfiguracionS3) AplicarDefaults() {
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
}

// ClienteS3 define las operaciones S3 utilizadas por el sistema.
// Esta interfaz permite inyectar mocks para testing unitario.
//
// El tipo *s3.Client del AWS SDK implementa todos estos métodos,
// por lo que puede usarse directamente donde se espera ClienteS3
// (Go usa duck typing: si tiene los métodos, implementa la interfaz).
type ClienteS3 interface {
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// CrearClienteS3 crea un cliente S3 configurado para endpoints personalizados.
// Compatible con AWS S3, Garage, MinIO, Cloudflare R2, etc.
// Usa la API moderna de AWS SDK v2 (BaseEndpoint en lugar de EndpointResolver deprecado).
// Retorna un ClienteS3 que puede ser usado directamente o mockeado en tests.
func CrearClienteS3(cfg ConfiguracionS3) (*s3.Client, error) {
	cfg.AplicarDefaults()
	if err := cfg.Validar(); err != nil {
		return nil, fmt.Errorf("configuración S3 inválida: %w", err)
	}

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("error al cargar configuración de AWS: %w", err)
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
	}), nil
}

// GenerarClaveS3Datos genera la clave para un bloque de datos en S3
// Formato: {nodoID}/{serieId}_{tiempoInicio}_{tiempoFin}
func GenerarClaveS3Datos(nodoID string, serieId int, tiempoInicio, tiempoFin int64) string {
	return fmt.Sprintf("%s/%010d_%020d_%020d", nodoID, serieId, tiempoInicio, tiempoFin)
}

// GenerarPrefijoS3Serie genera el prefijo para listar/eliminar datos de una serie
// Formato: {nodoID}/{serieId}_
func GenerarPrefijoS3Serie(nodoID string, serieId int) string {
	return fmt.Sprintf("%s/%010d_", nodoID, serieId)
}

// ParsearClaveS3Datos extrae los componentes de una clave S3
// Entrada: {nodoID}/{serieId}_{tiempoInicio}_{tiempoFin}
// Retorna serieId, tiempoInicio, tiempoFin o error si el formato es inválido
func ParsearClaveS3Datos(clave string) (serieId int, tiempoInicio, tiempoFin int64, err error) {
	partes := strings.Split(clave, "/")
	if len(partes) != 2 {
		return 0, 0, 0, fmt.Errorf("formato de clave inválido: %s", clave)
	}

	componentes := strings.Split(partes[1], "_")
	if len(componentes) != 3 {
		return 0, 0, 0, fmt.Errorf("formato de nombre inválido: %s", partes[1])
	}

	serieId64, err := strconv.ParseInt(strings.TrimLeft(componentes[0], "0"), 10, 32)
	if err != nil && componentes[0] != "0000000000" {
		return 0, 0, 0, fmt.Errorf("error parseando serieId: %v", err)
	}

	tiempoInicio, err = strconv.ParseInt(strings.TrimLeft(componentes[1], "0"), 10, 64)
	if err != nil && componentes[1] != strings.Repeat("0", 20) {
		return 0, 0, 0, fmt.Errorf("error parseando tiempoInicio: %v", err)
	}

	tiempoFin, err = strconv.ParseInt(strings.TrimLeft(componentes[2], "0"), 10, 64)
	if err != nil && componentes[2] != strings.Repeat("0", 20) {
		return 0, 0, 0, fmt.Errorf("error parseando tiempoFin: %v", err)
	}

	return int(serieId64), tiempoInicio, tiempoFin, nil
}
