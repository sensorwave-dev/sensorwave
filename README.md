# SensorWave

## Consideraciones

En un sistema de Internet de las Cosas tradicional, los datos recopilados por los nodos sensores y actuadores son enviados a la nube para su almacenamiento y análisis. Los nodos reciben como respuesta comandos o instrucciones de control que producen cambios en sus actuadores. Este enfoque conduce a una alta latencia en la comunicación, un flujo de datos ascendente alto y mayores costos en los centros de datos en la nube. Adicionalmente, muchos sistemas de Internet de las Cosas experimentan problemas de conectividad que provocan la pérdida de datos si no existe un almacenamiento local.

Los nodos basados en procesadores de bajo consumo tienen limitaciones de procesador y memoria, pero al borde de la red tienen la capacidad de almacenar datos, reducir la latencia, aumentar la confiabilidad y permitir la toma de decisiones.

## Objetivos

Se pretende desarrollar un  sistema de almacenamiento distribuido, transparente al usuario final, donde los datos residan al borde de la red y en un servicio de almacenamiento en la nube. Las solicitudes al sistema pueden provenir de usuarios locales a un nodo al borde o de usuarios conectados a uno o varios servidores despachadores ubicados en la nube (o al borde). 

Los nodos sensores y actuadores son responsables de la transmisión de datos a los nodos al borde y del cambio de estado de los actuadores a petición de los nodos al borde. Para la comunicación entre los nodos al borde y los nodos sensores y actuadores se pueden utilizar los protocolos CoAP, HTTP o MQTT mediante un middleware que soporta el paradigma publicación-suscripción.

Cada nodo al borde, equipado con una base de datos, un broker MQTT y servidores del middleware se encargan de capturar y almacenar los datos enviados por los nodos sensores y actuadores como series y ofrece un motor de reglas que permite modificar el estado de los actuadores.

Los nodos al borde tienen recursos de almacenamiento limitados y sólo pueden almacenar datos durante un período de tiempo determinado. Por ello se utiliza un sistema de compresión que permite determinar por cada serie el algoritmo de compresión y se usa el concepto de "Tiempo de almacenamiento", que permite definir un período de tiempo durante el cual los datos deben residir localmente.

Cuando se cumple el tiempo de almacenamiento, los datos son automáticamente marcados para ser migrados a un servicio de almacenamiento en la nube.

En la nube, el servicio despachador es responsable de realizar solicitudes a los nodos al borde o al servicio de almacenamiento en la nube y enviar respuestas a los clientes. El servicio despachador es escalable horizontalmente.

Un cliente puede realizar consultas locales o globales, donde las consultas locales abarcan los datos almacenados al borde y las globales pueden involucrar a distintos nodos al borde y al servicio de almacenamiento en la nube. Las consultas globales solo se pueden realizar mediante el servicio despachador.

En el sistema propuesto, las aplicaciones desplegadas al borde de la red pueden continuar funcionando ante eventuales problemas de conectividad entre el borde y la nube debido a que los nodos al borde disponen de almacenamiento local y un motor de reglas. Las consultas locales no se ven afectadas ante problemas de conectividad, pero si se realiza una consulta global el sistema retorna como parte de la respuesta a qué nodos al borde no pudo acceder.


## Herramientas a utilizar

Lenguajes de programación:
- Golang

Artefactos:
- Middleware para obtener datos de sensores y modificar estado de actuadores al borde.
- Base de datos al borde de la red basada en Pebble
- Almacenamiento en la nube basado en S3-compatible (Garage, AWS S3, Cloudflare R2, MinIO, etc.)
- Servicio despachador de consultas en la nube sin estado
- Registro de nodos unificado en S3 (usado por edge y despachador)
