<h1 align="center"> Data Lakehouse con Amazon S3, MongoDB y Amazon Redshift </h1>

## Índice

- [Resumen del proyecto](#Resumen-del-proyecto)

## Resumen del proyecto
Este proyecto de Data Lakehouse se alimenta desde una base de datos NoSQL MongoDB, la cual almacena el dataset de Yelp
(en Kaggle: https://acortar.link/vl8Q5S) en colecciones de datos, a las cuales se accede mediante API de Python para
reducir en volumen (AWS free tier tiene limte de 5GB para S3) e insertar en un bucket de Amazon S3 para almcenamiento de archivos json en bruto
como Data Lake. Posteriormente, son modificados los archivos para obtener sólo los atributos que interesan procesar, y guardar
en directorio stagging en el mismo bucket de S3. El paso siguiente es modelar los datos para generar un esquema constelación
de Data Warehouse de ejemplo, el cual es implementado en Amazon Redshift, dada su estructra orientada a columnas,
y por ende ideal para analítica y reporting, y es poblado con datos provenientes del bucket mediante la conexión nativa en AWS con
Redshift, usando el metodo "copy" y su opción "jsonpath". Finalmente, se muestra un ejemplo de la conexion por defecto que trae
Tableau con Redshift y potenciales preguntas de negocio sobre el dataset de Yelp que se derivan de la visualización de datos.
