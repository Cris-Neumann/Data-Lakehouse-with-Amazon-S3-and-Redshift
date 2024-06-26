<h1 align="center"> Data Lakehouse con Amazon S3, MongoDB y Amazon Redshift </h1>

## Índice

- [Resumen del proyecto](#Resumen-del-proyecto)
- [Arquitectura empleada](#Arquitectura-empleada)
- [Modelado de datos](#Modelado-de-datos)
- [Detalles de ejecución](#Detalles-de-ejecución)
- [Dashboard y preguntas de negocio](#Dashboard-y-preguntas-de-negocio)
- [Instalaciones adicionales](#Instalaciones-adicionales)

## Resumen del proyecto
Este proyecto de Data Lakehouse se alimenta desde una base de datos NoSQL MongoDB, la cual almacena el dataset de Yelp
(ver en Kaggle: https://acortar.link/vl8Q5S) en colecciones de datos, a las cuales se accede mediante una API de Python para
reducir en volumen (AWS free tier tiene limte de 5GB para S3) e insertar en un bucket de Amazon S3 (Simple Storage Service)
para almacenamiento de archivos json en bruto como Data Lake. Posteriormente, son modificados los archivos para obtener sólo
los atributos que interesan procesar para este caso, y guardar en directorio staging en el bucket de S3. El paso siguiente es
modelar los datos para generar un esquema constelación de Data Warehouse de ejemplo, el cual es implementado en Amazon Redshift,
dada su estructra orientada a columnas, y por ende ideal para analítica y reporting, y es poblado con datos provenientes del
bucket mediante la conexión nativa entre S3 y Redshift, usando el metodo "copy" y su opción "jsonpath". Finalmente, se muestra
un ejemplo de la conexion por defecto que trae Tableau con Redshift y potenciales preguntas de negocio sobre el dataset de Yelp
que se derivan de la visualización de los datos.

## Arquitectura empleada
El esquema general del modo en que se relacionan las partes del sistema es el siguiente:

<div align="center">
	<img width="800" alt="tablas_raw" src="https://github.com/Cris-Neumann/Data-Lakehouse-with-Amazon-S3-and-Redshift/assets/99703152/674356e4-a253-48d3-a993-a8af2071333a">
</div>

## Modelado de datos
Yelp es una plataforma que recopila información sobre negocios en tiempo real, esto es: reseñas, consejos y fotografías sobre cada negocio. También
se usa para que sus propietarios puedan conectar con sus clientes, fomentar sus empresas y los clientes puedan compartir información entre ellos.
El dataset de Yelp consta de 5 archivos json, cada uno con información distinta acerca de los negocios, los cuales son:

-  business.json: Contiene datos comerciales, incluidos datos de ubicación, atributos y categorías.
-  checkin.json: Registros en un negocio (su id y fecha).
-  review.json: Contiene el texto de la reseña, incluido el id de usuario que escribió la reseña y el id de empresa para la cual la escribió.
-  tip.json: Consejos escritos por un usuario sobre un negocio. Los consejos son más breves que las reseñas y transmiten sugerencias rápidas.
-  user.json:	Datos del usuario, incluida la asignación de amigos del usuario y todos los metadatos asociados con el usuario.

Estos son los atributos que tiene cada archivo json en bruto en MongoDB, donde los campos entre llaves ('{}') son campos anidados del dataset:

<div align="center">
	<img width="600" alt="tablas_raw" src="https://github.com/Cris-Neumann/Data-Lakehouse-with-Amazon-S3-and-Redshift/assets/99703152/af19e104-bd04-4ff9-8f78-b7c5b0621611">
</div>

Luego de la transformación ejecutada en el bucket de S3 para cargar los archivos modificados en staging, la cual reduce el volumen de los datos, dada la limitación
de espacio en AWS free tier, los archivos json quedan con estos atributos, donde los últimos campos del json "business" contiene campos extraidos desde el campo anidado "attributes":

<div align="center">
	<img width="600" alt="tablas_staging" src="https://github.com/Cris-Neumann/Data-Lakehouse-with-Amazon-S3-and-Redshift/assets/99703152/fcb7ae7d-4bb7-4595-9285-3693c7a61d53">
</div>

Finalmente, luego de tener los datos preprocesados en S3 y listos para insertar a Redshift, el esquema de ejemplo que se utilizará como Data Warehouse es uno llamado
"esquema constelación" (ver en web de IBM: https://acortar.link/qWSUMB), el cual es punto medio entre los esquemas "estrella" y "copo de nieve", basandose en las
llamadas tablas de "hechos" (medidas de negocio) y "dimensiones" (atributos de las tablas de hechos), y creando el siguiente esquema:

<div align="center">
	<img width="600" alt="esquema" src="https://github.com/Cris-Neumann/Data-Lakehouse-with-Amazon-S3-and-Redshift/assets/99703152/2b9056f1-734c-477b-8f9b-9e30b4a5d1de">
</div>

## Detalles de ejecución
**1)** Para poder cargar los archivos json desde S3 hasta Redshift, se usa una conexión nativa de AWS entre dichos servicios, usando el metodo "copy" y su opción "jsonpath", donde esta última es una forma estandarizada para consultar elementos de un objeto JSON. Estos jsonpath deben insertarse previamente en el bucket de S3,
trabajo que lo realiza el script "insert_jsonpath.py", hacia el directorio "yelp_jsonpath_files". Por ejemplo, para el json "review", que contiene las reseñas de clientes,
el jsonpath tiene la siguiente estructura: 

```
{
  "jsonpaths":
  [
    "$['review_id']",
    "$['user_id']",
    "$['business_id']",
    "$['stars']",
    "$['useful']",
    "$['funny']",
    "$['cool']",
    "$['text']",
    "$['date']"
  ]
}
```
**2)** Con la ejecución del script create_table.py se crearán todas las tablas necesarias en Amazon Redshift. </br>
**3)** Al ejecutar el script insert_data_lake.py se cargará el dataset de Yelp desde MongoDB hacia Amazon S3. </br>
**4)** Luego, con la ejecución del script transform_data_lake.py se modifican los formatos de los json anidados y se reduce el volumen de datos. </br>
**5)** Finalmente, con la ejecución del script etl_data_warehouse.py se insertan todos los registros de S3 en el Data Warehouse de Redshift.

## Dashboard y preguntas de negocio
A continuación, una vista diseñada en Tableau, dada su conexión incorporada con Redshift, cuyo archivo está alojado en directorio 'dashboard', a modo de ejemplo de las
distintas visualizaciones de datos que podrían incorporarse, donde se muestran los promedios de calificación por ciudad, la cantidad de negocios por ciudad, 
y el detalle dentro de cada ciudad de la calificación de cada negocio. Por ejemplo, el usuario del dahsboard podría, a raíz de la imagen, preguntarse y 
ahondar en los detalles de por qué la ciudad de New Orleans tiene un promedio de calificación de 4.1 (escala de 1 a 5), teniendo 20 negocios activos.

<div align="center">
	<img width="600" alt="esquema" src="https://github.com/Cris-Neumann/Data-Lakehouse-with-Amazon-S3-and-Redshift/assets/99703152/9137903a-0143-4e36-b5f0-776fd7a6060c">
</div>

Y por otro lado, la ciudad de Philadelphia tiene un promedio de calificación de 2.9, teniendo 15 negocios activos, donde podría apreciarse qué locales tienen muy baja evaluación y entender el motivo de dichos números agregados.

<div align="center">
	<img width="600" alt="esquema" src="https://github.com/Cris-Neumann/Data-Lakehouse-with-Amazon-S3-and-Redshift/assets/99703152/cf96cdeb-6a79-459a-a79d-93ef981c7d38">
</div>

## Instalaciones adicionales
- Se requiere tener instalado las siguientes 4 librerías de Python: boto3 1.34.68, configparser, psycopg2 2.9.6 y pymongo 4.6.3.
- Se debe poseer una access key id y una secret access key de AWS. Para poder acceder a la nube de Amazon Web Services, se puede instalar una versión de prueba: https://aws.amazon.com/es/free/start-your-free-trial/
- En Amazon S3 se debe crear un bucket llamado 'streaming-bucket-1', que cuente con los siguientes directorios en su interior: 'raw_yelp_files',
'staging_yelp_files' y 'yelp_jsonpath_files'.
- Debe crear un cluster de Amazon Redshift llamado 'redshift-cluster-1', y dentro de él crear una base de datos.
- Finalmente, en AWS debe crear un rol de IAM (Identity and Access Management) que permita la conexión y gestión de S3 y Redshift. Para este caso de ejemplo,
se creó un rol que utiliza una adaptación de la política (permisos) predefinida por AWS llamada 'AWSServiceRoleForRedshift', para que pueda conectarse con S3,
añadiendo en el sector 'Statement', un 'Action' que permite la conexión a S3 (ver cómo construir políticas de AWS: https://acortar.link/BbMCqQ).


