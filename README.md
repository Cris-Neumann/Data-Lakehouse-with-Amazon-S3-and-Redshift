<h1 align="center"> Data Lakehouse con Amazon S3, MongoDB y Amazon Redshift </h1>

## Índice

- [Resumen del proyecto](#Resumen-del-proyecto)
- [Modelado de datos](#Modelado-de-datos)

## Resumen del proyecto
Este proyecto de Data Lakehouse se alimenta desde una base de datos NoSQL MongoDB, la cual almacena el dataset de Yelp
(ver en Kaggle: https://acortar.link/vl8Q5S) en colecciones de datos, a las cuales se accede mediante API de Python para
reducir en volumen (AWS free tier tiene limte de 5GB para S3) e insertar en un bucket de Amazon S3 para almcenamiento de archivos json en bruto
como Data Lake. Posteriormente, son modificados los archivos para obtener sólo los atributos que interesan procesar, y guardar
en directorio staging en el mismo bucket de S3. El paso siguiente es modelar los datos para generar un esquema constelación
de Data Warehouse de ejemplo, el cual es implementado en Amazon Redshift, dada su estructra orientada a columnas,
y por ende ideal para analítica y reporting, y es poblado con datos provenientes del bucket mediante la conexión nativa entre S3 y
Redshift, usando el metodo "copy" y su opción "jsonpath". Finalmente, se muestra un ejemplo de la conexion por defecto que trae
Tableau con Redshift y potenciales preguntas de negocio sobre el dataset de Yelp que se derivan de la visualización de los datos.

## Modelado de datos
Yelp es una plataforma que recopila información sobre negocios del entorno en tiempo real, esto es: reseñas, consejos y fotografías sobre cada negocio. También
se usa para que sus propietarios puedan conectar con sus clientes, fomentar sus negocios y los clientes puedan compartir información entre ellos.
El dataset de Yelp consta de 5 archivos json, cada uno con información distinta acerca del negocio, los cuales son:

-  business.json: Contiene datos comerciales, incluidos datos de ubicación, atributos y categorías.
-  checkin.json: Registros en un negocio (su id y fecha).
-  review.json: Contiene el texto de la reseña, incluido el id de usuario que escribió la reseña y el id de empresa para la cual la escribió.
-  tip.json: Consejos escritos por un usuario sobre un negocio. Los consejos son más breves que las reseñas y transmiten sugerencias rápidas.
-  user.json:	Datos del usuario, incluida la asignación de amigos del usuario y todos los metadatos asociados con el usuario.

Estos son los atributos que tiene cada archivo json en bruto en MongoDB, donde los campos entre llaves ('{}') son campos anidados del dataset:

<div align="center">
	<img width="397" alt="tablas_raw" src="https://github.com/Cris-Neumann/Data-Lakehouse-with-Amazon-S3-and-Redshift/assets/99703152/af19e104-bd04-4ff9-8f78-b7c5b0621611">
</div>

Luego de la transformación ejecutada en el bucket de S3 para cargar los archivos modificados en staging, la cual reduce el volumen de los datos, dada la limitación
de espacio en AWS free tier, los archivos json quedan con estos atributos, donde los últimos campos del json "business" contiene campos extraidos desde el campo anidado "attributes":

<div align="center">
	<img width="550" alt="tablas_staging" src="https://github.com/Cris-Neumann/Data-Lakehouse-with-Amazon-S3-and-Redshift/assets/99703152/fcb7ae7d-4bb7-4595-9285-3693c7a61d53">
</div>

Finalmente, luego de tener los datos preprocesados en S3 y listos para insertar a Redshift, el esquema de ejemplo que se utilizará como Data Warehouse es uno llamado
"esquema constelación" (ver en web de IBM: https://acortar.link/qWSUMB), el cual es punto medio entre los esquemas "estrella" y "copo de nieve", basandose en las
llamadas tablas de "hechos" (medidas de negocio) y "dimensiones" (atributos de las tablas de hechos), y creando el siguiente esquema:

<div align="center">
	<img width="481" alt="esquema" src="https://github.com/Cris-Neumann/Data-Lakehouse-with-Amazon-S3-and-Redshift/assets/99703152/2b9056f1-734c-477b-8f9b-9e30b4a5d1de">
</div>


