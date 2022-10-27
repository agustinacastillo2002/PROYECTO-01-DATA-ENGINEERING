Bienvenidos 
les presento mi  proyecto 01 de Data Engineering!


En esta carpeta se encontraran con varios archivos 

creadb, es el archivo SQL que tendran  que reproducir primero para
crear la base de datos.

Main, es el archivo .Py que tienen que reproducir para que empieze a 
abrir, convertir, limpiar, transformar y cargar a la base de datos MySql los archivos. 

Hay dos carpetas de apoyo que son para funcionalidad del Main, en caso de que esten vacias.

Un archivo PNG, que es el diagrama de flujo de mi proyecto, con los pasos a seguir. 


Pueden ver el video y seguir los pasos!

Les dejare un instructivo de como Inicializar este proceso.

*Cosas a tener en cuenta*
 Los datasets deben estar en la misma carpeta que el script de python.

Paso 1 

Abrir el archivo creardb.sql y ejecutar las dos primeras lineas, las cuales crean la database y la ponen en uso. 

Paso 2

Ejecutar Script main.py 

Este se encargara de Convertir todos los archivos de diferentes formatos a csv para poder trabajar con mayor eficacia en ellos (Los guardara en la carpeta csvfiles). Al momento de limpiarlos, transformarlos e inputar los valores faltantes, los abrira de la carpeta *csvfiles* luego de limpiarlos, los guardara en la carpeta *archivoslistos* . 

La ultima parte y mas importante, es la carga de estos archivos a la base de datos MySql. Se realiza mediante SqlAlchemy. 
Al finalizar el script, si abrimos la base de datos creada en el paso 1 en la seccion tables, veremos que estan creadas las tablas con sus valores correspondientes inputados. 

!IMPORTANTE!

SqlAlchemy no pudo subir el archivo *archivo2listo*. Por lo cual tendremos que hacer algunos pasos mas :

 Primero copiamos y pegamos el csv en la carpeta "C:\ProgramData\MySQL\MySQL Server 5.7\Uploads"
 Luego ejecutamos la query que nos crea la tabla productos y posterior el *loaddatainfile* que nos carga los datos del archivo *archivo2listo* en la tabla. 

FIN


Luego de ello lo unico que nos quedaria por hacer es realizar la query, la ejecutamos y vemos que nos da 
*203.80414580588536*







