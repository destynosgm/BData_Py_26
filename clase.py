# llamado de modulos 

from pyspark.sql import SparkSession 
from pyspark.sql.functions import *

# Crear sesión de spark 

spark = (SparkSession
         .builder
         .appName("Repatarticion_Otros")
         .getOrCreate())

# Llamado de datos 

Flight_csv = spark.read.csv(
    "/content/drive/MyDrive/flight data.csv",
    header = True,
    inferSchema=True
)

# Repartition (10 partes)

flight_repart = Flight_csv.repartition(10)

flight_repart.show()

# ¿Cuantas particiones tiene un dataframe? 

Flight_csv.rdd.getNumPartitions()

flight_repart.rdd.getNumPartitions()


# Uso de partición (exportar csv según el país)

(
    Flight_csv
    .write
    .partitionBy("from_country")
    .parquet("/content/drive/MyDrive/flights")
)

# Vamos a leer nuevamente 

Flight_pq_p = spark.read.parquet("/content/drive/MyDrive/flights")

Flight_pq_p.count()

Flight_pq_p.show()

# Vamos a leer una particion (El caso China)

Flight_China = spark.read.parquet(
    "/content/drive/MyDrive/flights/from_country=China"
  )

Flight_China.show()


# Proceso para usar un Coalesce mediante un proceso (filtros)

Flight_Algeria = Flight_csv.filter(
    col("from_country") == "Algeria"
)

Flight_Algeria.rdd.getNumPartitions()

## Aplicando nuevamente con df particionado en 10

Flight_Algeria = flight_repart.filter(
    col("from_country") == "Algeria"
)

Flight_Algeria.rdd.getNumPartitions()
Flight_Algeria.count()

## Aplicando coalesce 

Flight_Coalesced = Flight_Algeria.coalesce(1)

Flight_Coalesced.rdd.getNumPartitions()






