# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 20:20:44 2026

@author: SALVADOR
"""

# Crear sesión de PySpark 

from pyspark.sql import SparkSession 

spark = SparkSession.builder\
        .appName('Agrupaciones')\
        .getOrCreate()

# Importar encuesta como dataframe 

ENIGH = spark.read.csv(
  "data/concentradohogar.csv",
  inferSchema=True,
  header=True
)

ENIGH.printSchema()

# Recodificación de variables 

from pyspark.sql.functions import * 

ENIGH = ENIGH.withColumn("sexo_jefe",
                         when(col("sexo_jefe") == 1, "Hombre")
                         .when(col("sexo_jefe") == 2, "Mujer")
                         .otherwise("No identificado"))

ENIGH.select("edad_jefe", "sexo_jefe", "educa_jefe").show()

ENIGH=ENIGH.withColumn("educa_jefe",
                 when(col("educa_jefe").isin([4,6]), "Básica completa")
                 .when(col("educa_jefe").isin([3,5]), "Básica incompleta")
                 .when(col("educa_jefe") == 8, "Media superior completa")
                 .when(col("educa_jefe") == 7, "Media superior incompleta")
                 .when(col("educa_jefe").isin([10,11]), "Superior completa")
                 .when(col("educa_jefe") == 9, "Superior incompleta")
                 .otherwise("Otro"))

ENIGH.select("edad_jefe", "sexo_jefe", "educa_jefe").show()
