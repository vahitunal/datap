from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Merhaba Spark").getOrCreate()

data = [("Ali", 30), ("Ayşe", 25)]
df = spark.createDataFrame(data, ["Ad", "Yaş"])
df.show()
