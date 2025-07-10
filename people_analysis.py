from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark oturumu başlat
spark = SparkSession.builder.appName("PeopleAnalysis").getOrCreate()

# Veriyi S3'ten oku
df = spark.read.option("header", True).csv("s3a://spark/people.csv")

# Yaşı 30'dan büyük olanları filtrele
older_than_30 = df.filter(col("age").cast("int") > 30)

# Şehre göre grupla ve say
city_counts = df.groupBy("city").count()

# Sonuçları yazdır
print("30 yaşından büyük kişiler:")
older_than_30.show()

print("Şehre göre kişi sayısı:")
city_counts.show()

# S3'e yazmak istersen:
city_counts.write.mode("overwrite").csv("s3a://spark/output/city_counts")
