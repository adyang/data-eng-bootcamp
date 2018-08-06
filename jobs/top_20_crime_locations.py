from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from utils import get_absolute_path_of


spark = SparkSession.builder.appName("Top 20 Locations for Crimes").getOrCreate()

crimes = spark.read.csv("data/crimes/Crimes_-_One_year_prior_to_present.csv", header=True, inferSchema=True)

columnNames = crimes.columns
for col in columnNames:
    crimes = crimes.withColumnRenamed(col, col.strip())


top_locations = crimes.groupBy('LOCATION DESCRIPTION').count().sort(desc("count")).limit(20)
top_locations.coalesce(1).write.mode("overwrite").csv("data/top_locations", header=True)
