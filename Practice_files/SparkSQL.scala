import org.apache.spark.sql.SparkSession

// Create a Spark session
val spark = SparkSession.builder()
  .appName("Spark SQL Example")
  .config("spark.master", "local")
  .getOrCreate()

// Import implicits for DataFrame operations
import spark.implicits._

// Create a DataFrame from a CSV file
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("path/to/data.csv")

// Show the schema of the DataFrame
df.printSchema()

// Show the first few rows of the DataFrame
df.show()

// Register the DataFrame as a temporary view
df.createOrReplaceTempView("people")

// Perform SQL queries on the DataFrame
val result1 = spark.sql("SELECT * FROM people WHERE age > 30")
result1.show()

val result2 = spark.sql("SELECT gender, AVG(age) AS avg_age FROM people GROUP BY gender")
result2.show()

// Use DataFrame API for operations
val filteredDF = df.filter($"age" > 30)
val selectedDF = df.select("name", "age", "gender")
val groupedDF = df.groupBy("gender").agg(avg("age"), max("salary"))

// Write the result to a Parquet file
filteredDF.write.parquet("output/filtered_data.parquet")
selectedDF.write.json("output/selected_data.json")
groupedDF.write.csv("output/grouped_data.csv")

// Stop the Spark session
spark.stop()