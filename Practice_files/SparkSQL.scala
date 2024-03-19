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

