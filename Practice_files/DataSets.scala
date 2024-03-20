import org.apache.spark.sql.{SparkSession, Dataset}

// Define a case class representing the schema
case class Person(name: String, age: Int)

object DatasetExample {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("DatasetExample")
      .getOrCreate()

    // Import implicit encoders for standard library classes
    import spark.implicits._

    // Read CSV file into a Dataset of Person objects
    val peopleDS: Dataset[Person] = spark.read
      .option("header", "true")  
      .csv("peoples.csv")
      .as[Person]  // Convert DataFrame to Dataset using the Person case class

    // Perform operations on the Dataset
    val filteredPeople = peopleDS.filter(_.age > 30)

    // Display the filtered Dataset
    filteredPeople.show()

    // Stop the Spark session
    spark.stop()
  }
}
