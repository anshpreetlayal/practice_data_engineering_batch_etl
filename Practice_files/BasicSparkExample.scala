import org.apache.spark.{SparkConf, SparkContext}

object BasicSparkExample {
  def main(args: Array[String]): Unit = {
    // Configure Spark
    val conf = new SparkConf().setAppName("BasicSparkExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Create an RDD from a list of numbers
    val numbersRDD = sc.parallelize(List(1, 2, 3, 4, 5))

    // Perform transformations
    val squaredRDD = numbersRDD.map(x => x * x)
    val filteredRDD = squaredRDD.filter(x => x > 10)

    // Action: collect the results
    val result = filteredRDD.collect()

    // Print the result
    println("Filtered and squared numbers greater than 10:")
    result.foreach(println)

    // Save the result to a text file
    filteredRDD.saveAsTextFile("output")

    // Stop Spark context
    sc.stop()
  }
}

