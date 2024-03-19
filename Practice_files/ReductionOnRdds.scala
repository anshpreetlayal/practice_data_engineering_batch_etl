import org.apache.spark.{SparkConf, SparkContext}

object SparkOperationsExample {
  def main(args: Array[String]): Unit = {
    // Configure Spark
    val conf = new SparkConf().setAppName("SparkOperationsExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Create an RDD from a list of numbers
    val numbersRDD = sc.parallelize(List(1, 2, 3, 4, 5))

    // Perform fold operation to calculate the sum of numbers
    val sumFold = numbersRDD.fold(0)((acc, ele) => acc + ele)

    // Perform aggregate operation to calculate sum and count of numbers
    val sumCountAggregate = numbersRDD.aggregate((0, 0))(
      (acc, ele) => (acc._1 + ele, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    // Print the results of fold and aggregate operations
    println("Sum of numbers using fold: " + sumFold)
    println("Sum and count of numbers using aggregate: " + sumCountAggregate)

    // Create a Pair RDD from a list of key-value pairs
    val pairRDD = sc.parallelize(List("key1" -> 1, "key2" -> 2, "key1" -> 3))

    // Perform transformations and actions on Pair RDDs
    val groupedRDD = pairRDD.groupByKey()
    val sumByKeyRDD = pairRDD.reduceByKey(_ + _)
    val sortedRDD = pairRDD.sortByKey()
    val joinedRDD = pairRDD.join(sc.parallelize(List("key1" -> "value1", "key2" -> "value2")))
    val avgByKeyRDD = pairRDD.aggregateByKey((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).mapValues { case (sum, count) => sum.toDouble / count }

  // Print the results of transformations and actions on Pair RDDs
    println("Grouped by key:")
    groupedRDD.collect().foreach(println)
    println("Sum by key:")
    sumByKeyRDD.collect().foreach(println)
    println("Sorted by key:")
    sortedRDD.collect().foreach(println)
    println("Joined with another RDD:")
    joinedRDD.collect().foreach(println)
    println("Average by key:")
    avgByKeyRDD.collect().foreach(println)

    // Stop Spark
  }}