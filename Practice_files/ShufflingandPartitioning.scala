import org.apache.spark.{SparkConf, SparkContext}

object ShufflingAndPartitioningExample {
  def main(args: Array[String]): Unit = {
    // Configure Spark
    val conf = new SparkConf().setAppName("ShufflingAndPartitioningExample").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Create an RDD with key-value pairs
    val data = Seq("A" -> 1, "B" -> 2, "C" -> 3, "D" -> 4, "E" -> 5)
    val rdd = sc.parallelize(data)

    // Partitioning using HashPartitioner
    val partitionedRDD = rdd.partitionBy(new org.apache.spark.HashPartitioner(3)) // Hash partitioning with 3 partitions

    // Shuffling examples
    val groupedRDD = rdd.groupByKey() // Shuffling for grouping by key
    val joinedRDD = rdd.join(partitionedRDD) // Shuffling for join operation

    // Actions to trigger computations and display results
    println("Original RDD:")
    rdd.collect().foreach(println)

    println("\nPartitioned RDD:")
    partitionedRDD.glom().collect().foreach(arr => println(arr.mkString(", ")))

    println("\nGrouped RDD:")
    groupedRDD.collect().foreach { case (key, values) =>
      println(s"$key -> ${values.mkString(", ")}")
    }

    println("\nJoined RDD:")
    joinedRDD.collect().foreach { case (key, (value1, value2)) =>
      println(s"$key -> ($value1, $value2)")
    }

    // Stop Spark context
    sc.stop()
  }
}