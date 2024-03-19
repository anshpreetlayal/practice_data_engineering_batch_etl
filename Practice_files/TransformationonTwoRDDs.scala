import org.apache.spark.{SparkConf, SparkContext}

object SparkTransformationsExample {
  def main(args: Array[String]): Unit = {
    // Configure Spark
    val conf = new SparkConf().setAppName("SparkTransformationsExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Create an RDD
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5))

    // Transformer: map
    val squaredRDD = rdd.map(x => x * x)

    // Accessor: collect
    val squaredArray = squaredRDD.collect()

    // Print the squared elements
    squaredArray.foreach(println)

    // Stop Spark context
    sc.stop()
  }
}
