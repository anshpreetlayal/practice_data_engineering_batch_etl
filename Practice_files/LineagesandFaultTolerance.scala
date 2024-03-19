import org.apache.spark.{SparkConf, SparkContext}

object SparkLineageFaultTolerance {
  def main(args: Array[String]): Unit = {
    // Spark Configuration
    val conf = new SparkConf().setAppName("SparkLineageFaultTolerance").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Lineage Example
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2 = rdd1.map(_ * 2)
    val rdd3 = rdd2.filter(_ % 4 == 0)

    println("Lineage of rdd3:")
    rdd3.toDebugString.split("\n").foreach(println)

    // Fault Tolerance Example (Checkpointing)
    sc.setCheckpointDir("/path/to/checkpoint/dir")
    rdd3.checkpoint()

    // Dependencies Example
    val pairs1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
    val pairs2 = sc.parallelize(Seq(("b", 4), ("c", 5), ("d", 6)))

    val joined = pairs1.join(pairs2)

    println("Dependencies of joined RDD:")
    joined.toDebugString.split("\n").foreach(println)

    // Stop the Spark Context
    sc.stop()
  }
}
