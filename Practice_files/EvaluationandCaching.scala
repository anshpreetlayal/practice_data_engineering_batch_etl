import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object SparkExamples {
  def main(args: Array[String]): Unit = {
    // Spark Configuration
    val conf = new SparkConf().setAppName("SparkExamples").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Example RDD
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5))

    // Evaluation Example: Reduce action to sum the RDD
    val sum = rdd.reduce(_ + _)
    println(s"Sum of RDD elements: $sum")

    // Iteration Example: Iterative training of a model
    var model = 0
    val numIterations = 3
    val data = sc.parallelize(Seq(1, 2, 3, 4, 5))

    for (i <- 1 to numIterations) {
      model = data.map(point => updateModel(model, point)).reduce(combineModels)
    }

     println(s"Final model after $numIterations iterations: $model")

    // Caching Example: Cache the RDD in memory
    rdd.cache()

    // Persistence Example: Persist the RDD in memory and on disk
    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Action to trigger evaluation and reuse cached/persisted RDD
    val count = rdd.count()
    println(s"Count of RDD elements: $count")

    // Stop the Spark Context
    sc.stop()
  }

  // Example functions for iteration
  def updateModel(model: Int, point: Int): Int = model + point
  def combineModels(model1: Int, model2: Int): Int = model1 + model2
}
  