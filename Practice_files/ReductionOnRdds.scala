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

  }}