import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TopCommodity {
    def main(args: Array[String]) {
      if (args.length < 1) {
        System.err.println("Usage: <file>")
        System.exit(1)
      }

      val conf = new SparkConf().setAppName("Scala_TopCommodity")
      val sc = new SparkContext(conf)
      val file = sc.textFile(args(0))
      val file_sorted = file.map(line => line.split(",", -1))
                            .filter(line => line(6) != "0" && line(6) != "")
                            .map(line => (line(1), 1))
                            .reduceByKey(_ + _)
                            .sortBy(-_._2)
      val result = file_sorted.zipWithIndex()
                              .map(line => (line._1, line._2 + 1))
                              .map(line => (line._2, line._1._1 + ": " + String.valueOf(line._1._2)))
                              .take(100)

      val rdd_res = sc.parallelize(result, 1)
      rdd_res.saveAsTextFile("output_sp1")
    
      sc.stop()
    }
}
