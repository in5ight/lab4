import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TopSeller {
    def main(args: Array[String]) {
      if (args.length < 1) {
        System.err.println("Usage: <file>")
        System.exit(1)
      }

      val conf = new SparkConf().setAppName("Scala_TopCommodity")
      val sc = new SparkContext(conf)
      val file1 = sc.textFile(args(0))
      val file2 = sc.textFile(args(1))
      val file1_pair = file1.map(line => (line.split(",", -1)(0), line.split(",", -1)(3), line.split(",", -1)(6)))
                            .filter(line => line._3 != "0")
                            .map(line => (line._1, line._2))
      val file2_pair = file2.map(line => (line.split(",", -1)(0), line.split(",", -1)(1)))
      val file_join = file1_pair.join(file2_pair)

      val file_sorted = file_join.filter(line => line._2._2 == "1" || line._2._2 == "2" || line._2._2 == "3")
                                  .map(line => (line._2._1, 1))
                                  .reduceByKey(_ + _)
                                  .sortBy(-_._2)

      val result = file_sorted.zipWithIndex()
                              .map(line => (line._1, line._2 + 1))
                              .map(line => (line._2, line._1._1 + ": " + String.valueOf(line._1._2)))
                              .take(100)

      val rdd_res = sc.parallelize(result, 1)
      rdd_res.saveAsTextFile("output_sp2")
    
      sc.stop()
    }
}
