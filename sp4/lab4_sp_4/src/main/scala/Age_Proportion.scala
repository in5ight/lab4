import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Age_Proportion {
    def main(args: Array[String]) {
      if (args.length < 1) {
        System.err.println("Usage: <file>")
        System.exit(1)
      }

      val conf = new SparkConf().setAppName("Age_Proportion")
      val sc = new SparkContext(conf)
      val file1 = sc.textFile(args(0))
                    .map(line => (line.split(",", -1)(0), line.split(",", -1)(6)))
                    .filter(line => line._2 == "2")
                    .distinct()

      val file2 = sc.textFile(args(1))
                    .map(line => (line.split(",", -1)(0), line.split(",", -1)(1)))
                    .filter(line => line._2 != "0" && line._2 != "")
                    .distinct()

      val file_join = file1.join(file2)
                            .flatMap(line => line._2._2)
                            .countByValue()
      
      var sum = 0.0
      for (value <- file_join.values) {
        sum = sum + value
      }

      println("**********输出结果**********")
      print("<18岁占总人数比例：")
      println((file_join('1') / sum).formatted("%.4f"))
      print("[18,24]占总人数比例：")
      println((file_join('2') / sum).formatted("%.4f"))
      print("[25,29]占总人数比例：")
      println((file_join('3') / sum).formatted("%.4f"))
      print("[30,34]占总人数比例：")
      println((file_join('4') / sum).formatted("%.4f"))
      print("[35,39]占总人数比例：")
      println((file_join('5') / sum).formatted("%.4f"))
      print("[40,49]占总人数比例：")
      println((file_join('6') / sum).formatted("%.4f"))
      print(">=50岁占总人数比例：")
      println(((file_join('7') + file_join('8')) / sum).formatted("%.4f"))
      println("**********输出结果**********")

      sc.stop()
    }
}
