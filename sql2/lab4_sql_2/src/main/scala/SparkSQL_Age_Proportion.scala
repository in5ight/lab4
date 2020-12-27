import org.apache.spark.sql.SparkSession

object SparkSQL_Age_Proportion {
    def main(args: Array[String]) {
      if (args.length < 1) {
        System.err.println("Usage: <file>")
        System.exit(1)
      }
      val spark = SparkSession.builder().appName("SparkSQL_Age_Proportion").getOrCreate()

      import spark.implicits._

      val info = spark.read.format("csv").option("header", "true")
                    .load(args(1))

      val log = spark.read.format("csv").option("header", "true")
                    .load(args(0))

      log.createOrReplaceTempView("df1")
      var df1 = spark.sql("SELECT user_id,action_type FROM df1 WHERE action_type = 2")

      df1.createOrReplaceTempView("df1")
      df1 = spark.sql("SELECT DISTINCT user_id,action_type FROM df1")

      info.createOrReplaceTempView("df2")
      var df2 = spark.sql("SELECT user_id,age_range FROM df2 WHERE age_range = 1 or age_range = 2 or age_range = 3 or age_range = 4 or age_range = 5 or age_range = 6 or age_range = 7 or age_range = 8")

      df2.createOrReplaceTempView("df2")
      df2 = spark.sql("SELECT DISTINCT user_id,age_range FROM df2")

      df1.createOrReplaceTempView("df1")
      df2.createOrReplaceTempView("df2")
      var df_join = spark.sql("SELECT * FROM df1 INNER JOIN df2 on df1.user_id=df2.user_id")
      
      df_join.createOrReplaceTempView("df1")
      var res = spark.sql("SELECT df1.age_range, round(Count(*)/(SELECT count(df1.age_range) AS count FROM df1), 4) AS Proportion FROM df1 GROUP BY df1.age_range")
      res.show()

      spark.stop()
    }
}
