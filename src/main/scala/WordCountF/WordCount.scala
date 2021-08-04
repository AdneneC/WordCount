package WordCountF

import org.apache.spark.sql.{Dataset, SparkSession}


object WordCount {
  def wordCount(input: Dataset[String] , delimiter: String): Dataset[(String,Long)] = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Sample App")
      .getOrCreate()

    import spark.implicits._
    val words = input.flatMap(x => x.split(delimiter))
    val counts = words.groupByKey(word => word).count()

    counts

  }
}
