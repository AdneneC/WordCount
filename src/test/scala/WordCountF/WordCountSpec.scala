package WordCountF

import WordCountF.WordCount.wordCount
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class WordCountSpec extends AnyFlatSpec with Matchers with GivenWhenThen{
  implicit val  spark : SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Sample App")
    .getOrCreate()

  import spark.implicits._
  val text =  Seq("gehen so schnell wie möglich nach frankreich").toDF()

  "WordCount" should "Count the occurence of each word" in {

    Given ("The text given")
    val splitter = " "
    val input = text.as[String]
    When ("WordCount is invoked")
    val Frequency = wordCount(input,splitter)

    Then ("the occurence of each word")
    val expectedResult = Seq(("gehen",1 : Long) ,("so",1 : Long), ("schnell",1 : Long), ("wie",1 : Long)
      ,("möglich",1 : Long), ("nach",1 : Long),("frankreich",1 : Long)).toDS()

    Frequency.collect() should contain theSameElementsAs expectedResult.collect()
  }
}