import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
object WordCountExample {
  def main(args : Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.INFO)
    val sc = new SparkContext("local[*]" , "SparkExample")
    println("Spark Context created ..."+ sc)
    val data = sc.textFile("C:\\Data.txt")
    val wordCount = data
      .flatMap(row => row.split(","))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    wordCount.foreach(println)
  }
}