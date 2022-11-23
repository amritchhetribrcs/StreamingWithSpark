import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkStreaming1 {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    // Schema definition
    val schemaObj1 = StructType(Array(
      StructField("id",StringType,true),
      StructField("name",StringType,true)
    ))

    val df = spark
      .readStream
      .format("csv")
      .schema(schemaObj1)
      .load("file:///C:/CDPDataLakeProjects/inputx/data")


    df.writeStream.format("console")
      .option("checkpointLocation",
        "file:///C:/CDPDataLakeProjects/data1/outcome")
      .start()
      .awaitTermination()

  }
}
