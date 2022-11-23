import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkStreamingFixed {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("Streaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(
      List(
        StructField("PrdCode", IntegerType, true),
        StructField("PrdType", StringType, true),
        StructField("PrdCat",  StringType, true)
      )
    )

    val df = spark.readStream
      .schema(schema)
      .json("C:/CDP-3001-Certification/Datasets/Streaming")

    df.printSchema()

    val groupDF = df.select("PrdCode")
      .groupBy("PrdCode").count()
    groupDF.printSchema()

    groupDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }
}
