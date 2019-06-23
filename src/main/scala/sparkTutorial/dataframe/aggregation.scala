package sparkTutorial.dataframe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object aggregation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApp")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .option("multiline", value = true)
      .json("file:///Users/myjiang/Documents/Poplar_hills/Dev/spark/play-with-spark/in/orders.json")

    df.printSchema()  // 输出 DataFrame 的 schema 信息
    df.show()         // 查询所有列的数据（默认前20条）

    aggregationFunctions(df, spark)

    spark.stop()
  }

  def aggregationFunctions(df: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._

    val count = df.select("Quantity").distinct().count()  // 返回值的方式
    println(count)

    df.select(sumDistinct("Quantity")).show()  // 返回列的方式

    df.select(min("Quantity"), max("Quantity")).show()

    df.select(sum("Quantity")).show()

    df.select(
      countDistinct("Quantity").alias("Distinct_Quantity_Count"),
      avg("Quantity").alias("Average_Quantity"),
      mean("Quantity").alias("Mean_Quantity_1"),
      expr("mean(Quantity)").alias("Mean_Quantity_2")
    ).show()

    // 除了对单列进行统计的函数外，还有对多列进行统计的函数 - Covariance and Correlation
    df.select(
      corr("Quantity", "Price"),  // Correlation
      covar_samp("Quantity", "Price"),   // Sample Covariance
      covar_pop("Quantity", "Price")     // Population Covariance
    ).show()
  }
}
