package sparkTutorial.dataframe

import org.apache.spark.sql.functions.{count, _}
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

    df.printSchema() // 输出 DataFrame 的 schema 信息
    df.show() // 查询所有列的数据（默认前20条）

//    aggregationFunctions(df)
    grouping(df)

    spark.stop()
  }

  def aggregationFunctions(df: DataFrame): Unit = {
    val count = df.select("Quantity").distinct().count() // 返回值的方式
    println(count)

    df.select(sumDistinct("Quantity")).show() // 返回列的方式

    df.select(min("Quantity"), max("Quantity")).show()

    df.select(sum("Quantity")).show()

    df.select(  // 另一种方式是将聚合函数放在 agg 方法中，而不是放在 select 方法中，效果相同
      countDistinct("Quantity").alias("Distinct_Quantity_Count"),
      avg("Quantity").alias("Average_Quantity"),
      mean("Quantity").alias("Mean_Quantity_1"),
      expr("mean(Quantity)").alias("Mean_Quantity_2")
    ).show()

    // 除了对单列进行统计的函数外，还有对多列进行统计的函数 - Covariance and Correlation
    df.agg(
      corr("Quantity", "Price"), // Correlation
      covar_samp("Quantity", "Price"), // Sample Covariance
      covar_pop("Quantity", "Price") // Population Covariance
    ).show()
  }

  def grouping(df: DataFrame): Unit = {
    // 分组：SELECT COUNT(*) FROM table GROUP BY CreateTime
    df.groupBy("CreateTime").count().show()

    // 根据多字段分组：SELECT COUNT(*) FROM table GROUP BY CreateTime, Published
    df.groupBy("CreateTime", "Published").count().show()

    // 以下三种写法作用相同
    df.groupBy("Quantity").count().show()
    df.groupBy("Quantity").count.show  // 无参方法调用可以不写括号
    df.groupBy("Quantity").agg(count("Quantity")).show()  // 这种更常见，把聚合函数放在 agg 方法中，而非调用聚合方法

    // 先按 Quantity 分组，再对每组按 Price 求均值（group with map 写法）
    df.groupBy("Quantity").agg("Price" -> "avg").show()

    // 先按 Quantity 分组，再对每组按 Price 求均值、对每组按 Price 求标准差（group with map 写法）
    df.groupBy("Quantity").agg(
      "Price" -> "avg",
      "Price" -> "stddev_pop"
    ).show()
  }
}