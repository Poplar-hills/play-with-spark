package sparkTutorial.dataframe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameBasics {

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

//    selectOperations(df, spark)
//    columnOperations(df, spark)
    rowOperations(df, spark)

//    // 聚合操作：SELECT OrderId, COUNT(1) FROM table GROUP BY Quantity
//    df.groupBy("Quantity")  // 按 Quantity 分组，然后数每组中的条目数
//      .count()
//      .show()

    spark.stop()
  }

  def selectOperations(df: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._                 // enables $"CreateTime" syntax

    // 查询某一列或多列的数据：SELECT OrderId, Quantity FROM table
    df.select("OrderId","Quantity").show()

    // 查询某几列的数据：SELECT OrderId, CreateTime, Quantity * 10 as Quantity2 FROM table
    df.select(         // select 多列
      col("OrderId"),
      $"CreateTime",   // 与 ordersDF.col 等效
      (col("Quantity") * 10).as("Quantity2")  // 变换该列数据，并重命名列名
    ).show()
  }

  def columnOperations(df: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._  // enables $ selection syntax

    // add a column with literal value
    df.withColumn("numberOne", lit(1))
      .show()

    // conditionally add a column
    val newDf = df.withColumn("Good", $"Published" === $"Censored")
    newDf.show()

    // renaming a column
    newDf.withColumnRenamed("Good", "Excellent")
      .show()

    // removing multi columns
    newDf.drop("Title", "CreateTime")
      .show()

    // casting a column’s type
    newDf.withColumn("Quantity2", col("Quantity").cast("int"))
      .printSchema()
  }

  def rowOperations(df: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._

    // 过滤：SELECT OrderId, Quantity FROM table WHERE Quantity > 3
    df.filter("Quantity > 20")  // .filter() 和 .where() 等效可互换
      .select("OrderId", "Quantity")
      .show()

    // 过滤：SELECT * FROM table WHERE Title CONTAINS 'Scalable'
    df.where("Quantity > 20")
      .where($"Title".contains("Scalable"))
      .show()

    // 对多个字段去重：SELECT DISTINCT(Quantity, Published)) FROM table
    df.select("Quantity", "Published")
      .distinct()
      .show()

    // 对单个字段去重并计数：SELECT COUNT(DISTINCT(Quantity))) FROM table
    val count = df.select("Quantity").distinct().count()
    println(count)

    // 按单个字段的降序进行排序（默认是升序）：SELECT OrderId, Quantity FROM table ORDER BY Quantity DESC LIMIT 5
    df.sort(desc("Quantity"))  // sort 和 orderBy 等效可互换
      .select("OrderId", "Quantity")
      .show(5)

    // 按两个字段进行排序：SELECT * FROM table ORDER BY OrderId ASC, Quantity DESC LIMIT 5
    df.orderBy(asc("OrderId"), desc("Quantity"))  // 但若同时排序多个，则一个字段使用了 asc 或 desc，则所有字段都要使用
      .show(5)
    // 更多排序函数：asc_nulls_first, desc_nulls_first, asc_nulls_last, desc_nulls_last
  }

}
