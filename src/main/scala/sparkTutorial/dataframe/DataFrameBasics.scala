package sparkTutorial.dataframe

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
    columnOperations(df, spark)

//    // 聚合操作：SELECT OrderId, COUNT(1) FROM table GROUP BY Quantity
//    df.groupBy("Quantity")  // 按 Quantity 分组，然后数每组中的条目数
//      .count()
//      .show()

    spark.stop()
  }

  def selectOperations(df: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._                 // enables $"CreateTime" syntax
    import org.apache.spark.sql.functions._  // enables col function

    // 查询某一列的数据：SELECT Quantity FROM table
    df.select("Quantity").show()

    // 查询某几列的数据：SELECT OrderId, Quantity*10 as Quantity2 FROM table
    df.select(         // select 多列
      col("OrderId"),
      $"CreateTime",   // 与 ordersDF.col 等效
      (col("Quantity") * 10).as("Quantity2")  // 变换该列数据，并重命名列名
    ).show()

    // 过滤：SELECT OrderId, Quantity FROM table WHERE Quantity > 3
    df.filter(col("Quantity") > 3)    // .filter() 和 .where() 等效，参数是 Columns（Columns 即表达式）
      .select("OrderId", "Quantity")   // select 可以直接选择多列（但不能像上面那样对其中某列进行变换）
      .show()
  }

  def columnOperations(df: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._  // enables $"CreateTime" syntax
    import org.apache.spark.sql.functions._  // enables col, lit function

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
    newDf.withColumn("Quantity2", col("Quantity").cast("long"))
      .printSchema()
  }

}
