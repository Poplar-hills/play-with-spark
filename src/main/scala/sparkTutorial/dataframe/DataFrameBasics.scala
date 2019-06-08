package sparkTutorial.dataframe

import org.apache.spark.sql.SparkSession

object DataFrameBasics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApp")
      .master("local[2]")
      .getOrCreate()

    val ordersDF = spark.read
      .option("multiline", value = true)
      .json("file:///Users/myjiang/Documents/Poplar_hills/Dev/spark/play-with-spark/in/orders.json")

    // 输出 DataFrame 的 schema 信息
    ordersDF.printSchema()

    // 查询所有列的数据（默认前20条）
    ordersDF.show()

    // 查询某一列的数据：SELECT Quantity FROM table
    ordersDF.select("Quantity").show()

    import spark.implicits._  // enable "$"
    // 查询某几列的数据：SELECT OrderId, Quantity*10 as Quantity2 FROM table
    ordersDF.select(
      ordersDF.col("OrderId"),
      $"CreateTime",  // 与 ordersDF.col 等效
      (ordersDF.col("Quantity") * 10).as("Quantity2")  // 变换该列数据，并重命名列名
    ).show()

    // 过滤：SELECT OrderId, Quantity FROM table WHERE Quantity > 3
    ordersDF
      .filter(ordersDF.col("Quantity") > 3)  // .filter() 和 .where() 等效
      .select("OrderId", "Quantity")  // 上面查询某几列的简化写法（但不能变换）
      .show()

    // 聚合：SELECT OrderId, COUNT(1) FROM table GROUP BY Quantity
    ordersDF
      .groupBy("Quantity")
      .count()
      .show()

    spark.stop()
  }
}
