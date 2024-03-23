package project1

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.from_json

case class Data(peer_id: String, id_1: String, id_2: String, year: Int)

object Main {

  def parseJson(jsonString: String, spark: SparkSession): DataFrame = {
    val regex = """\((?=(?:[^']*'[^']*')*[^']*$)"""
    val columnNames = Seq("peer_id", "id_1", "id_2", "year")

    // parse jsonlike string to dataframe
    val dataList = jsonString.stripMargin.replaceAll(regex, "").replaceAll("[\\[\\]()]", "").replaceAll("'", "").split(",").map(_.trim).grouped(4).toList.map {
      case Array(a, b, c, d) => Data(a, b, c, d.toInt)
    }

    spark.createDataFrame(dataList)
  }

  def filterAndSelect(df: DataFrame): DataFrame = {
    val columnsResult1 = Seq("peer_id_1", "year_1")
    df.filter(expr("peer_id LIKE CONCAT('%', id_2, '%')"))
      .select("peer_id", "year")
      .toDF(columnsResult1: _*)
  }

  def joinAndFilter(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.join(df2, df1("peer_id") === df2("peer_id_1"), "inner")
      .filter(expr("year <= year_1"))
      .groupBy("peer_id", "year")
      .agg(count("*").alias("count"))
  }

  def calculateCumulativeSum(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("peer_id").orderBy(desc("year"))
    df.withColumn("cumulative_sum", sum("count").over(windowSpec))
  }

  def filterBySize(df: DataFrame, sizeNumber: Int): DataFrame = {
    df.filter(col("cumulative_sum") < sizeNumber)
      .select("peer_id", "year")
  }

  def getTopRows(df: DataFrame): DataFrame = {
    val windowSpec1 = Window.partitionBy(col("peer_id")).orderBy(col("year").desc)
    df.withColumn("row_num", row_number().over(windowSpec1))
      .filter(col("row_num") === 1)
      .select("peer_id", "year")
  }

  def formatOutput(df: DataFrame): String = {
    val withQuotedStringColumn = df.withColumn("peer_id", concat(lit("'"), col("peer_id"), lit("'")))
    val concatenatedDF = withQuotedStringColumn.select(concat_ws(",", withQuotedStringColumn.columns.map(col): _*))
    val content = concatenatedDF.rdd.map(row => s"\t(${row.mkString(", ")})").collect().mkString("\n")
    ("[\n" +: content :+ "\n]").mkString
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MySparkApp")
      .master("local[2]")
      .getOrCreate()

    val jsonString1 = """[
                        |    ('AE686(AE)', '7', 'AE686', 2022),
                        |    ('AE686(AE)', '8', 'BH2740', 2021),
                        |    ('AE686(AE)', '9', 'EG999', 2021),
                        |    ('AE686(AE)', '10', 'AE0908', 2023),
                        |    ('AE686(AE)', '11', 'QA402', 2022),
                        |    ('AE686(AE)', '12', 'OA691', 2022),
                        |    ('AE686(AE)', '12', 'OB691', 2022),
                        |    ('AE686(AE)', '12', 'OC691', 2019),
                        |    ('AE686(AE)', '12', 'OD691', 2017)
                        |]""".stripMargin
    val sizeNumber = 7
    val df = parseJson(jsonString1, spark)

    val result1 = filterAndSelect(df)
    val result2 = joinAndFilter(df, result1)
    val cumulativeSumDF = calculateCumulativeSum(result2)
    val lessSizeDF = filterBySize(cumulativeSumDF, sizeNumber)
    val moreSizeDF = cumulativeSumDF.filter(col("cumulative_sum") >= sizeNumber)
    val markedMoreSizeDF = getTopRows(moreSizeDF)

    val result3 = lessSizeDF.union(markedMoreSizeDF).orderBy(asc("peer_id"), desc("year"))

    val formattedOutput = formatOutput(result3)
    println(formattedOutput)
  }

}
