import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.BeforeAndAfterAll
import project1.Main._

case class Data(peer_id: String, id_1: String, id_2: String, year: Int)
case class Result(peer_id: String, year: Int, count: Long)

class MySparkFunctionsTest extends AnyFunSuite with Matchers with DataFrameSuiteBase with BeforeAndAfterAll {

  override lazy val spark: SparkSession = {

    SparkSession.builder
      .master("local[2]")
      .appName("MySparkFunctionsTest")
      .getOrCreate()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  test("parseJson function should convert custom formatted string to DataFrame") {
    val spark = SparkSession.builder().appName("ParseJsonTest").master("local[*]").getOrCreate()

    val jsonString = "[('ABC17969(AB)', '1', 'ABC17969', 2022)]"
    val expectedSchema = StructType(Seq(
      StructField("peer_id", StringType, nullable = true),
      StructField("id_1", StringType, nullable = true),
      StructField("id_2", StringType, nullable = true),
      StructField("year", IntegerType, nullable = false)
    ))

    val df = parseJson(jsonString, spark)
    df.schema shouldBe expectedSchema
    df.collect() shouldBe Array(Row("ABC17969AB", "1", "ABC17969", 2022))
  }

  test("filterAndSelect function should filter and select columns as expected") {
    val inputDfData = Seq(
      ("ABC17969(AB)", "1", "ABC17969", 2022),
      ("DEF456(BC)", "2", "DEF456", 2021),
      ("GHI789(DE)", "3", "GHI", 2020),
      ("ABC17969(AB)", "4", "XXX", 2019)
    )
    val inputDfColumns = Seq("peer_id", "id_1", "id_2", "year")
    val inputDf = spark.createDataFrame(inputDfData).toDF(inputDfColumns: _*)

    val expectedDfData = Seq(
      ("ABC17969(AB)", 2022),
      ("DEF456(BC)", 2021)
    )
    val expectedDfColumns = Seq("peer_id_1", "year_1")
    val expectedDf = spark.createDataFrame(expectedDfData).toDF(expectedDfColumns: _*)

    val resultDf = filterAndSelect(inputDf)
    resultDf.schema.fieldNames shouldBe expectedDf.schema.fieldNames
  }

  test("calculateCumulativeSum function should calculate cumulative sum over partitions") {
    val inputDfData = Seq(
      ("ABC17969(AB)", 2022, 1),
      ("ABC17969(AB)", 2021, 2),
      ("DEF456(BC)", 2021, 3),
      ("DEF456(BC)", 2020, 4),
      ("GHI789(DE)", 2020, 5)
    )
    val inputDfColumns = Seq("peer_id", "year", "count")
    val inputDf = spark.createDataFrame(inputDfData).toDF(inputDfColumns: _*)

    val expectedDfData = Seq(
      ("ABC17969(AB)", 2022, 1, 1),
      ("ABC17969(AB)", 2021, 2, 3),
      ("DEF456(BC)", 2021, 3, 3),
      ("DEF456(BC)", 2020, 4, 7),
      ("GHI789(DE)", 2020, 5, 5)
    )
    val expectedDfColumns = Seq("peer_id", "year", "count", "cumulative_sum")
    val expectedDf = spark.createDataFrame(expectedDfData).toDF(expectedDfColumns: _*)

    val resultDf = calculateCumulativeSum(inputDf)
    resultDf.schema.fieldNames should contain theSameElementsInOrderAs (inputDfColumns ++ Seq("cumulative_sum"))
    resultDf.orderBy("peer_id", "year").collect() should contain theSameElementsInOrderAs expectedDf.orderBy("peer_id", "year").collect()
  }

  test("filterBySize function should filter rows based on cumulative_sum less than given sizeNumber") {
    val inputDfData = Seq(
      ("ABC17969(AB)", 2022, 1),
      ("ABC17969(AB)", 2021, 3),
      ("DEF456(BC)", 2021, 5),
      ("DEF456(BC)", 2020, 7),
      ("GHI789(DE)", 2020, 10)
    )
    val inputDfColumns = Seq("peer_id", "year", "cumulative_sum")
    val inputDf = spark.createDataFrame(inputDfData).toDF(inputDfColumns: _*)

    val sizeNumber = 6

    val expectedDfData = Seq(
      ("ABC17969(AB)", 2022),
      ("ABC17969(AB)", 2021)
    )
    val expectedDfColumns = Seq("peer_id", "year")
    val expectedDf = spark.createDataFrame(expectedDfData).toDF(expectedDfColumns: _*)

    val resultDf = filterBySize(inputDf, sizeNumber)
    resultDf.schema.fieldNames shouldBe expectedDf.schema.fieldNames
  }

  test("getTopRows function should retrieve the top rows within each partition by year desc") {
    val inputDfData = Seq(
      ("ABC17969(AB)", 2022),
      ("ABC17969(AB)", 2021),
      ("DEF456(BC)", 2021),
      ("DEF456(BC)", 2020),
      ("GHI789(DE)", 2020)
    )
    val inputDfColumns = Seq("peer_id", "year")
    val inputDf = spark.createDataFrame(inputDfData).toDF(inputDfColumns: _*)

    val expectedDfData = Seq(
      ("ABC17969(AB)", 2022),
      ("DEF456(BC)", 2021),
      ("GHI789(DE)", 2020)
    )
    val expectedDf = spark.createDataFrame(expectedDfData).toDF(inputDfColumns: _*)

    val resultDf = getTopRows(inputDf)
    resultDf.schema.fieldNames shouldBe inputDfColumns
    resultDf.collect() should contain theSameElementsAs expectedDf.collect()
  }

}
