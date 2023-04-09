import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition.first
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object Slides {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark SQL Seminar")
      .config("spark.master", "local")
      .getOrCreate()

    rddFunc(spark)
  }

  def rddFunc(spark: SparkSession): Unit = {
    val citiesRDD: RDD[String] = spark.sparkContext
      .textFile("W:\\HTWG_Master\\Seminar\\data\\cities.csv")
      .map(_.split(","))
      .map(attributes => City(attributes(0).trim.toInt, attributes(1), attributes(2), attributes(3), attributes(4), attributes(5), attributes(6).trim.toDouble, attributes(7).trim.toDouble, attributes(8).trim.toDouble, attributes(9).trim.toDouble, attributes(10).trim.toDouble, attributes(11), attributes(12), attributes(13), attributes(14), "false".equalsIgnoreCase(attributes(15))))
      .map(city => city.city)

    citiesRDD.foreach(println)
  }

  def dsFunc(spark: SparkSession): Unit = {
    implicit val cityEncoder: Encoder[City] = Encoders.product[City]
    val citiesDS = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("W:\\HTWG_Master\\Seminar\\data\\cities.csv")
      .as[City]

    citiesDS.select("city").show()
  }

  def dfFunc(spark: SparkSession): Unit = {
    val citiesDF = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("W:\\HTWG_Master\\Seminar\\data\\cities.csv")
    citiesDF.select("city").show()
  }



}
