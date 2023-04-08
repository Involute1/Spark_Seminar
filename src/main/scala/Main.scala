import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Spark SQL Seminar")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read.json("W:\\HTWG_Master\\Seminar\\data\\cities.json")
    df.show()

    df.select("City").show()

    df.createOrReplaceTempView("cities")

    val sqlDF = spark.sql("SELECT City, Country FROM cities")
    sqlDF.show()


    //TODO json Example

    //TODO csv Example

    //TODO SQL Example

    //TODO JSON as SQL Example

  }
}
