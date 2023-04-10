import org.apache.spark.sql.SparkSession

import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Spark SQL Seminar")
      .config("spark.master", "local")
      .getOrCreate()

//    genericLoad(spark)
//    jsonExample(spark)
    jsonSQLExample(spark)

  }

  def genericLoad(spark: SparkSession): Unit = {
    //TODO csv Example
    val peopleDFCsv = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("W:\\HTWG_Master\\Seminar\\data\\people.csv")

    peopleDFCsv.show()
  }

  def jsonExample(spark: SparkSession): Unit = {
    //TODO json Example
    val df = spark.read.json("W:\\HTWG_Master\\Seminar\\data\\people.json")
    df.show()
  }

  def jdbcExample(spark: SparkSession): Unit = {
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
  }

  def jsonSQLExample(spark: SparkSession): Unit = {
    val df = spark.read.json("W:\\HTWG_Master\\Seminar\\data\\people.json")
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT name, age FROM people")
    sqlDF.show()
  }
}
