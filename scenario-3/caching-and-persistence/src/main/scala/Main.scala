import org.apache.spark.sql.{SparkSession, functions => F}

object Main extends App {

  val spark = SparkSession.builder
    .appName("User Log Analysis")
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.types._
  val schema = StructType(Array(
    StructField("user_id", StringType, true),
    StructField("timestamp", StringType, true),
    StructField("action", StringType, true),
    StructField("item_id", StringType, true)
  ))

  val logFile = "/Users/abhishekanand/Documents/scala-project-2/scenario-3/caching-and-persistence/src/main/scala/user_logs.csv"
  val logsDF = spark.read
    .schema(schema)
    .option("header", "true")
    .csv(logFile)

  println("Without Caching:")

  time {
    val actionsPerUserDF = logsDF.groupBy("user_id").count()
    actionsPerUserDF.show()

    val uniqueItemsPerUserDF = logsDF.groupBy("user_id").agg(F.countDistinct("item_id").alias("unique_items"))
    uniqueItemsPerUserDF.show()

    val mostRecentActionPerUserDF = logsDF.groupBy("user_id").agg(F.max("timestamp").alias("most_recent_action"))
    mostRecentActionPerUserDF.show()
  }

  logsDF.cache()

  time {
    val actionsPerUserDFCache = logsDF.groupBy("user_id").count()
    actionsPerUserDFCache.show()

    val uniqueItemsPerUserDFCache = logsDF.groupBy("user_id").agg(F.countDistinct("item_id").alias("unique_items"))
    uniqueItemsPerUserDFCache.show()

    val mostRecentActionPerUserDFCache = logsDF.groupBy("user_id").agg(F.max("timestamp").alias("most_recent_action"))
    mostRecentActionPerUserDFCache.show()
  }

  logsDF.unpersist()

  def time[R](block: => R): R = {
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    println(s"Elapsed time: ${(end - start) / 1e9} seconds")
    result
  }

}


