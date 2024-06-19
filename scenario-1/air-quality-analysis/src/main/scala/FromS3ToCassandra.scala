object FromS3ToCassandra {
  def main(args: Array[String]): Unit = {
    println("Hello world!")

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder
      .appName("CSV Schema Analysis")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.cassandra.connection.host","cassandra.ap-south-1.amazonaws.com")
      .config("spark.cassandra.connection.port", "9142")
      .config("spark.cassandra.connection.ssl.enabled", "true")
      .config("spark.cassandra.auth.username", "xxx")
      .config("spark.cassandra.auth.password", "xxx")
      .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
      .config("spark.cassandra.connection.ssl.trustStore.path", "/Users/abhishekanand/cassandra_truststore.jks")
      .config("spark.cassandra.connection.ssl.trustStore.password", "xxx")
      .getOrCreate()

    val hadoopConfig = spark.sparkContext.hadoopConfiguration

    hadoopConfig.set("fs.s3a.access.key", "xxx")
    hadoopConfig.set("fs.s3a.secret.key", "xxx")
    hadoopConfig.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val df = spark.read
//      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("s3a://abhishek-payoda-final-project/zaragoza_data.csv")
//      .csv("/Users/abhishekanand/Documents/scala-project-2/scenario-1/zaragoza_data.csv") // local path

    df.show()
    df.printSchema()

    val schema = df.describe().show()
    println("Printing the schema")
    println(schema);

    println("Number of records ")
//    val count = df.count()
//    println(count)
    println("Writing the data to Amazon keyspaces")

    val renamedDF = df.toDF("date", "no2", "o3", "pm10", "latitude", "longitude", "station_name",
      "wind_speed_u", "wind_speed_v", "dewpoint_temp", "temp", "vegitation_high", "vegitation_low",
      "soil_temp", "total_precipitation", "relative_humidity")

    renamedDF.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "abhishek_keyspace")
      .option("table", "air_quality")
//      .option("spark.cassandra.output.batch.size.rows", "1000")
      .mode("append")
      .save()

  }
}