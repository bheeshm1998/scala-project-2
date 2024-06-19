import org.apache.spark.sql.{DataFrame, SparkSession}

object FromCassandraToS3{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Read from Keyspaces and Write to S3 in Parquet")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.cassandra.connection.host", "cassandra.ap-south-1.amazonaws.com")
      .config("spark.cassandra.connection.port", "9142")
      .config("spark.cassandra.connection.ssl.enabled", "true")
      .config("spark.cassandra.auth.username", "xxx")
      .config("spark.cassandra.auth.password", "xxx")
      .config("spark.cassandra.connection.ssl.trustStore.path", "/Users/abhishekanand/cassandra_truststore.jks")
      .config("spark.cassandra.connection.ssl.trustStore.password", "xxx")
      .getOrCreate()

    val hadoopConfig = spark.sparkContext.hadoopConfiguration

    hadoopConfig.set("fs.s3a.access.key", "xxx")
    hadoopConfig.set("fs.s3a.secret.key", "xxx")
    hadoopConfig.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val df = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "air_quality", "keyspace" -> "abhishek_keyspace"))
      .load()

    println("READ from Cassandra Successful")
    df.show()
    df.describe().show()

    val avgAirQualityByStation = df.groupBy("station_name").avg("no2", "o3", "pm10")
    println("avgAirQualityByStation = ");
    avgAirQualityByStation.show();

    val avgAirQualityByLatitudeAndLongitude = df.groupBy("latitude", "longitude").avg("no2", "o3", "pm10")

    println("avgAirQualityByLatitudeAndLongitude")
    avgAirQualityByLatitudeAndLongitude.show()

    val avgAirQualityByStationAndVegetationHigh = df.groupBy("station_name", "vegitation_high").avg("no2", "o3", "pm10")
    println("avgAirQualityByStationAndVegetationHigh")
    avgAirQualityByStationAndVegetationHigh.show()


    //    writeDateToParquet(df);
  }

  def writeDateToParquet(dataframe: DataFrame) = {
    dataframe.write
      .mode("overwrite")
      .parquet("s3a://abhishek-payoda-final-project/output2.parquet")
  }
}
