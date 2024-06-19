import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object NormalizeBooksData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Normalize Books Data")
      .master("local[*]")
      .getOrCreate()

    val jdbcUrl = "jdbc:mysql://34.100.169.199:3306/books"
    val dbTable = "book"
    val dbUser = "root"
    val dbPassword = "xxx"

    val booksDF = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbTable)
      .option("user", dbUser)
      .option("password", dbPassword)
      .load()

    booksDF.show()
    booksDF.printSchema()

    val booksNormalizedDF = booksDF.select(
      col("book_id"),
      col("title"),
      col("genre"),
      col("publish_date")
    ).distinct()

    val authorsNormalizedDF = booksDF.select(
      col("author_id"),
      col("author_name")
    ).distinct()

    val salesNormalizedDF = booksDF.select(
      col("sale_id"),
      col("book_id"),
      col("sale_date"),
      col("quantity"),
      col("price")
    ).distinct()

    booksNormalizedDF.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "normalized_books")
      .option("user", dbUser)
      .option("password", dbPassword)
      .mode("overwrite")
      .save()

    authorsNormalizedDF.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "normalized_authors")
      .option("user", dbUser)
      .option("password", dbPassword)
      .mode("overwrite")
      .save()

    salesNormalizedDF.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "normalized_sales")
      .option("user", dbUser)
      .option("password", dbPassword)
      .mode("overwrite")
      .save()

    val salesByBookTitleDF = salesNormalizedDF
      .join(booksNormalizedDF, "book_id")
      .groupBy("title")
      .agg(
        sum("quantity").as("total_quantity"),
        sum(col("quantity") * col("price")).as("total_sales")
      )

    val salesByAuthorNameDF = booksDF
      .groupBy("author_name")
      .agg(
        sum("quantity").as("total_quantity"),
        sum(col("quantity") * col("price")).as("total_sales")
      )

    val salesByMonthDF = salesNormalizedDF
      .withColumn("sale_month", date_format(col("sale_date"), "yyyy-MM"))
      .groupBy("sale_month")
      .agg(
        sum("quantity").as("total_quantity"),
        sum(col("quantity") * col("price")).as("total_sales")
      )

    salesByBookTitleDF.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "sales_by_book_title")
      .option("user", dbUser)
      .option("password", dbPassword)
      .mode("overwrite")
      .save()

    salesByAuthorNameDF.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "sales_by_author_name")
      .option("user", dbUser)
      .option("password", dbPassword)
      .mode("overwrite")
      .save()

    salesByMonthDF.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "sales_by_month")
      .option("user", dbUser)
      .option("password", dbPassword)
      .mode("overwrite")
      .save()

    spark.stop()
  }
}
