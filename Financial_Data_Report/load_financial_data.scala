
import org.apache.spark.sql.functions.expr;
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Load Financial Data")
  .getOrCreate()

def loadStock(year:String, month:String, stock:String):org.apache.spark.sql.DataFrame = {
  spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"datasources/$year/$month/$stock.csv")
    .withColumnRenamed("Adj Close", "AdjClose")
    .withColumn("Intraday_Delta", expr("AdjClose - Open"))
}

val year = "2021"
val month = "dec"

val apple_stock_df = loadStock(year,month, "Apple")
val apptech_stock_df = loadStock(year,month, "AppTech")
val buzzfeed_stock_df = loadStock(year,month, "Buzzfeed")
val eur_usd_df = loadStock(year,month, "Apple")
val imetal_df = loadStock(year,month, "AppTech")
val microsoft_stock_df = loadStock(year,month, "Buzzfeed")

microsoft_stock_df.show()

val dfSeq = Seq(apple_stock_df, apptech_stock_df, buzzfeed_stock_df,eur_usd_df,imetal_df,microsoft_stock_df)
val monthly_assets_df = dfSeq.reduce(_ union _)

monthly_assets_df.registerTempTable("tmp_monthly_assets")
monthly_assets_df.show()

monthly_assets_df.write.mode("overwrite").save(s"datasources/$year/$month/monthly")
