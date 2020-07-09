package com.example {

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BostonCrimesMap {
   def main(args: Array[String]) {
	if (args.length == 0) {
            println("dude, i need at least one parameter")
        }
        val file_crime = args(0)
        val file_codes = args(1)
	val output_folder = args(2)

        //val spark = SparkSession.builder.master("yarn").appName("Spark SQL").getOrCreate()
	val spark = SparkSession.builder.master("local").appName("Spark SQL").getOrCreate()
	
	// For implicit conversions like converting RDDs to DataFrames
	import spark.implicits._

	val df_crime = spark.read.option("header", "true").option("inferSchema", "true").csv(file_crime)
	val df_code = spark.read.option("header", "true").option("inferSchema", "true").csv(file_codes)
	//val df_crime = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/crime.csv")
	//val df_code = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/offense_codes.csv")

	//df_code.printSchema()
	//df_code.show()
	//df_crime.printSchema()
	//df_crime.show()
	//df_crime.groupBy("DISTRICT").count().withColumnRenamed("count", "crimes").show()
	//df_crime.groupBy("DISTRICT", "YEAR", "MONTH").count().withColumnRenamed("count", "crimes").orderBy("DISTRICT", "YEAR", "MONTH").show(df_crime.count.toInt, false)


	/////////////////// create base sql view ///////////////////////////////
	val df_crime_base = df_crime.groupBy("DISTRICT", "YEAR", "MONTH").count().withColumnRenamed("count", "crimes").orderBy("DISTRICT", "crimes")
	df_crime_base.createOrReplaceTempView("crime_base_sql")
        val df_crime_base2 = df_crime.select("DISTRICT", "Lat", "Long")
	df_crime_base2.createOrReplaceTempView("crime_base2_sql")

	/////////////////// crimes total ///////////////////////////////
	val df_crimes_total = spark.sql("SELECT DISTRICT, sum(crimes) as crimes_total  from crime_base_sql group by DISTRICT order by crimes_total")
	//df_crimes_total.show()

	/////////////////// crimes monthly ///////////////////////////////
	val df_crimes_monthly = spark.sql("SELECT DISTRICT, percentile_approx(crimes, 0.5) as crimes_monthly  from crime_base_sql group by DISTRICT order by crimes_monthly")
	//df_crimes_monthly.show()

	////////////////// crime location ////////////////////////////////////
	val df_crime_location = spark.sql("SELECT DISTRICT, avg(Lat) as lat, avg(Long) as lng from crime_base2_sql group by DISTRICT")
	//df_crime_location.show()

	/////////////////// create crime_type sql view ///////////////////////////////
	val df_join_type = df_crime.join(broadcast(df_code), df_code("CODE") <=> df_crime("OFFENSE_CODE"))
	df_join_type.createOrReplaceTempView("crime_join_sql")
	val df_crime_type = spark.sql("SELECT DISTRICT, substring_index(NAME, ' - ', 1) as crime_type, count(*) as crimes FROM crime_join_sql group by DISTRICT, crime_type order by DISTRICT, crimes")
	df_crime_type.createOrReplaceTempView("crime_type_sql")
	//df_crime_type.where($"DISTRICT".isNull).show(100, false)

	/////////////////// frequent crime types  ///////////////////////////////
	val df_freq_crime = spark.sql("SELECT DISTRICT, crime_type, crimes FROM (SELECT DISTRICT, crime_type, crimes, dense_rank() OVER (PARTITION BY DISTRICT ORDER BY crimes DESC) as rank FROM crime_type_sql) tmp WHERE rank <= 3 ORDER BY DISTRICT, crimes DESC")
	//df_freq_crime.show(100, false)
	val df_freq_group = df_freq_crime.groupBy("DISTRICT").agg(concat_ws(", ", collect_list("crime_type") as "crime_type").alias("frequent_crime_types")).orderBy("DISTRICT")
	//df_freq_group.show(false)

	/////////////////// summary table ////////////////////////////////////////
	val df_summary_1 = df_crimes_total.alias("a").join(df_crimes_monthly.alias("b"), df_crimes_total("DISTRICT") <=> df_crimes_monthly("DISTRICT")).select("a.DISTRICT", "a.crimes_total", "b.crimes_monthly")
	val df_summary_2 = df_summary_1.alias("c").join(df_freq_group.alias("d"), df_summary_1("DISTRICT") <=> df_freq_group("DISTRICT")).select("c.DISTRICT", "c.crimes_total", "c.crimes_monthly", "d.frequent_crime_types")
	val df_summary_3 = df_summary_2.alias("e").join(df_crime_location.alias("f"), df_summary_2("DISTRICT") <=> df_crime_location("DISTRICT")).select("e.DISTRICT", "e.crimes_total", "e.crimes_monthly", "e.frequent_crime_types", "f.lat", "f.lng")
	//df_summary_3.orderBy("crimes_total").show(false)
        //df_summary_3.orderBy("crimes_total").repartition(1).write.parquet("./result")
        df_summary_3.orderBy("crimes_total").repartition(1).write.parquet(output_folder)

   }
 }
}

