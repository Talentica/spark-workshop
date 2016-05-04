package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class JsonDataSource {
	private static transient JavaSparkContext ctx;

	//Pass stocks.json as input file
	public static void main(String[] args) {
		String fileName = args[0];
		SparkConf sparkConf = new SparkConf().setAppName("JSONDataSource")
				.setMaster("local");
		ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(ctx.sc());

		DataFrame stocks = sqlContext.read().json(fileName);

		// Schema Generation
		stocks.printSchema();
		String[] fields = stocks.schema().fieldNames();
		for (int i = 0; i < fields.length; i++) {
			System.out.println(fields[i]);
		}

		// Reading Data
		System.out.println("FirstRow:" + stocks.first());

		// GroupBy Operations
		DataFrame filterStocks = stocks.select(stocks.col("Company"),stocks.col("Price")).filter(stocks.col("Price").between(15, 20))
				.orderBy(stocks.col("Price")).limit(7);
		
		filterStocks.printSchema();
		filterStocks.show();

		stocks.groupBy(stocks.col("Sector"))
				.agg(org.apache.spark.sql.functions.max(stocks.col("Price")))
				.show();

		// Query as a Table
		stocks.registerTempTable("tstocks");

		sqlContext.sql("select count(1) from tstocks").show();

		sqlContext.sql("select max(\"52-Week Low\") from tstocks").show();

		sqlContext.sql("select Sector, max(Price) from tstocks group by Sector").show();

	}

}
