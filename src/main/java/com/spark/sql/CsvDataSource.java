package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class CsvDataSource {

	private static transient JavaSparkContext ctx;

	//Pass us-500.csv as input file
	public static void main(String[] args) {
		String fileName = args[1];
		SparkConf sparkConf = new SparkConf().setAppName("CSVDataSource")
				.setMaster(args[0]);
		ctx = new JavaSparkContext(sparkConf);
		SQLContext sql = new SQLContext(ctx.sc());

		DataFrame rows = sql.read().format("com.databricks.spark.csv")
				.option("delimiter", ",")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true").load(fileName);

		rows.printSchema();
		rows.write().format("json").save(args[2]);

//		DataFrame drows = rows.select(rows.col("first_name"),rows.col("city"),rows.col("state"));
//		drows.write().format("com.databricks.spark.csv").option("header", "true").save("D:\\SparkWorkshop\\test");
		
		JavaRDD<Row> rdd = rows.toJavaRDD();
		System.out.println(rdd.count());
	}

}
