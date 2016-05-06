package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class SqlDataSource {

	private static transient JavaSparkContext ctx;

	// Read table from MySQL (MasterURL, db, user, pswd, table,
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("SQLDataSource")
				.setMaster(args[0]);
		String database = args[1];
		String user = args[2];
		String password = args[3];
		String table = args[4];
		ctx = new JavaSparkContext(sparkConf);
		SQLContext sql = new SQLContext(ctx.sc());

		DataFrame rows = sql
				.read()
				.format("jdbc")
				.option("url",
						"jdbc:mysql://localhost:3306/" + database + "?user="
								+ user + "&password=" + password )
				.option("dbtable", table).load();

		rows.printSchema();
		System.out.println("table count:" + rows.count());
		rows.limit(100).show();
		rows.write().format("json").save(args[5]);

		JavaRDD<Row> rdd = rows.toJavaRDD();
		System.out.println(rdd.count());
	}
}
