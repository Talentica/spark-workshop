package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class RddtoDF {

	private static transient JavaSparkContext ctx;

	//Pass persons.txt as input file
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("rddtodf")
				.setMaster(args[0]);
		ctx = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> textFile = ctx.textFile(args[1]);
		
		JavaRDD<Person> person = textFile.map(new Function<String, Person>(){

			private static final long serialVersionUID = 1L;

			public Person call(String row) throws Exception {
				String[] cols = row.split(",");
				Person record = new Person();
				record.setName(cols[0]);
				record.setCity(cols[1]);
				record.setState(cols[2]);
				return record;
			}
			
		});
		System.out.println(person.count());
		
		//RDD to DataFrame
		SQLContext sqlContext = SQLContext.getOrCreate(person.context());
		DataFrame personDF = sqlContext.createDataFrame(person, Person.class);
		personDF.registerTempTable("tpersons");
		personDF.groupBy(personDF.col("city")).count().orderBy(org.apache.spark.sql.functions.desc("count")).limit(10).show();
		
		sqlContext.sql("select distinct state from tpersons").show();
		
	}

}
