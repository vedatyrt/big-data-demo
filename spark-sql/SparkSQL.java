//this class query by using Spark SQL on MongoDB
//there are two way in this class : using DataFrame and Parquet File

package com.mongodb.spark.sql;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.spark.bean.Observation;

import scala.Tuple2;

public class SparkSqlMongo {

	public static void main(String[] args) {
		
		System.out.println("starts");
		long start = System.currentTimeMillis();

		Configuration obsConf = new Configuration();

		obsConf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
		obsConf.set("mongo.input.uri", "mongodb://192.168.1.149:27017/test.zips");

		// Configuration sensConf = new Configuration();
		//
		// sensConf.set("mongo.job.input.format",
		// "com.mongodb.hadoop.MongoInputFormat");
		// sensConf.set("mongo.input.uri",
		// "mongodb://192.168.1.149:27017/test.sens");

		SparkConf sconf = new SparkConf().setMaster("local[2]").setAppName("SQL DENEME");

		JavaSparkContext sc = new JavaSparkContext(sconf);
		SQLContext sql = new SQLContext(sc);

		JavaRDD<Observation> obs = sc.newAPIHadoopRDD(obsConf, MongoInputFormat.class, Object.class, BSONObject.class)
				.map(new Function<Tuple2<Object, BSONObject>, Observation>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Observation call(Tuple2<Object, BSONObject> v1) throws Exception {

						String id = (String) v1._2.get("id");
						double value = (double) v1._2.get("pop");
						// Date time = (Date) v1._2.get("Time");
						// int sensor = (int) v1._2.get("Name");
						// int stream = (int) v1._2.get("DataStreamId");

						Observation obs = new Observation(Integer.parseInt(id), value);
						return obs;

					}
				});
		DataFrame obsi = sql.createDataFrame(obs, Observation.class);

		obsi.registerTempTable("obsi");

		// JavaRDD<Sensor> sens = sc.newAPIHadoopRDD(sensConf,
		// MongoInputFormat.class, Object.class, BSONObject.class)
		// .map(new Function<Tuple2<Object, BSONObject>, Sensor>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Sensor call(Tuple2<Object, BSONObject> v1) throws Exception {
		//
		// int id = (int) v1._2.get("_id");
		// String name = (String) v1._2.get("Name");
		// String description = (String) v1._2.get("Description");
		//
		// Sensor s = new Sensor(id, name, description);
		//
		// // System.out.println(s.getName());
		// return s;
		//
		// }
		// });

		// DataFrame sensi = sql.createDataFrame(sens, Sensor.class);
		// sensi.cache();
		// // sql.registerDataFrameAsTable(sensi, "sensore");
		// // sql.registerDataFrameAsTable(obsi, "obsere");
		//
		// sensi.registerTempTable("sensi");

		// sensi.select("name").show();

		

		// DataFrame obser = sql.sql("SELECT obsi.value, obsi.id, sensi.name " +
		// "FROM obsi, sensi WHERE"
		// + " obsi.sensorID = sensi.id and sensi.id = 107");
		//
		// // DataFrame obser = sql.sql("select * from sensi").cache();
		//
		// System.out.println("count ===>>> " + obser.count());

		// List<Row> aa = obser.collectAsList();
		//
		// for (Iterator iterator = aa.iterator(); iterator.hasNext();) {
		// Row row = (Row) iterator.next();
		//
		// }

		// System.out.println("row ====>> " + counter);

		DataFrame oo = sql.sql("select * from obsi where value > 14 limit 1000000");

		System.out.println("counter : " + oo.count());

		long stop = System.currentTimeMillis();
		// System.out.println("count ====>>> " + a.toString());
		System.out.println("toplam sorgu zamani : " + (stop - start));
		;

	}
