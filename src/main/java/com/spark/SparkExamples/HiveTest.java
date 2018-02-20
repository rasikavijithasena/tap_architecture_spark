package com.spark.SparkExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * Created by cloudera on 11/2/17.
 */
public class HiveTest {


    public static void main(String[] args) {

        String appName = "com.spark.SparkExamples.Hive";
        String master = "local[*]";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();


        SparkSession spark1 = SparkSession
                .builder()
                .appName("Java Spark com.spark.SparkExamples.Hive")
                .config("spark.sql.warehouse.dir", "hdfs://quickstart.cloudera:8020/user/hive/warehouse")
                .config("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
                .enableHiveSupport()
                .getOrCreate();

        spark1.sql("CREATE TABLE t1 ("+
                "Date String, no_of_trans String, channel_type String, direction String )");

        Dataset<Row> namesDF = spark1.sql("show tables");
        namesDF.show();

        //createHiveTable(spark);
        //createTableFromDataFrame(spark);
        //readExternalHive(spark);

    }

    public static void readExternalHive(SparkSession spark) {

        //Dataset<Row> hiveDF = spark.read().load("hdfs://quickstart.cloudera:8020/user/hive/warehouse/products");
        //hiveDF.createOrReplaceTempView("products_hive");

        Dataset<Row> namesDF = spark.sql("SELECT * FROM trans_hive_spark limit 10");
        namesDF.show();

    }

}
