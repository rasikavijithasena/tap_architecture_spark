package com.spark.SparkExamples; /**
 * Created by cloudera on 10/23/17.
 */

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHive {

    public static class Record implements Serializable {
        private int age;
        private String name;

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setValue(String name) {
            this.name = name;
        }
    }

    public static void main(String[] args){


        String appName = "com.spark.SparkExamples.SparkHive";
        String master = "local[*]";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);


        // warehouseLocation points to the default location for managed databases and tables
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        //String warehouseLocation = "file:" + System.getProperty("user.dir") + "spark-warehouse";
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark com.spark.SparkExamples.Hive Example")
                //.config("spark.sql.warehouse.dir", "hdfs://quickstart.cloudera/user/hive/warehouse/")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("CREATE TABLE IF NOT EXISTS src2 (name string, age string) row format delimited fields terminated by ','");
        spark.sql("LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/test.txt' INTO TABLE src2");

// Queries are expressed in HiveQL
        spark.sql("SELECT * FROM src2").show();
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

// Aggregation queries are also supported.
        spark.sql("SELECT COUNT(*) FROM src2").show();
// +--------+
// |count(1)|
// +--------+
// |    500 |
// +--------+

        // The results of SQL queries are themselves DataFrames and support all normal functions.
        Dataset<Row> sqlDF = spark.sql("SELECT name, age FROM src WHERE age < 10 ORDER BY age");

        // The items in DataFrames are of type Row, which lets you to access each column by ordinal.
        Dataset<String> stringsDS = sqlDF.map(
                (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
                Encoders.STRING());
        stringsDS.show();
// +--------------------+
// |               value|
// +--------------------+
// |Key: 0, Value: val_0|
// |Key: 0, Value: val_0|
// |Key: 0, Value: val_0|
// ...

        // You can also use DataFrames to create temporary views within a SparkSession.
        List<Record> records = new ArrayList<>();
        for(
                int key = 1;
                key<100;key++)

        {
            Record record = new Record();
            record.setAge(key);
            record.setValue("val_" + key);
            records.add(record);
        }

        Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");

// Queries can then join DataFrames data with data stored in com.spark.SparkExamples.Hive.
        spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
// +---+------+---+------+
// |key| value|key| value|
// +---+------+---+------+
// |  2| val_2|  2| val_2|
// |  2| val_2|  2| val_2|
// |  4| val_4|  4| val_4|
// ...
    }


}