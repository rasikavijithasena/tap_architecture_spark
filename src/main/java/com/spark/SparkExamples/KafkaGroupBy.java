package com.spark.SparkExamples; /**
 * Created by cloudera on 11/14/17.
 */
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.File;
import java.util.*;

/**
 * Created by cloudera on 11/10/17.
 */

/**
 * Created by cloudera on 11/1/17.
 */


public class KafkaGroupBy{

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("com.spark.SparkExamples.StreamGroupFunction").setMaster("local[*]");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000));



        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark com.spark.SparkExamples.Hive")
                .config("spark.sql.warehouse.dir", "hdfs://quickstart.cloudera:8020/user/hive/warehouse")
                .config("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
                .enableHiveSupport()
                .getOrCreate();


        createHiveTable(spark);
        readDataStream(spark, ssc);


    }

    public static void createHiveTable(SparkSession spark) {

        spark.sql("CREATE TABLE IF NOT EXISTS stream_summary (" +
                "time_stamp String, channel_type String, travel_direction String )");

        spark.sql("CREATE TABLE IF NOT EXISTS daily_summary(" +
                "date String, sp_id String, num String)");

        spark.sql("CREATE TABLE IF NOT EXISTS trans_hive_spark(" +
                "id String, time_stamp String, sp_id String, service_provider String, app_id String,"+
                "app_name String, state_app String, source_entity_address String, source_entity_masked String, channel_type String,"+
                "source_protocol String,dest_address String, dest_masked String, dest_channel_type String, dest_protocol String,"+
                "travel_direction String, ncs String, billing STRING, part_entity_type String, charge_amount String,"+
                "currency String, exchange_rates String, charging_service_code String, msisdn String, masked_msisdn String,"+
                "billing_event String, response_code String, response_desc String, transaction_state String, transaction_keyword String,"+
                "col_31 String, col_32 String, col_33 String, col_34 String, col_35 String," +
                "col_36 String, col_37 String, col_38 String, col_39 String, col_40 String," +
                "col_41 String, col_42 String, col_43 String, col_44 String, col_45 String," +
                "col_46 String, col_47 String, col_48 String, col_49 String, col_50 String," +
                "col_51 String, col_52 String, col_53 String, col_54 String, col_55 String," +
                "col_56 String, col_57 String, col_58 String, col_59 String, col_60 String" +
                ") ");

    }

    public static void readDataStream(SparkSession spark, JavaStreamingContext ssc) throws InterruptedException {


        String brokers = "localhost:9092";
        String topics = "new_topic";

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));


        // Get the lines, split them into words, count the words and print
        JavaDStream<String> logdata = messages.map(ConsumerRecord::value);

        JavaDStream<Transaction> tranStream = logdata
                .map(line -> {

                    String[] part = line.split("\\|");
                    Transaction trans = new Transaction();

                    int length = part.length;
                    String parts[] = new String[60];

                    if (length < 60) {

                        for (int i = 0; i < 60; i++) {
                            parts[i] = " ";
                        }

                        for (int j = 0; j < length; j++) {
                            parts[j] = part[j];
                        }
                    } else {
                        parts = part;
                    }

                    String channel = parts[9];
                    String direction = parts[15];
                    //    String date = parts[1].substring(0, 10);

                    trans.setTime_stamp(parts[1]);
                    trans.setChannel_type(channel);
                    trans.setTravel_direction(direction);
                    trans.setSp_id(parts[4]);

                    return trans;

                });

        tranStream.foreachRDD(rdd -> {

            Dataset<Row> df = spark.createDataFrame(rdd, Transaction.class);
            df.repartition(1);

            Dataset<Row> df1_for_save = df.select("time_stamp", "travel_direction","channel_type").where("channel_type = 'sms'");
            Dataset<Row> df2_for_save = df1_for_save.select("time_stamp", "travel_direction","channel_type").where("travel_direction = 'mo'");

            Dataset<Row> df1 = df.selectExpr("substring(time_stamp,0,10) as time_stamp", "sp_id", "travel_direction").where("channel_type = 'sms'");
            Dataset<Row> df2 = df1.select("time_stamp", "sp_id").where("travel_direction = 'mo'");

//            Dataset<Row> ds = df1.groupBy("date").count();



            JavaRDD<Row> rd = df2.toJavaRDD();
            JavaPairRDD<Row,Integer> x = rd.mapToPair(word -> {return new Tuple2<>(word,1);})
                    .reduceByKey((a, b) -> a + b);



//            Dataset<Row> ds2 = spark.createDataFrame(JavaPairRDD.toRDD(x),Encoders.tuple(Encoders.bean(sum.class), Encoders.INT())).toDF("key","value");

            List<Sum> sumList = new ArrayList<Sum>();

            List<Tuple2<Row, Integer>> output = x.collect();
            for (Tuple2<?,?> tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());

                String[] dateStr = tuple._1().toString().split(",");
                Sum s = new Sum();
                s.setDate(dateStr[0].substring(1));
                s.setSp_id(dateStr[1].substring(0,dateStr[1].length()-1));

                s.setNum(tuple._2().toString());
                sumList.add(s);

            }

            if(sumList.size() != 0){

                Dataset<Row> dbDataset = spark.createDataFrame(sumList , Sum.class);
                dbDataset.show();
                dbDataset.createOrReplaceTempView("dbDataset");

                //Insert data to com.spark.SparkExamples.Hive
                //spark.sql("INSERT INTO TABLE daily_summary SELECT date, num, sp_id FROM dbDataset ");

                Dataset<Row> dailySummary = spark.sql("SELECT * FROM daily_summary");

                if(dailySummary.count() != 0) {

                    Dataset<Sum> dailyObjects = dailySummary.as(Encoders.bean(Sum.class));
                    //                   List<DailySummary> dataList = dailyObjects.collectAsList();

                    List<Sum> dataList = dailyObjects.takeAsList(100000);


                    List<Sum> insertList = new ArrayList<Sum>();


                    outloop:
                    for (Sum sumNew : sumList){

                        int counter = 0;

                        inloop:
                        for (Sum db : dataList){


                            if (sumNew.getDate().equals(db.getDate()) && sumNew.getSp_id().equals(db.getSp_id())){
                                String value = String.valueOf(Integer.parseInt((sumNew.getNum())) + Integer.parseInt((db.getNum())));

                                db.setNum(value);
                                System.out.println(value);
                                counter = 1;

                                break inloop;

                                //update quary

                                //spark.sql("UPDATE daily_summary SET sp_id = " + value + " WHERE num = '" + sumNew.getSp_id() + "' AND date = '" + sumNew.getDate() + "';");

                            } else {

                                //insert quary
                                //insertList.add(sumNew);

                            }


                        }

                        if(counter == 0) {
                            insertList.add(sumNew);
                        }

                    }


                    System.out.println(insertList.size());

                    Dataset<Row> updateDataset = spark.createDataFrame(dataList, Sum.class);
                    updateDataset.createOrReplaceTempView("updateData");

                    //Insert Query
                    spark.sql("TRUNCATE TABLE daily_summary");
                    spark.sql("CREATE TABLE IF NOT EXISTS daily_summary(" +
                            "date String, sp_id String, num String)");

                    spark.sql("INSERT INTO TABLE daily_summary SELECT date, sp_id, num FROM updateData");


                    if(insertList.size() != 0) {


                        Dataset<Row> insertDataset = spark.createDataFrame(insertList, Sum.class);
                        insertDataset.createOrReplaceTempView("insertData");


                        spark.sql("INSERT INTO TABLE daily_summary SELECT date, sp_id, num FROM insertData");



                    }

                } else {

                    //insert query
                    spark.sql("INSERT INTO TABLE daily_summary SELECT date, sp_id, num FROM dbDataset");
                }




            }

            Dataset<Row> printData = spark.sql("SELECT * FROM daily_summary");
            printData.show();


        });


        ssc.start();
        ssc.awaitTermination();
        //ssc.stop();
    }


    public static class Sum{

        private String date;
        private String sp_id;
        private String num;

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public String getSp_id() {
            return sp_id;
        }

        public void setSp_id(String sp_id) {
            this.sp_id = sp_id;
        }

        public String getNum() {
            return num;
        }

        public void setNum(String num) {
            this.num = num;
        }
    }

}


