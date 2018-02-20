package com.spark.SparkExamples; /**
 * Created by cloudera on 10/23/17.
 */

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;

import org.apache.spark.sql.*;

import org.apache.spark.SparkConf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


import static java.lang.Thread.sleep;

public class SparkStream {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("com.spark.SparkExamples.SparkStream").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(40000));

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.sql.warehouse.dir", "hdfs://quickstart.cloudera:8020/user/hive/warehouse")
                .config("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
                .enableHiveSupport()
                .getOrCreate();
          spark.conf().set("spark.sql.shuffle.partitions", "2");
      //  spark.conf().set("spark.default.parallelism", "10");

        DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
        Date dateobj = new Date();
        System.out.println(df.format(dateobj));
        //getTransaction(spark);
        readFile(spark, ssc);
        System.out.println(df.format(dateobj));



    }


    private static void readFile(SparkSession spark, JavaStreamingContext ssc) throws InterruptedException {

        DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
        Date dateobj = new Date();
        System.out.println("start " + df.format(dateobj));

        JavaDStream<String> logdata = ssc.textFileStream("/home/cloudera/Downloads/rasika/sdp-log2");  ///cp /home/cloudera/Downloads/rasika/sdp-log1/sdp-1-translog.log.2017-10-16-10-40 /home/cloudera/Downloads/rasika/sdp-log2/sdp-1-translog.log.2017-10-16-10-40

        JavaDStream<Transaction> tranStream = logdata
                .map(line -> {
                    String[] part = line.split("\\|");
                    Transaction trans = new Transaction();

                    int length = part.length;
                    String parts[] = new String[60];

                    if (length<60) {

                        for (int i = 0; i<60; i++){
                            parts[i] = " ";
                        }

                        for(int j = 0; j<length; j++){
                            parts[j] = part[j];
                        }
                    } else {
                        parts = part;
                    }


                    trans.setId(parts[0]); trans.setTime_stamp(parts[1]); trans.setSp_id(parts[2]); trans.setService_provider(parts[3]); trans.setApp_id(parts[4]);
                    trans.setApp_name(parts[5]); trans.setState_app(parts[6]); trans.setSource_entity_address(parts[7]); trans.setSource_entity_masked(parts[8]); trans.setChannel_type(parts[9]);

                    trans.setSource_protocol(parts[10]); trans.setDest_address(parts[11]); trans.setDest_masked(parts[12]); trans.setDest_channel_type(parts[13]); trans.setDest_protocol(parts[14]);
                    trans.setTravel_direction(parts[15]); trans.setNcs(parts[16]); trans.setBilling(parts[17]); trans.setPart_entity_type(parts[18]); trans.setCharge_amount(parts[19]);

                    trans.setCurrency(parts[20]); trans.setExchange_rates(parts[21]); trans.setCharging_service_code(parts[22]); trans.setMsisdn(parts[23]); trans.setMasked_msisdn(parts[24]);
                    trans.setBilling_event(parts[25]); trans.setResponse_code(parts[26]); trans.setResponse_desc(parts[27]); trans.setTransaction_state(parts[28]); trans.setTransaction_keyword(parts[29]);

                    trans.setCol_31(parts[30]); trans.setCol_32(parts[31]); trans.setCol_33(parts[32]); trans.setCol_34(parts[33]); trans.setCol_35(parts[34]);
                    trans.setCol_36(parts[35]); trans.setCol_37(parts[36]); trans.setCol_38(parts[37]); trans.setCol_39(parts[38]); trans.setCol_40(parts[39]);

                    trans.setCol_41(parts[40]); trans.setCol_42(parts[41]); trans.setCol_43(parts[42]); trans.setCol_44(parts[43]); trans.setCol_45(parts[44]);
                    trans.setCol_46(parts[45]); trans.setCol_47(parts[46]); trans.setCol_48(parts[47]); trans.setCol_49(parts[48]); trans.setCol_50(parts[49]);

                    trans.setCol_51(parts[50]); trans.setCol_52(parts[51]); trans.setCol_53(parts[52]); trans.setCol_54(parts[53]); trans.setCol_55(parts[54]);
                    trans.setCol_56(parts[55]); trans.setCol_57(parts[56]); trans.setCol_58(parts[57]); trans.setCol_59(parts[58]);
                    trans.setCol_60(parts[59]);


                    //System.out.println(parts[1]);
                    return trans;
                });



        tranStream.foreachRDD(rdd-> {

            Date dateobj1 = new Date();
            System.out.println("stream over "  + df.format(dateobj1));

            Dataset<Row> transDF = spark.createDataFrame(rdd, Transaction.class);

            Dataset<Row> groupDF = transDF.groupBy("app_id").count();
            groupDF.show();


        /*Dataset<Row> transDF = spark.createDataFrame(transRDD, Transaction.class);
        transDF.createOrReplaceTempView("temp");

        spark.sql("CREATE TABLE IF NOT EXISTS trans_large(" +
                "id String,time_stamp String, sp_id String, service_provider String, app_id String,"+
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

        spark.sql("INSERT INTO TABLE trans_large SELECT * FROM temp");*/

            Date dateobj2 = new Date();
            System.out.println("hive over" + df.format(dateobj2));
            System.out.println( df.format(dateobj2));

        });

        ssc.start();
        ssc.awaitTermination();

    }








    public static void readDataStream(SparkSession spark, JavaStreamingContext ssc) throws InterruptedException {
        JavaDStream<String> logdata = ssc.textFileStream("/home/cloudera/Downloads/rasika/sdp-log2");  ///cp /home/cloudera/Downloads/rasika/sdp-log1/sdp-1-translog.log.2017-10-16-10-40 /home/cloudera/Downloads/rasika/sdp-log2/sdp-1-translog.log.2017-10-16-10-40

        JavaDStream<Transaction> tranStream = logdata
                   .map(line -> {
                    String[] part = line.split("\\|");
                    Transaction trans = new Transaction();

                    int length = part.length;
                    String parts[] = new String[60];

                    if (length<60) {

                        for(int i = 0; i<60; i++){
                            parts[i] = " ";
                        }

                        for(int j = 0; j<length; j++){
                            parts[j] = part[j];
                        }
                    } else {
                        parts = part;
                    }


                    trans.setId(parts[0]); trans.setTime_stamp(parts[1]); trans.setSp_id(parts[2]); trans.setService_provider(parts[3]); trans.setApp_id(parts[4]);
                    trans.setApp_name(parts[5]); trans.setState_app(parts[6]); trans.setSource_entity_address(parts[7]); trans.setSource_entity_masked(parts[8]); trans.setChannel_type(parts[9]);

                    trans.setSource_protocol(parts[10]); trans.setDest_address(parts[11]); trans.setDest_masked(parts[12]); trans.setDest_channel_type(parts[13]); trans.setDest_protocol(parts[14]);
                    trans.setTravel_direction(parts[15]); trans.setNcs(parts[16]); trans.setBilling(parts[17]); trans.setPart_entity_type(parts[18]); trans.setCharge_amount(parts[19]);

                    trans.setCurrency(parts[20]); trans.setExchange_rates(parts[21]); trans.setCharging_service_code(parts[22]); trans.setMsisdn(parts[23]); trans.setMasked_msisdn(parts[24]);
                    trans.setBilling_event(parts[25]); trans.setResponse_code(parts[26]); trans.setResponse_desc(parts[27]); trans.setTransaction_state(parts[28]); trans.setTransaction_keyword(parts[29]);

                    trans.setCol_31(parts[30]); trans.setCol_32(parts[31]); trans.setCol_33(parts[32]); trans.setCol_34(parts[33]); trans.setCol_35(parts[34]);
                    trans.setCol_36(parts[35]); trans.setCol_37(parts[36]); trans.setCol_38(parts[37]); trans.setCol_39(parts[38]); trans.setCol_40(parts[39]);

                    trans.setCol_41(parts[40]); trans.setCol_42(parts[41]); trans.setCol_43(parts[42]); trans.setCol_44(parts[43]); trans.setCol_45(parts[44]);
                    trans.setCol_46(parts[45]); trans.setCol_47(parts[46]); trans.setCol_48(parts[47]); trans.setCol_49(parts[48]); trans.setCol_50(parts[49]);

                    trans.setCol_51(parts[50]); trans.setCol_52(parts[51]); trans.setCol_53(parts[52]); trans.setCol_54(parts[53]); trans.setCol_55(parts[54]);
                    trans.setCol_56(parts[55]); trans.setCol_57(parts[56]); trans.setCol_58(parts[57]); trans.setCol_59(parts[58]);
                    trans.setCol_60(parts[59]);

                     System.out.println("");
                    return trans;
                });

        tranStream.print();

        tranStream.foreachRDD(rdd->{
            Dataset<Row> df = spark.createDataFrame(rdd,Transaction.class);
            df.createOrReplaceTempView("trans1");

            Dataset<Row> counts =
            spark.sql("SELECT SUBSTR(time_stamp,1,10) AS date, count(id) AS no_of_trans FROM trans1 GROUP BY SUBSTR(time_stamp,1,10)");


            if(tranStream!=null) {
                File f = new File("streaming.parquet");
                if (f.exists() || f.isDirectory()) {
                    counts.write().mode(SaveMode.Append)
                            .format("parquet")
                            .save("streaming.parquet");

                } else {
                    counts.write()
                            .mode(SaveMode.Overwrite)
                            .format("parquet")
                            .save("streaming.parquet");

                }
            }

            Dataset<Row> s2 = spark.read().parquet("streaming.parquet");
            s2.createOrReplaceTempView("trans_spark_date");
            Dataset<Row> result = spark.sql("SELECT * FROM trans_spark_date");
            result.show();

        });

        ssc.start();
        ssc.awaitTermination();
        //ssc.stop();
    }

    public static void read(SparkSession spark){

        Dataset<Row> parquetFileDF = spark.read().parquet("streaming.parquet");
        parquetFileDF.createOrReplaceTempView("parquetFile");

        //All records
        Dataset<Row> allDF = spark.sql("SELECT * FROM parquetFile");

        //Summary of records
        Dataset<Row> summaryDF = spark.sql("SELECT date, sum(no_of_trans) AS all_trans FROM parquetFile group by date");

        System.out.println("All records");
        allDF.show();

        System.out.println("Summary of records");
        summaryDF.show();
    }

}
