package com.spark.SparkExamples; /**
 * Created by cloudera on 10/18/17.
 */

import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SparkSql {


    public static void main(String[] args) throws AnalysisException {

        String appName = "SparkExample";
        String master = "local[*]";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.sql.warehouse.dir", "hdfs://quickstart.cloudera:8020/user/hive/warehouse")
                .config("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
                .enableHiveSupport()
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", "2");
        spark.conf().set("spark.default.parallelism", "2");

        DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
        Date dateobj = new Date();
        System.out.println(df.format(dateobj));
        //getTransaction(spark);
        readFile(spark);
        System.out.println(df.format(dateobj));
        spark.stop();
    }



    private static void readFile(SparkSession spark) {

        DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
        Date dateobj = new Date();
        System.out.println("start " + df.format(dateobj));

        JavaRDD<Transaction> transRDD = spark.read()
                .textFile("/home/cloudera/Downloads/rasika/sdp-log1/created")
                .javaRDD()
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

        Date dateobj1 = new Date();
        System.out.println("stream over "  + df.format(dateobj1));

        Dataset<Row> transDF = spark.createDataFrame(transRDD, Transaction.class);
        Dataset<Row> groupDF = transDF.groupBy("app_id" , "time_stamp").count();
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
        System.out.println(df.format(dateobj) + "    " + df.format(dateobj2));

    }






    private static void getTransaction(SparkSession spark) {

        // Create an RDD of Person objects from a text file
        JavaRDD<Transaction> transRDD = spark.read()
                .textFile("/home/cloudera/Downloads/rasika/sdp-log1/created")
                .javaRDD()
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



                    return trans;
                });


        //transRDD.saveAsTextFile("/home/cloudera/Desktop/trans.txt");


        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> transDF = spark.createDataFrame(transRDD, Transaction.class);

        /***
         * dataframe write into the parquet file
         *
         transDF.write().parquet("people.parquet");
        Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");
        parquetFileDF.show();

         ***/


        //save as parquet file with partitioning
        transDF.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("col_12")
                .format("parquet")
                .save("namesPartByCol_13.parquet");        //saveAsTable can be used to save as a table

        Dataset<Row> parquetFileDF = spark.read().parquet("namesPartByCol_13.parquet");
        parquetFileDF.createOrReplaceTempView("parquetFile");

        Dataset<Row> namesDF = spark.sql("SELECT * FROM parquetFile");

        //get the row count
        Dataset<Row> countDF = spark.sql("SELECT count(*) FROM parquetFile");

        //display data
        namesDF.show();
        countDF.show();

        // Register the DataFrame as a temporary view
        transDF.createOrReplaceTempView("transaction");

        //transDF.show();




/**
 * table format
 *
        transDF.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("col_12")
                .format("parquet")
                .saveAsTable("namesPartByColumns");

        Dataset<Row> namesDF = spark.sql("SELECT * FROM namesPartByColumns");

        //display data
        namesDF.show();
 ***/

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> selectDF = spark.sql("SELECT id,time_stamp FROM transaction limit 10");

       //The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING();

        Dataset<String> selectColumnByIndexDF = selectDF.map(
                (MapFunction<Row, String>) row -> "id: " + row.getString(0) + "time_stamp" +row.getString(1),
                stringEncoder);
        selectColumnByIndexDF.show();



    }


}//https://www.cyberciti.biz/faq/centos-linux-6-install-java-sdk/
