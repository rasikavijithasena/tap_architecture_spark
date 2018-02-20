package com.spark.SparkExamples; /**
 * Created by cloudera on 10/26/17.
 */

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkWithHive {

    public static void main(String[] args) {
        String appName = "com.spark.SparkExamples.SparkHive";
        String master = "local[*]";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark com.spark.SparkExamples.Hive")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        createHiveTable(spark);
        createTableFromDataFrame(spark);
        //readExternalHive(spark);

    }

    public static void createHiveTable(SparkSession spark) {

        spark.sql("CREATE TABLE trans_hive_spark("+
                "id String, time_stamp String, sp_id String, service_provider String, app_id String,"+
                "app_name String, state_app String, source_entity_address String, source_entity_masked String, channel_type String,"+
                "source_protocol String,dest_address String, dest_masked String, dest_channel_type String, dest_protocol String,"+
                "travel_direction String, ncs String, billing STRING, part_entity_type String, charge_amount String,"+
                "currency String, exchange_rates String, charging_service_code String, msisdn String, masked_msisdn String,"+
                "billing_event String, response_code String, response_desc String, transaction_state String, transaction_keyword String,"+
                "col_31 String, col_32 String, col_33 String, col_34 String, col_35 String,"+
                "col_36 String, col_37 String, col_38 String, col_39 String, col_40 String,"+
                "col_41 String, col_42 String, col_43 String, col_44 String, col_45 String,"+
                "col_46 String, col_47 String, col_48 String, col_49 String, col_50 String,"+
                "col_51 String, col_52 String, col_53 String, col_54 String, col_55 String,"+
                "col_56 String, col_57 String, col_58 String, col_59 String, col_60 String"+
                ") ");

    }

    public static void createTableFromDataFrame(SparkSession spark) {

        JavaRDD<Transaction> transRDD = spark.read()
                .textFile("/home/cloudera/Downloads/rasika/sdp-log/sdp-1-translog.log.2017-09-18-19-21")
                .javaRDD()
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

                    System.out.println("");
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


        Dataset<Row> transDF = spark.createDataFrame(transRDD, Transaction.class);
        transDF.createOrReplaceTempView("trans_logs");

        spark.sql("INSERT INTO TABLE trans_hive_spark SELECT * FROM trans_logs");
        spark.sql("SELECT * FROM trans_hive_spark LIMIT 10").show();

    }

    public static void readExternalHive(SparkSession spark) {

        Dataset<Row> hiveDF = spark.read().load("hdfs://quickstart.cloudera:8020/user/hive/warehouse/products");
        hiveDF.createOrReplaceTempView("products_hive");

        Dataset<Row> namesDF = spark.sql("SELECT * FROM products_hive limit 10");
        namesDF.show();

    }



}
