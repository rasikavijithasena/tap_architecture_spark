package com.spark.kafka; /**
 * Created by cloudera on 10/30/17.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.File;
import java.util.*;

public class SparkKafkaTrans {
    //private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {


        //  StreamingExamples.setStreamingLogLevels();

        String brokers = "localhost:9092";
        //String topics = args[1];
        String topics = "new_topic";

        // Create context with a 2 seconds batch interval
        String appName = "SparkKafkaTrans";
        String master = "local[*]";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
        jssc.checkpoint("/home/cloudera/Documents/check");


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark com.spark.SparkExamples.Hive")
                .config("spark.sql.warehouse.dir", "hdfs://quickstart.cloudera:8020/user/hive/warehouse")
                .config("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
                .enableHiveSupport()
                .getOrCreate();


        createHiveTable(spark);
        streamKafka( jssc,  brokers, topics, spark);
        //readHive(spark);
        //readKafka(spark);

    }

    public static void createHiveTable(SparkSession spark) {

        spark.sql("CREATE TABLE IF NOT EXISTS trans_hive_kafka(" +
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

    }

    public static void streamKafka(JavaStreamingContext jssc, String brokers, String topics, SparkSession spark) throws Exception {

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
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));


        // Get the lines, split them into words, count the words and print
        JavaDStream<String> logdata = messages.map(ConsumerRecord::value);

        logdata.print();

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

                    System.out.println("");

                    trans.setId(parts[0]);
                    trans.setTime_stamp(parts[1]);
                    trans.setSp_id(parts[2]);
                    trans.setService_provider(parts[3]);
                    trans.setApp_id(parts[4]);
                    trans.setApp_name(parts[5]);
                    trans.setState_app(parts[6]);
                    trans.setSource_entity_address(parts[7]);
                    trans.setSource_entity_masked(parts[8]);
                    trans.setChannel_type(parts[9]);

                    trans.setSource_protocol(parts[10]);
                    trans.setDest_address(parts[11]);
                    trans.setDest_masked(parts[12]);
                    trans.setDest_channel_type(parts[13]);
                    trans.setDest_protocol(parts[14]);
                    trans.setTravel_direction(parts[15]);
                    trans.setNcs(parts[16]);
                    trans.setBilling(parts[17]);
                    trans.setPart_entity_type(parts[18]);
                    trans.setCharge_amount(parts[19]);

                    trans.setCurrency(parts[20]);
                    trans.setExchange_rates(parts[21]);
                    trans.setCharging_service_code(parts[22]);
                    trans.setMsisdn(parts[23]);
                    trans.setMasked_msisdn(parts[24]);
                    trans.setBilling_event(parts[25]);
                    trans.setResponse_code(parts[26]);
                    trans.setResponse_desc(parts[27]);
                    trans.setTransaction_state(parts[28]);
                    trans.setTransaction_keyword(parts[29]);

                    trans.setCol_31(parts[30]);
                    trans.setCol_32(parts[31]);
                    trans.setCol_33(parts[32]);
                    trans.setCol_34(parts[33]);
                    trans.setCol_35(parts[34]);
                    trans.setCol_36(parts[35]);
                    trans.setCol_37(parts[36]);
                    trans.setCol_38(parts[37]);
                    trans.setCol_39(parts[38]);
                    trans.setCol_40(parts[39]);

                    trans.setCol_41(parts[40]);
                    trans.setCol_42(parts[41]);
                    trans.setCol_43(parts[42]);
                    trans.setCol_44(parts[43]);
                    trans.setCol_45(parts[44]);
                    trans.setCol_46(parts[45]);
                    trans.setCol_47(parts[46]);
                    trans.setCol_48(parts[47]);
                    trans.setCol_49(parts[48]);
                    trans.setCol_50(parts[49]);

                    trans.setCol_51(parts[50]);
                    trans.setCol_52(parts[51]);
                    trans.setCol_53(parts[52]);
                    trans.setCol_54(parts[53]);
                    trans.setCol_55(parts[54]);
                    trans.setCol_56(parts[55]);
                    trans.setCol_57(parts[56]);
                    trans.setCol_58(parts[57]);
                    trans.setCol_59(parts[58]);
                    trans.setCol_60(parts[59]);

                    return trans;
                });

        tranStream.print();

        tranStream.foreachRDD(rdd -> {
            rdd.checkpoint();
            Dataset<Row> df = spark.createDataFrame(rdd, Transaction.class);
            df.createOrReplaceTempView("trans1");
            df.show();



            //  Dataset<Row> smscount =
            //         spark.sql("SELECT SUBSTR(time_stamp,1,10) AS date, count(id) AS no_of_trans, channel_type, travel_direction AS direction FROM trans1 WHERE (channel_type = 'sms' AND travel_direction = 'mo') GROUP BY SUBSTR(time_stamp,1,10), channel_type, travel_direction");
            //  smscount.createOrReplaceTempView("summary");
            //  smscount.show();

//            spark.sql("INSERT INTO TABLE trans_hive_kafka SELECT * FROM trans1");
//            Dataset<Row> summary = spark.sql("SELECT * FROM trans_hive_kafka  ");
//            summary.show();

        });




        jssc.start();
        jssc.awaitTermination();


    }

    public static void readHive(SparkSession spark) {

        Dataset<Row> namesDF = spark.sql("SELECT * FROM trans_hive_kafka limit 10");
        namesDF.show();

    }


}
