package com.spark.SparkExamples; /**
 * Created by cloudera on 10/29/17.
 */

import java.util.*;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

public class SparkKafka {

  /*  public static void main(String[] args) {

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        String appName = "SparkExample";
        String master = "local[*]";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset df = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "mytopic")
                .load();

        df.printSchema();

         **/

        private static final Pattern SPACE = Pattern.compile(" ");

        public static void main(String[] args) throws Exception {
            if (args.length < 2) {
                System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
                        "  <brokers> is a list of one or more Kafka brokers\n" +
                        "  <topics> is a list of one or more kafka topics to consume from\n\n");
                System.exit(1);
            }

            //  StreamingExamples.setStreamingLogLevels();

            String brokers = args[0];
            String topics = args[1];

            // Create context with a 2 seconds batch interval
            String appName = "SparkExample";
            String master = "local[*]";
            SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
            JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

            streamKafka( jssc,  brokers, topics);

        }

    public static void streamKafka(JavaStreamingContext jssc, String brokers, String topics) throws Exception{


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
            JavaDStream<String> lines = messages.map(ConsumerRecord::value);

            //get the words as strings
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
            words.print();

            //count
            JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                    .reduceByKey((i1, i2) -> i1 + i2);
            wordCounts.print();

            // Start the computation
            jssc.start();
            jssc.awaitTermination();


    }
}
