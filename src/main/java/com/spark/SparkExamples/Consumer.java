package com.spark.SparkExamples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by cloudera on 11/8/17.
 */
public class Consumer {
    private static Scanner in;
    private static boolean stop = false;

    public static void main(String[] argv)throws Exception{

//        if (argv.length != 2) {
//            System.err.printf("Usage: %s <topicName> <groupId>\n",
//                    com.spark.SparkExamples.Consumer.class.getSimpleName());
//            System.exit(-1);
//        }

        in = new Scanner(System.in);
        //String topicName = argv[0];
//        String groupId = argv[1];
        String topicName = "test";
        String groupId = "testgroup";

        ConsumerThread consumerRunnable = new ConsumerThread(topicName,groupId);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread{
        private String topicName;
        private String groupId;
        private KafkaConsumer<String,String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId){
            this.topicName = topicName;
            this.groupId = groupId;
        }
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            //configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));

            System.out.println("start message");

            final int giveUp = 100;   int noRecordsCount = 0;

            //Start processing messages
            try {
                while (true) {
                    System.out.println("try inside 1");

                    ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);

                    if (records.count()==0) {
                        noRecordsCount++;
                        if (noRecordsCount > giveUp) break;
                        else continue;
                    }

                    System.out.println("try inside message");
                    for (ConsumerRecord<String, String> record : records)
                        System.out.println(record.value());
                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            }finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }

        public KafkaConsumer<String,String> getKafkaConsumer(){
            return this.kafkaConsumer;
        }
    }
}