package com.spark.SparkExamples; /**
 * Created by cloudera on 11/8/17.
 */
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

public class BasicProducerExample {

    public static void main(String[] args){

        Scanner in = new Scanner(System.in);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        TestCallback callback = new TestCallback();
        /*Random rnd = new Random();
        for (long i = 0; i < 100 ; i++) {
            ProducerRecord<String, String> data = new ProducerRecord<String, String>(
                    "test", "key-" + i, "message-"+i );
            producer.send(data, callback);
        }*/


        FileInputStream fis;
        BufferedReader br = null;
        int lineCount = 0;

        try {
            fis = new FileInputStream(new File("/home/cloudera/Downloads/rasika/sdp-log1/created"));
            //Construct BufferedReader from InputStreamReader
            br = new BufferedReader(new InputStreamReader(fis));

           int count = 0;

            while(count == 0){

                String line = null;

                StringBuffer stringBuffer = new StringBuffer();

                while ((line = br.readLine()) != null) {
                    stringBuffer.append(line);
                    stringBuffer.append("\n");

                }

                ProducerRecord<String, String> rec = new ProducerRecord<>("new_topic", stringBuffer.toString());
                producer.send(rec);
                count = 1;
            }


        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            try {
                br.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

//        String line = in.nextLine();
//
//        while(!line.equals("exit")) {
//            ProducerRecord<String, String> rec = new ProducerRecord<>("test", line);
//
//            producer.send(rec);
//            line = in.nextLine();
//        }
//        in.close();
//        producer.close();





    }


    private static class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.println(message);
            }
        }
    }

}