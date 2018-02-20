package com.spark.SparkExamples; /**
 * Created by cloudera on 10/19/17.
 */

import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


public class SparkEx2 {
    public static void main(String[] args) throws Exception {

        String master = "local[*]";

        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("com.spark.SparkExamples.SparkEx2").setMaster(master).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> textFile = sc.textFile("/home/cloudera/Downloads/rasika/sdp-log/sdp-1-translog.log.2017-09-18-19-10");
        JavaRDD<List<String>> x =textFile.map(s -> Arrays.asList(s.split("\\|")));

        JavaPairRDD<String, Integer> count = textFile
                .flatMap(s-> Arrays.asList(s.split("\\|")).iterator())
                .mapToPair(word -> {
                    System.out.println(word);
                    return new Tuple2<>(word, 1);})
                .reduceByKey((a, b) -> a + b);
//        count.foreach({});
        List<Tuple2<String, Integer>> output = count.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }


        //count.saveAsTextFile("/home/cloudera/Desktop/people1.txt");
    }
}