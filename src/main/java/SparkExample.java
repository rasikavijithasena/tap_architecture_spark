/**
 * Created by cloudera on 10/18/17.
 */

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;

public class SparkExample {

    public static void main(String[] args){
        //String appName = "SparkExample";
        //String master = "local[*]";
        //SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        ///JavaSparkContext sc = new JavaSparkContext(conf);

       // JavaRDD<String> distFile = sc.textFile("data.txt");
        //JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        //lineLengths.persist(StorageLevel.MEMORY_ONLY());
        //int totalLength = lineLengths.reduce((a, b) -> a + b);


        System.out.println("jjjj");
    }
}
