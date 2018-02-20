package com.spark.SparkExamples;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static jodd.util.ThreadUtil.sleep;

/**
 * Created by cloudera on 11/14/17.
 */
public class WriteFile {

    public static void main(String[] args) {

        int count = 0;

        while(count <=10) {
            try {

                count ++;
                String content = "0171012180029044"+ count +"|2017-10-12 18:00:29 117|SPP_000938|hsenid217@gmail.com|APP_005541|Winter|live|" +
                        "94753456889||sms|smpp|||||mo|subscription|unknown||||||||registration|S1000|Request was successfully processed.|" +
                        "success|winter|77000|airtel||percentageFromMonthlyRevenue|70||||S1000|Success|||0|||||||||||daily|||||||" + "\n";

                File file = new File("/home/cloudera/Downloads/rasika/sdp-log1/created");

                // if file doesnt exists, then create it
                if (!file.exists()) {
                    file.createNewFile();
                }

                FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(content);
                bw.close();

                sleep(5000);

                System.out.println("Done");

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
