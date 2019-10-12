/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/6
 * Time: 16:22
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package com.flow.generateData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class GenerateData {
    public static void main(String[] args) throws IOException {
        String log_name = "log.txt";
        File file = new File(log_name);
        if (file.exists() == false) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        FileWriter fileWriter = new FileWriter(file, true);
        PrintWriter printWriter = new PrintWriter(fileWriter);


        for (int i = 0; i < 1000; i++) {
            String phoneNumber = generateNum();
            long up_flow = (long) (Math.random() * Math.exp(3));
            long down_flow = (long) (Math.random() * Math.exp(5));
            printWriter.println(phoneNumber + " " + up_flow + " " + down_flow);
        }
        printWriter.close();
    }

    static String generateNum() {
        String phoneNumber = "135";
        for (int i = 0; i < 8; i++) {
            phoneNumber += (int) (Math.random() * 10) + "";
        }
        return phoneNumber;
    }
}
