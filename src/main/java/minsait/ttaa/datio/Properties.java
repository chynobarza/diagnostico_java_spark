package minsait.ttaa.datio;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import static minsait.ttaa.datio.common.Common.PARAMS_PATH;

public class Properties {


    public String position(int index) {
        String line = null;
        try {
            InputStream inputStream = new FileInputStream(PARAMS_PATH);

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            int i = 0;
            while (reader.ready()) {
                line = reader.readLine();
                if (index == i) {
                    break;
                }
                i++;
            }
        } catch (Exception e) {
            System.err.print(e);
        }
        return line;
    }
}
