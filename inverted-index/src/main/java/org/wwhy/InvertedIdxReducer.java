package org.wwhy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class InvertedIdxReducer extends Reducer<Text, Text, Text, Text> {

    private static final DecimalFormat formatter = new DecimalFormat("0.00");
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Set<String> fileSet = new HashSet<>();
        int wordSum = 0;
        StringBuilder invertedIdx = new StringBuilder();
        Iterator<Text> iter = values.iterator();
        while (iter.hasNext()) {
            String[] numWithFile = iter.next().toString().split("/");
            fileSet.add(numWithFile[1]);
            wordSum += Integer.parseInt(numWithFile[0]);
            invertedIdx.append(numWithFile[1]).append(": ").append(numWithFile[0]);
            if (iter.hasNext()) {
                invertedIdx.append("; ");
            }
        }
        double avgFrequency = (double)wordSum / fileSet.size();
        String outValue = formatter.format(avgFrequency)+", "+ invertedIdx;
        context.write(new Text("["+key.toString()+"]"), new Text(outValue));
    }
}
