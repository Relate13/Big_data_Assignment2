package org.wwhy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIdxCombiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (Text v : values) {
            sum += Integer.parseInt(v.toString());
        }
        String[] wordWithFile = key.toString().split("/");
        context.write(new Text(wordWithFile[0]), new Text(sum +"/"+wordWithFile[1]));
    }
}
