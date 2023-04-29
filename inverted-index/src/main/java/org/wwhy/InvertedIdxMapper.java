package org.wwhy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIdxMapper extends Mapper<Object, Text, Text, Text> {
    private static final Text valOne = new Text("1");

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        StringTokenizer iter = new StringTokenizer(value.toString());
        FileSplit split = (FileSplit) context.getInputSplit();
        String filename = split.getPath().getName();
        Text word = new Text();
        while (iter.hasMoreTokens()) {
            word.set(iter.nextToken()+"/"+filename);
            context.write(word, valOne);
        }
    }
}
