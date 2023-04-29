package org.wwhy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] remainArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainArgs.length != 2) {
            System.err.println("Wrong Usage. Usage: <command> <in> <out>");
            System.exit(1);
        }
        Job job = Job.getInstance(conf, "Inverted-Index");
        job.setJarByClass(Main.class);
        job.setMapperClass(InvertedIdxMapper.class);
        job.setCombinerClass(InvertedIdxCombiner.class);
        job.setReducerClass(InvertedIdxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(remainArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }

}
