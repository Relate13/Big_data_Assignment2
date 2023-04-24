package InvertIndex;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;


public class InvertIndex {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "invert index");
            job.setJarByClass(InvertIndex.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Word.class);
            job.setOutputValueClass(Info.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class Word implements WritableComparable<Word> {
    private String word;

    public Word() {

    }

    public Word(String  word) {
        this.word = word;
    }

    public String getWord() {
        return word;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('[');
        stringBuilder.append(word.toString());
        stringBuilder.append("]\t");
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Word)
            return this.word.equals(((Word)o).getWord());
        return false;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //word.write(dataOutput);
        dataOutput.writeUTF(word);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word = dataInput.readUTF();
    }


    @Override
    public int compareTo(Word o) {
        String thisValue = word;
        String thatValue = o.word;
        return thisValue.compareTo(thatValue);
    }

}

class Info implements WritableComparable<Info> {
    private String emerge;
    private int times;
    private int file_num;
    private HashSet<String> files;

    public Info() {
        emerge = new String();
        times = 0;
        file_num = 0;
        files = new HashSet<String>();
    }

    public Info(String emerge, int times, HashSet<String> files) {
        this.emerge = emerge;
        this.times = times;
        this.files = new HashSet<String>(){{addAll(files);}};
        this.file_num = files.size();
    }

    public int getTimes() {
        return times;
    }

    public String getEmerge() {
        return emerge;
    }

    public HashSet<String> getFiles() {
        return files;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        double average = (double) times / (double) file_num;
        //double average = (double) files.size();
        stringBuilder.append(Double.toString(average));
        stringBuilder.append(", ");
        stringBuilder.append(emerge.toString());
        return stringBuilder.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //word.write(dataOutput);
        dataOutput.writeUTF(emerge);
        dataOutput.writeInt(times);
        dataOutput.writeInt(file_num);
        for(String s: files)
            dataOutput.writeUTF(s);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //dataInput.readFully(this.word.getBytes());
        emerge = dataInput.readUTF();
        times = dataInput.readInt();
        file_num = dataInput.readInt();
        for (int i = 0; i < file_num; ++i) {
            String s = dataInput.readUTF();
            files.add(s);
        }
    }


    @Override
    public int compareTo(Info o) {
        return times < o.times? -1:(times == o.times?0: 1);
    }
}


class MyMapper extends Mapper<LongWritable, Text, Word, Info> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        Text word = new Text();
        Text offset = new Text(fileName + ":" + key.toString());
        StringTokenizer itr = new StringTokenizer(value.toString());
        for (; itr.hasMoreTokens();) {
            word.set(itr.nextToken());
            context.write(new Word(word.toString()), new Info(offset.toString(), 1, new HashSet<String>(){{add(fileName);}}));
        }
    }
}

class MyReducer extends Reducer<Word, Info, Word, Info> {
    HashSet<String> all_files = new HashSet<String>();
    @Override
    protected void reduce(Word key, Iterable<Info> values, Context context)
            throws IOException, InterruptedException {
        Iterator<Info> itr = values.iterator();
        StringBuilder all = new StringBuilder();
        int times = 0;


        if (itr.hasNext()) {
            Info tmp = itr.next();
            all.append(tmp.getEmerge().toString());
            all_files.addAll(tmp.getFiles());
            times += tmp.getTimes();
        }
        for (; itr.hasNext();) {
            Info tmp = itr.next();
            all.append("; ");
            all.append(tmp.getEmerge().toString());
            all_files.addAll(tmp.getFiles());
            times += tmp.getTimes();
        }
        context.write(key, new Info(all.toString(), times, all_files));
        all_files.clear();
    }
}
