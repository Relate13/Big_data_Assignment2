package org.wwhy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

// 输入 <忽略, 文档文本>
// 输出 <词语/文档名, 1>
public class InvertedIdxMapper extends Mapper<Object, Text, Text, Text> {
    // 表示单词数量为1
    private static final Text valOne = new Text("1");
    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 将文本按照空白符划分为token，每个token为一个词语
        StringTokenizer iter = new StringTokenizer(value.toString());
        // 获取当前文档的文件名
        FileSplit split = (FileSplit) context.getInputSplit();
        String filename = split.getPath().getName();
        Text word = new Text();
        while (iter.hasMoreTokens()) {
            // 对于每一个词语，合并词语和文件名作为key，词语和文件名用/分隔
            word.set(iter.nextToken()+"/"+filename);
            // value为1，表示单词出现了1次
            context.write(word, valOne);
        }
    }
}

