package org.wwhy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

// 输入 <词语, [词频/文档名]>
// 输出 <词语, 平均出现次数+[文档名: 词频]>
public class InvertedIdxReducer extends Reducer<Text, Text, Text, Text> {
    // 用于格式化词语平均出现次数，保留两位小数
    private static final DecimalFormat formatter = new DecimalFormat("0.00");
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 用于统计有key出现的文件数量
        Set<String> fileSet = new HashSet<>();
        // 用于统计key出现的总数
        int wordSum = 0;
        // 倒排索引，格式为<文件名: 词频; ...>
        StringBuilder invertedIdx = new StringBuilder();
        // 遍历所有value
        Iterator<Text> iter = values.iterator();
        while (iter.hasNext()) {
            // 从value中取得在Combine阶段写入的文件名和词频
            String[] numWithFile = iter.next().toString().split("/");
            // 将文件名加入set，利用set的不重复特性统计文件数量
            fileSet.add(numWithFile[1]);
            // 统计词语出现总数
            wordSum += Integer.parseInt(numWithFile[0]);
            // 写入倒排索引
            invertedIdx.append(numWithFile[1]).append(": ").append(numWithFile[0]);
            // 如果是最后一个索引，则不写分号（与实验文档保持一致）
            if (iter.hasNext()) {
                invertedIdx.append("; ");
            }
        }
        // 计算平均出现次数
        double avgFrequency = (double)wordSum / fileSet.size();
        // 保留两位小数，并将其拼接到倒排索引之前作为value
        String outValue = formatter.format(avgFrequency)+", "+ invertedIdx;
        // 输出条目，格式为<[词语], 平均出现次数, 文档名: 词频; ...>
        context.write(new Text("["+key.toString()+"]"), new Text(outValue));
    }
}

