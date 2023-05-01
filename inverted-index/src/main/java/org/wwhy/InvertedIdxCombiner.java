package org.wwhy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// 对本地节点Map的结果进行预聚合
// 输入 <词语/文档名, [词频]>
// 输出 <词语, 词频/文档名>
public class InvertedIdxCombiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 统计单词在某一文档中出现的次数
        int sum = 0;
        for (Text v : values) {
            sum += Integer.parseInt(v.toString());
        }
        // 从key中取得词语和文档名
        String[] wordWithFile = key.toString().split("/");
        // 将词语作为新的key，将词频和文档名合并作为新的value
        context.write(new Text(wordWithFile[0]), new Text(sum +"/"+wordWithFile[1]));
    }
}
