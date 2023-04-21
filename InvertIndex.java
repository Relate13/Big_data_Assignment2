import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * @author Relate13
 * @description 文档倒排索引算法
 */

public class InvertIndex {
    /**
     * @param args : 接受两个字符串参数，第一个参数为文件输入地址，第二个输入为结果输出地址
     */
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        //获取任务控制
        Job job = Job.getInstance();
        //设置Mapper,Combiner,Reducer
        job.setJarByClass(InvertIndex.class);
        job.setMapperClass(InvertIndexMapper.class);
        job.setCombinerClass(InvertIndexCombiner.class);
        job.setReducerClass(InvertIndexReducer.class);
        //设置Context格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入与输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //等待执行并退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * @description Mapper类，将输入Text映射为 Key:Text,Value:Text
     */
    public static class InvertIndexMapper extends Mapper<Object, Text, Text, Text> {
        /**
         * 关键词与文件名
         */
        private static final Text wordWithURL = new Text();
        /**
         * uno表示出现一次
         */
        private static final Text uno = new Text("1");

        /**
         * @param key     :
         * @param value   : mapper 分配到的文本片段
         * @param context : 用于传递信息
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //按空格将文本片段分割为词组
            String[] words = value.toString().split(" ");
            //对每个词组生成（“词组/URL”，uno），传送此此键值对给Combiner
            for (String word : words) {
                wordWithURL.set(word + "/" + ((FileSplit) context.getInputSplit()).getPath().getName());
                context.write(wordWithURL, uno);
            }
        }
    }

    /**
     * @description Combiner类，统计某关键词在某文件中出现的次数，并把关键词作为key，文件url与出现次数作为值传递给Reducer
     */
    public static class InvertIndexCombiner extends Reducer<Text, Text, Text, Text> {

        /**
         * 存储聚合后的结果，是一个由 url 与 关键词在对应url文件中出现的次数 组成的字符串
         */
        private static final Text urlWithOccurrence = new Text();

        /**
         * @param key     : “词组/URL”信息
         * @param values  : 一组uno，数量为对应词组在对应URL文件中出现次数
         * @param context : 用于传递信息
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //values的数量即为单词在对应url文件中出现的次数
            int occurrence = 0;
            for (Text value : values)
                ++occurrence;
            //提取关键词，并将其作为key
            String[] wordAndUrl = key.toString().split("/");
            key.set(wordAndUrl[0]);
            //设置value，由 url 与 关键词在对应url文件中出现的次数 组成
            urlWithOccurrence.set(wordAndUrl[1] + ":" + occurrence);
            //传送键值对给Reducer
            context.write(key, urlWithOccurrence);
        }
    }

    /**
     * @description Reducer类，统计关键词在各文件中出现的次数并计算词频，得到结果
     */
    public static class InvertIndexReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * 存储reduce后的结果，是一串由 文件url 与 关键词在对应文件中出现次数 组成的字符串
         */
        private static final Text outputValue = new Text();
        /**
         * 用于浮点数格式化
         */
        private static final DecimalFormat formatter = new DecimalFormat("0.00");

        /**
         * @param key     : 某个关键词
         * @param values  : 一组 关键词在某文件中出现次数 信息，数量为有对应关键词出现的文件数量
         * @param context : 用于传递信息
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //统计关键词在任务中的出现总数 与 有对应关键词出现的文件数量，便于计算词频信息
            int sum = 0, passage = 0;
            //拼接 关键词在某文件中出现次数 字符串, 并统计关键词出现总数与出现文件数量
            StringBuilder urlWithOccurrenceList = new StringBuilder();
            for (Text value : values) {
                ++passage;
                String urlWithOccurrence = value.toString();
                sum += Integer.parseInt(urlWithOccurrence.split(":")[1]);
                urlWithOccurrenceList.append(urlWithOccurrence).append("; ");
            }
            //计算并格式化词频为两位浮点数
            float frequency = (float) sum / (float) passage;
            outputValue.set(formatter.format(frequency) + ", " + urlWithOccurrenceList);
            //传递结果
            context.write(key, outputValue);
        }
    }
}
