

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;


//made in niu
public class Sort
{
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        //获取任务控制
        Job job = Job.getInstance();

        //设置partitioner的相关配置
        //1随机采样方法
        InputSampler.RandomSampler<Text,Text>sampler=new InputSampler.RandomSampler<>(0.1,10000,10);

        //设置配置文件
        Path partitionFile = new Path( "_partitions");
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);




        //设置Mapper,Combiner,Reducer
        job.setJarByClass(Sort.class);
        job.setMapperClass(SortMapper.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setReducerClass(SortReducer.class);

        //设置Context格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置输入与输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //等待执行并退出
        boolean flag=job.waitForCompletion(true);
        if(flag)
        {
            System.out.println("Ok");
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {
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
            //读取relate13输出的文件的值,先按行读取
            ArrayList<ArrayList<String>> label_vals = new ArrayList<ArrayList<String>>();
            String[] records = value.toString().split("\n");
            for(String line:records)
            {
                //line :0	3.00, 第三部-阿兹卡班的囚徒:2; 第四部-火焰杯:4;

                //这里得到items：[0	3.00, 第三部-阿兹卡班的囚徒:2 | 第四部-火焰杯:4]
                String[] items=line.split(";");

                //head=0	3.00, 第三部-阿兹卡班的囚徒:2
                String head=items[0];

                //head_items=[0	3.00 |  第三部-阿兹卡班的囚徒:2]
                String[] head_items=head.split(",");

                //head_val=0	3.00
                String head_val=head_items[0];

                //value_key=[0,3.00]
                String [] value_key=head_val.split("\t");

                //new_key=3.00
                String new_key=value_key[1];

                //new_val=0
                String new_val=value_key[0];

                ArrayList<String> key_val_pair = new ArrayList<String>();
                key_val_pair.add(new_key);
                key_val_pair.add(new_val);

                label_vals.add(key_val_pair);
            }

            //设置context，传入key_val
            for(ArrayList<String> item:label_vals)
            {
                //将key值转为double类型
                double temp_key_double = Double.valueOf(item.get(0).toString());

                //double转为DoubleWritable
                DoubleWritable temp_key=new DoubleWritable();
                temp_key.set(temp_key_double);

                final Text temp_val = new Text(item.get(1));
                context.write(temp_key,temp_val);
            }
        }
    }
    public static class SortReducer extends Reducer<DoubleWritable, Text, Text, Text>
    {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {

            final Text  key_text= new Text();

            for (Text t : values)
            {
                String key_str=key.toString();
                key_text.set(key_str);
                context.write(key_text,t);
            }
        }
    }

}