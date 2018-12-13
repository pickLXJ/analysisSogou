package com.longxujie.sogou;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class QueryTotalNumber extends Configured implements Tool {

    public static class QueryTotalNumberMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Text okey=new Text("QueryTotalNumber");
        private LongWritable ovalue=new LongWritable(1L);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line=value.toString();
            if(!"".equals(line)) {
                context.write(okey, ovalue);
            }

        }
    }

    public static class QueryTotalNumberReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        private LongWritable ovalue=new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum=0;
            for(LongWritable value:values) {
                sum +=value.get();
            }
            ovalue.set(sum);
            context.write(key, ovalue);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=new Configuration();
        //远程调试必须加上
        conf.set("fs.defaultFS", "hdfs://192.168.2.201:8020");

        Job job=Job.getInstance(conf,"SogouLogCount");
        job.setJarByClass(QueryTotalNumber.class);

        FileInputFormat.addInputPath(job, new Path("/mr/analysis_sougoulog/data/sogou_1000_log.txt.flt"));
        job.setMapperClass(QueryTotalNumberMapper.class);
        job.setReducerClass(QueryTotalNumberReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job, new Path("/mr/analysis_sougoulog/data/1_QueryTotalNumber"));
        return job.waitForCompletion(true)? 0:1;
    }

    public static void main(String[] args) throws Exception {
        int res=ToolRunner.run(new QueryTotalNumber(), args);
        System.exit(res);
    }

}