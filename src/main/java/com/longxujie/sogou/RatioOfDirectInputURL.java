package com.longxujie.sogou;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RatioOfDirectInputURL extends Configured implements Tool {

    public static class RatioOfDirectInputURLMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Text okey=new Text("SubInputURLPerMapper");
        private LongWritable ovalue=new LongWritable(1L);
        String pattern=".*www.*";
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line=value.toString();
            String[] lineSplited=line.split("\t");
            String keyword=lineSplited[2];
            if(Pattern.matches(pattern, keyword)) {
                context.write(okey, ovalue);
            }
        }
    }

    public static class RatioOfDirectInputURLReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        private LongWritable ovalue=new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum=0;
            for(LongWritable value:values) {
                sum +=value.get();
            }
            ovalue.set(sum);
            context.write(key, ovalue);
        }
    }

    public static class UserDutyThanTwoMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Text okey=new Text("subInputURLPer");
        private LongWritable ovalue=new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line=value.toString();
            String[] lineSplited=line.split("\t");
            @SuppressWarnings("unused")
            String word=lineSplited[0];
            long count=Long.parseLong(lineSplited[1]);
            ovalue.set(count);
            context.write(okey, ovalue);
        }
    }

    public static class UserDutyThanTwoReducere extends Reducer<Text, LongWritable, Text, DoubleWritable>{

        private DoubleWritable percent=new DoubleWritable();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            StringBuffer buffer=new StringBuffer();
            for(LongWritable value:values) {
                buffer.append(value).append(",");
            }
            String[] moleculeOrDenominator=buffer.toString().split(",");
            double a=Double.valueOf(moleculeOrDenominator[0]);
            double b=Double.valueOf(moleculeOrDenominator[1]);
            double per=0.0;
            if(a<=b) {
                per=a/b;
            }else {
                per=b/a;
            }
            percent.set(per);
            context.write(key, percent);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.201:8020");
        Job job1=Job.getInstance(conf);

        job1.setJarByClass(RatioOfDirectInputURL.class);
        FileInputFormat.addInputPath(job1, new Path("/mr/analysis_sougoulog/data/sogou_log.txt.flt"));
        job1.setMapperClass(RatioOfDirectInputURLMapper.class);
        job1.setReducerClass(RatioOfDirectInputURLReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job1, new Path("/mr/analysis_sougoulog/data/sogou_subInputURLPer"));
        job1.waitForCompletion(true);


        Job job2=Job.getInstance(conf);
        job2.setJarByClass(RatioOfDirectInputURL.class);
        MultipleInputs.addInputPath(job2, new Path("/mr/analysis_sougoulog/data/sogou_subInputURLPer"),
                TextInputFormat.class, UserDutyThanTwoMapper.class);
        MultipleInputs.addInputPath(job2, new Path("/mr/analysis_sougoulog/data/sogou_numberOfRankTen"),
                TextInputFormat.class, UserDutyThanTwoMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setReducerClass(UserDutyThanTwoReducere.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileOutputFormat.setOutputPath(job2, new Path("/mr/analysis_sougoulog/data/9_RatioOfDirectInputURL"));
        return job2.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res=ToolRunner.run(new RatioOfDirectInputURL(), args);
        System.exit(res);
    }
}