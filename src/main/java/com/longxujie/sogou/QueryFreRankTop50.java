package com.longxujie.sogou;


import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

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


public class QueryFreRankTop50 extends Configured implements Tool {

    public static class QueryFreRankMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        private Text okey=new Text();
        private LongWritable ovalue=new LongWritable(1L);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line=value.toString();
            String[] lineSplited=line.split("\t");
            String keyword=lineSplited[2];
            if(!"".equals(keyword) || keyword!=null) {
                okey.set(keyword);
                context.write(okey, ovalue);
            }

        }
    }

    public static class QueryFreRankReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
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

    public static class Top50Mapper extends Mapper<LongWritable, Text, LongWritable, Text>{
        private static final int K=50;
        private TreeMap<Long, String> tm=new TreeMap<Long, String>();
        private LongWritable okey=new LongWritable();
        private Text ovalue=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line=value.toString();
            String[] lineSplited=line.split("\t");
            String keyword=lineSplited[0];
            long count=Long.valueOf(lineSplited[1].trim());
            tm.put(count, keyword);
            if(tm.size() > K) {
                tm.remove(tm.firstKey());
            }

        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
                throws IOException, InterruptedException {
            for(Map.Entry<Long,String> entry:tm.entrySet()) {
                long count=entry.getKey();
                String keyword=entry.getValue();
                okey.set(count);
                ovalue.set(keyword);
                context.write(okey, ovalue);

            }

        }
    }

    public static class Top50Reducer extends Reducer<LongWritable, Text, Text, LongWritable>{
        private LongWritable ovalue=new LongWritable();
        private Text okey=new Text();

        private static final int K=50;
        private TreeMap<Long, String> tm=new TreeMap<Long, String>(new Comparator<Long>() {

            @Override
            public int compare(Long o1, Long o2) {
                return o2.compareTo(o1);
            }

        });

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values) {
                tm.put(key.get(), value.toString());
                if(tm.size() > K) {
                    tm.remove(tm.firstKey());
                }
            }
        }

        @Override
        protected void cleanup(Reducer<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            for(Map.Entry<Long, String> entry:tm.entrySet()) {
                String keyword=entry.getValue();
                long count=entry.getKey();
                okey.set(keyword);
                ovalue.set(count);
                context.write(okey, ovalue);
            }
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.201:8020");
        Job job1=Job.getInstance(conf);

        job1.setJarByClass(QueryFreRankTop50.class);
        FileInputFormat.addInputPath(job1, new Path("/mr/analysis_sougoulog/data/sogou_1000_log.txt.flt"));
        job1.setMapperClass(QueryFreRankMapper.class);
        job1.setReducerClass(QueryFreRankReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job1, new Path("/mr/analysis_sougoulog/data/sogou_queryFreRank"));
        job1.waitForCompletion(true);

        Job job2=Job.getInstance(conf);

        job2.setJarByClass(QueryFreRankTop50.class);
        FileInputFormat.addInputPath(job2, new Path("/mr/analysis_sougoulog/data/sogou_queryFreRank"));
        job2.setMapperClass(Top50Mapper.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(Top50Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);


        FileOutputFormat.setOutputPath(job2, new Path("/mr/analysis_sougoulog/data/5_QueryFreRankTop50"));
        return job2.waitForCompletion(true)? 0:1;
    }

    public static void main(String[] args) throws Exception {
        int res=ToolRunner.run(new QueryFreRankTop50(), args);
        System.exit(res);
    }

}