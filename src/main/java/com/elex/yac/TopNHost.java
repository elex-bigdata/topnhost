package com.elex.yac;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TopNHost extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(args.length != 1){
			System.err.println("请输入topn参数！！！");
			System.exit(1);
		}
		
		ToolRunner.run(new Configuration(), new TopNHost(), args);
		ToolRunner.run(new Configuration(), new RawDataCollector(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"topN-host");
		
		conf.setInt("topN", Integer.parseInt(args[0]));
		job.setJarByClass(TopNHost.class);		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
		Path in = new Path("/yac/ton_host/raw");
		FileInputFormat.addInputPath(job, in);
		
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.addNamedOutput(job, "nation", TextOutputFormat.class, Text.class, Text.class);
		
		String output = "/yac/ton_host/topN";
		HdfsUtils.delFile(fs, output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)?0:1;
	}
		
	public static class MyMapper extends Mapper<Text, IntWritable, Text, Text> {

		private String[] nation_host;
		
		@Override
		protected void map(Text key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			nation_host = key.toString().split(",");
			
				context.write(new Text(nation_host[0]), new Text(nation_host[1]+Integer.toString(value.get())));
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private List<Pair<String,Integer>> list = new ArrayList<Pair<String,Integer>>();
		private String nation;
		private String[] kv;
		private int count,topN;
		private MultipleOutputs<Text, Text> nt;  
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			nt = new MultipleOutputs<Text, Text>(context);
			topN = context.getConfiguration().getInt("topN", 100);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			nation = key.toString();
			for(Text v:values){
				kv = v.toString().split(",");
				count = Integer.parseInt(kv[1]);
				if(count > 100){
					Pair<String,Integer> host = new Pair<String,Integer>(kv[0],count);
					list.add(host);
				}
			}
			
			Collections.sort(list, new Comparator<Pair<String,Integer>>() {
	            //降序排序
				@Override
	            public int compare(Pair<String,Integer> o1,
	            		Pair<String,Integer> o2) {
	                return o2.getSecond().compareTo(o1.getSecond());
	            }	            
	        });
			
			nt.write("nation", new Text(nation), null);
			
			topN=list.size()>topN?topN:list.size();
			for(int i=0;i<topN;i++){
				context.write(null, new Text(nation+","+list.get(i).getFirst()+","+list.get(i).getSecond().toString()));
			}
			
								
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,InterruptedException {
			nt.close();// 释放资源
		}
		
	}		
}
