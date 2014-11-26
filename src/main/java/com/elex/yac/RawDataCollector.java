package com.elex.yac;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RawDataCollector extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RawDataCollector(), args);
	}

	public int run(String[] args) throws Exception {
		long days = Long.parseLong(args[1]);
		Configuration conf = new Configuration();
		conf.set("mapred.job.priority", JobPriority.LOW.toString());
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"raw-data-collector");
		job.setJarByClass(RawDataCollector.class);
		long now = System.currentTimeMillis();
	    long before = now - Long.valueOf(days*24L*60L*60L*1000L);
 
	    List<Scan> scans = new ArrayList<Scan>(); 
	    Scan uaScan = new Scan();
	    uaScan.setStartRow(Bytes.add(new byte[]{1}, Bytes.toBytes(before)));
	    uaScan.setStopRow(Bytes.add(new byte[]{1}, Bytes.toBytes(now)));
	    uaScan.setCaching(1000);
	    uaScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("yac_user_action"));
	    uaScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("nt"));
	    uaScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("url"));
		scans.add(uaScan);
		
		TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class,Text.class, IntWritable.class, job);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(Integer.parseInt(Long.toString(days))*5);
		//job.setPriority(JobPriority.LOW);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		String output = "/yac/ton_host/raw";
		HdfsUtils.delFile(fs, output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)?0:1;
	}
		
	
	public static class MyMapper extends TableMapper<Text, IntWritable> {
		private String uid,nation,url,host;
		private IntWritable val = new IntWritable();
		
		@Override
		protected void map(ImmutableBytesWritable key, Result r,Context context) throws IOException, InterruptedException {
			if (!r.isEmpty()) {
				uid = Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-9));
								
				
				for (KeyValue kv : r.raw()) {										
					if ("ua".equals(Bytes.toString(kv.getFamily()))&& "nt".equals(Bytes.toString(kv.getQualifier()))) {
						nation = Bytes.toString(kv.getValue());
					}
					if ("ua".equals(Bytes.toString(kv.getFamily()))&& "url".equals(Bytes.toString(kv.getQualifier()))) {
						url = Bytes.toString(kv.getValue());
						try{
							if(url.startsWith("http") || url.startsWith("https")){
								host = new URL(url).getHost();
							}else{
								host = new URL("http://"+url).getHost();
							}
						}catch(MalformedURLException e){
							System.err.println("MalformedURLException:"+url);
						}
						
						
					}
				}
				
				if(host != null && nation != null){
					val.set(getIntFromStr(uid));
					context.write(new Text(nation.replace(",", "")+","+host.replace(",", "")), val);
				}				
								
			}
											
		}
		
		
		public static int getIntFromStr(String str){
			int seed = 131; // 31 131 1313 13131 131313 etc..
			int hash = 0;
			for (int i = 0; i < str.length(); i++) {
				hash = (hash * seed) + str.charAt(i);
			}
			return (hash & 0x7FFFFFFF);
			
		}
		
					
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private Set<Integer> uidSet = new HashSet<Integer>();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			uidSet.clear();				
			
			for(IntWritable v:values){				
				uidSet.add(v.get());				
			}
						
			context.write(key, new IntWritable(uidSet.size()));
						
		}	
	}
}
