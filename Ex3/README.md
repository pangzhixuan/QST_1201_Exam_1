题目3：编写MapReduce，统计这两个文件

`/user/hadoop/mapred_dev_double/ip_time`

`/user/hadoop/mapred_dev_double/ip_time_2`

当中，重复出现的IP的数量(40分)

---
加分项：

1. 写出程序里面考虑到的优化点，每个优化点+5分。
2. 额外使用Hive实现，+5分。
3. 额外使用HBase实现，+5分。

 这段代码是计算完两个文件去重后的ip数量，在第二题的部分可以计算出两个文件的去重ip数，用两个文件的各自去重数的和减去两个文件一起的去重数即可
package com.ceshi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DoubleCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(DoubleCount.class);
		
		job.setMapperClass(RCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(RCReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]),new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
   
	public static class RCMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Set<String> set1 = new HashSet<String>();
        private Set<String> set2 = new HashSet<String>();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			FileSplit fileSplit=(FileSplit) context.getInputSplit();
			String path=fileSplit.getPath().toString();
            if(path.contains("ip_time")){
            	set1.add(fields[0]);
            }else{
            	set2.add(fields[0]);
            }
		}
		
		private Text k = new Text();
		private Text v = new Text();
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Iterator it = set1.iterator();
			while(it.hasNext()){
				k.set("one");
				v.set(it.next().toString());
				context.write(k, v);
			}
			Iterator it2 = set2.iterator();
			while(it2.hasNext()){
				k.set("one");
				v.set(it2.next().toString());
				context.write(k, v);
			}
		}
		
	}
	
	public static class RCReducer extends Reducer<Text,Text,LongWritable,NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,LongWritable,NullWritable>.Context context)
				throws IOException, InterruptedException {
			    Set<String> set = new HashSet<String>();
			    Iterator it = values.iterator();
			    
			    while(it.hasNext()){
			    	 set.add(it.next().toString());
			     }
			     context.write(new LongWritable(set.size()), NullWritable.get());
		}
	}
}
