
题目2：编写MapReduce，统计`/user/hadoop/mapred_dev/ip_time` 中去重后的IP数，越节省性能越好。（35分）

---

运行完之后，描述程序里所做的优化点，每点+5分。
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class UVCount1201 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(UVCount1201.class);
		
		job.setMapperClass(DCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
   
	public static class DCMapper extends Mapper<LongWritable,Text,Text,Text>{
		private Set<String> set = new HashSet<String>();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			set.add(fields[0]);
		}

		private Text k = new Text();
		private Text v = new Text();
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Iterator it = set.iterator();
			while(it.hasNext()){
				k.set("one");
				v.set(it.next().toString());
				context.write(k, v);
			}
		}
		
	}
	
	public static class DCReducer extends Reducer<Text,Text,LongWritable,NullWritable>{
		private Set<String> set = new HashSet<String>();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,LongWritable,NullWritable>.Context context)
				throws IOException, InterruptedException {
		     Iterator it = values.iterator();
		     while(it.hasNext()){
		    	 set.add(it.next().toString());
		     }
		     context.write(new LongWritable(set.size()), NullWritable.get());
		}
		
	}
}
优化 ：在map端的cleanup进行去重输出
