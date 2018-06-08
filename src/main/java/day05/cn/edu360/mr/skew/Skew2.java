package day05.cn.edu360.mr.skew;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Skew2 {
	
	public static class Skew2Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			context.write(new Text(split[0].split("-")[0]), new IntWritable(Integer.parseInt(split[1])));
		}
	}

	
	public static class Skew2Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int count =0;
			for (IntWritable v : values) {
				count += v.get();
			}
			
			context.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Skew2.class);

		job.setMapperClass(Skew2Mapper.class);
		job.setReducerClass(Skew2Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("F:\\mrdata\\skew\\ot1"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\mrdata\\skew\\ot2"));

		job.setNumReduceTasks(1);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
}
