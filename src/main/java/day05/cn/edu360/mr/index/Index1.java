package day05.cn.edu360.mr.index;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import day04.cn.edu360.mr.line.Line;
import day04.cn.edu360.mr.line.Line.LineMapper;
import day04.cn.edu360.mr.line.Line.LineReducer;

public class Index1 {
	
	public static class Index1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		String fileName = null;

		/**
		 * maptask或者reducetask在正式开始调用map或reduce方法之前，会先调一次setup方法来做一些数据初始化准备工作
		 */
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			fileName = inputSplit.getPath().getName();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] words = value.toString().split(" ");
			for (String word : words) {
				
				context.write(new Text(word+"-"+fileName), new IntWritable(1));
			}
			
			
		}
		
		/**
		 * maptask或者reducetask在处理完自己所负责的数据后，会调一次cleanup方法来做一些清理工作
		 */
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
		}
		
	}
	
	public static class Index1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			
			int count = 0;
			for (IntWritable v : values) {
				count += v.get();
				
			}
			context.write(key, new IntWritable(count));
			
		}
		
	}
	
public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Index1.class);

		job.setMapperClass(Index1Mapper.class);
		job.setReducerClass(Index1Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		FileInputFormat.setInputPaths(job, new Path("F:\\mrdata\\index\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\mrdata\\index\\out1"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
		
	}
}
