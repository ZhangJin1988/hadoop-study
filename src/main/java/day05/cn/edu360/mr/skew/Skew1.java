package day05.cn.edu360.mr.skew;

import java.io.IOException;
import java.util.Random;

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

/**
 * 处理数据倾斜的核心思想：将key分类打散进行分类聚合；然后再进行全局聚合；
 * @author hunter.d
 * @create_time 2018年4月14日
 * @copyright www.edu360.cn
 */
public class Skew1 {

	public static class Skew1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		Random random = null;
		int reduceTasks = 0;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			random = new Random();
			reduceTasks = context.getNumReduceTasks();
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String[] split = value.toString().split(" ");
			for (String word : split) {
				context.write(new Text(word + "-" + random.nextInt(reduceTasks)), new IntWritable(1));

			}

		}

	}

	public static class Skew1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
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
		job.setJarByClass(Skew1.class);

		job.setMapperClass(Skew1Mapper.class);
		job.setReducerClass(Skew1Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("F:\\mrdata\\skew\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\mrdata\\skew\\ot1"));

		job.setNumReduceTasks(3);
		
		
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
}
