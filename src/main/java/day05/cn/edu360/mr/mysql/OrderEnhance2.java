package day05.cn.edu360.mr.mysql;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderEnhance2 {

	public static class OrderEnhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		HashMap<String, User> map = new HashMap<>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			UserDataLoader.loadUserData(map);

		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			context.write(new Text(value.toString()+","+map.get(split[0])), NullWritable.get());
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(OrderEnhance2.class);

		job.setMapperClass(OrderEnhanceMapper.class);

		// 不需要reducetask阶段
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path("F:\\mrdata\\orderdata\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\mrdata\\orderdata\\output2"));

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}

}
