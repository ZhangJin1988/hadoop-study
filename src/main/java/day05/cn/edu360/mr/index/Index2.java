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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import day05.cn.edu360.mr.index.Index1.Index1Mapper;
import day05.cn.edu360.mr.index.Index1.Index1Reducer;

public class Index2 {

	public static class Index2Mapper extends Mapper<LongWritable, Text, Text, Text> {
		Text k = new Text();
		Text v = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			// hello-a.html 4
			String[] split = value.toString().split("-");
			k.set(split[0]);
			v.set(split[1].replaceAll("\t", "-->"));

			context.write(k, v);
		}
	}

	public static class Index2Reducer extends Reducer<Text, Text, Text, Text> {
		Text v = new Text();

		@Override
		protected void reduce(Text word, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			for (Text text : values) {
				sb.append(text.toString()).append(" ");
			}

			v.set(sb.toString().trim());
			context.write(word, v);

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Index2.class);

		job.setMapperClass(Index2Mapper.class);
		job.setReducerClass(Index2Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//如果输出的最终结果不是文本文件，而是hadoop序列化文件：SequenceFile，则必须告诉reducetask，我们的输出keyvalue的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.setInputPaths(job, new Path("F:\\mrdata\\index\\out1"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\mrdata\\index\\out2"));

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}

}
