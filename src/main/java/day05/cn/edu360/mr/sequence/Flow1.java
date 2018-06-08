package day05.cn.edu360.mr.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Flow1 {
	
	public static class Flow1Mapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		FlowBean bean = new FlowBean();
		Text k = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			
			String[] split = value.toString().split("\t");
			bean.set(split[1],Long.parseLong(split[split.length-3]), Long.parseLong(split[split.length-2]));
			k.set(split[1]);
			context.write(k, bean);
		}
	}
	
	public static class Flow1Reducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		FlowBean bean = new FlowBean();
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			
			long upAmount = 0;
			long dAmount = 0;
			for (FlowBean flowBean : values) {
				
				upAmount += flowBean.getUpFlow();
				dAmount += flowBean.getdFlow();
			}
			
			bean.set(key.toString(),upAmount, dAmount);
			context.write(key, bean);
		}
		
		
		
		
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Flow1.class);

		job.setMapperClass(Flow1Mapper.class);
		job.setReducerClass(Flow1Reducer.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path("F:\\mrdata\\flow\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\mrdata\\flow\\out1"));

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
	

}
