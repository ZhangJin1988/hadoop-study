package day05.cn.edu360.mr.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Flow2 {

	public static class Flow2Mapper extends Mapper<Text, FlowBean, FlowBean, NullWritable> {
		@Override
		protected void map(Text key, FlowBean value, Mapper<Text, FlowBean, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {

			context.write(value, NullWritable.get());

		}

	}

	public static class Flow2Reducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {

		@Override
		protected void reduce(FlowBean key, Iterable<NullWritable> values,
				Reducer<FlowBean, NullWritable, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Flow2.class);

		job.setMapperClass(Flow2Mapper.class);
		job.setReducerClass(Flow2Reducer.class);
		job.setPartitionerClass(RangePartitioner.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(2);

		FileInputFormat.setInputPaths(job, new Path("F:\\mrdata\\flow\\out1"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\mrdata\\flow\\out3"));

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}

}
