package day05.cn.edu360.mr.distinct;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Distinct {
	
	public static class DistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		Text k = new Text();
		NullWritable v = NullWritable.get();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			k.set(split[0]);
			context.write(k,v);
		}
	}
	
	public static class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
		
		
	}

}
