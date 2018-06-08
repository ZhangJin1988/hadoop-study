package day04.cn.edu360.mr.movie_topn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

public class MovieRateTopn {

	public static class MovieRateTopnMapper extends Mapper<LongWritable, Text, MovieBean, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, MovieBean, NullWritable>.Context context)
				throws IOException, InterruptedException {

			try {
				String json = value.toString();
				ObjectMapper objectMapper = new ObjectMapper();
				MovieBean bean = objectMapper.readValue(json, MovieBean.class);
				context.write(bean, NullWritable.get());
			} catch (Exception e) {
				
			}
		}

	}

	public static class MovieRateTopnReducer extends Reducer<MovieBean, NullWritable, MovieBean, NullWritable> {

		@Override
		protected void reduce(MovieBean key, Iterable<NullWritable> values,
				Reducer<MovieBean, NullWritable, MovieBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			int topn = 2;
			for (NullWritable v : values) {
				context.write(key, v);
				if (--topn == 0)
					return;
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(MovieRateTopn.class);

		job.setMapperClass(MovieRateTopnMapper.class);
		job.setReducerClass(MovieRateTopnReducer.class);

		// 指定分区组件（maptask用）
		job.setPartitionerClass(MovieIdPartitioner.class);
		// 指定分组组件（reducetask用）
		job.setGroupingComparatorClass(MovieIdGroupingComparator.class);

		job.setMapOutputKeyClass(MovieBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		// 判断输出目录是否已存在，如果已存在则删除
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(output)) {
			fs.delete(output, true);
		}
		
		InputFormat in = null;
		
		FileOutputFormat.setOutputPath(job, output);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}

}
