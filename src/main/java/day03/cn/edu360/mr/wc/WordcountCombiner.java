package day03.cn.edu360.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * map task调此类
 * @author hunter.d
 * @create_time 2018年4月14日
 * @copyright www.edu360.cn
 */
public class WordcountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
	
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
