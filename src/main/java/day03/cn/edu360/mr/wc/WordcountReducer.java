package day03.cn.edu360.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	// 谁调reduce方法？ --》 reduce task程序
	// 它怎么调？ -->它对每一组key-value数据调一次
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		
		context.write(key, new IntWritable(count));
	}
	

}
