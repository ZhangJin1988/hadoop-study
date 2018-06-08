package day05.cn.edu360.mr.sequence;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RangePartitioner extends Partitioner<FlowBean, NullWritable> {

	@Override
	public int getPartition(FlowBean key, NullWritable value, int numReduceTasks) {
		
		
		return key.getSumFlow()>7000L?1:0;
	}
	
	
		
}
