package day04.cn.edu360.mr.movie_topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MovieIdPartitioner extends Partitioner<MovieBean, NullWritable>{

	@Override
	public int getPartition(MovieBean key, NullWritable value, int numReduceTasks) {
		
		return (key.getMovie().hashCode() & Integer.MAX_VALUE ) % numReduceTasks;
	}

}
