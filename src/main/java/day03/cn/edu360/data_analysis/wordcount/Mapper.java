package day03.cn.edu360.data_analysis.wordcount;

import org.apache.hadoop.fs.FSDataOutputStream;

public interface Mapper {
	
	public void map(String line, FSDataOutputStream out0, FSDataOutputStream out1) throws Exception;
}
