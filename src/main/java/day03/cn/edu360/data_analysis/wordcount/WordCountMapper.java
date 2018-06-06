package day03.cn.edu360.data_analysis.wordcount;

import org.apache.hadoop.fs.FSDataOutputStream;

public class WordCountMapper implements Mapper{

	@Override
	public void map(String line, FSDataOutputStream out0, FSDataOutputStream out1) throws Exception {
		String[] words = line.split(" ");
		for (String word : words) {
			if(word.hashCode()%2 == 0) {
				out0.write((word+",1\n").getBytes());
			}else {
				out1.write((word+",1\n").getBytes());
			}
		}
		
	}

}
