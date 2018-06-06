package day02.cn.edu360.hdfs.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class HdfsClientDemo2 {
	
	//  读取HDFS中的文件的内容
	@Test
	public void testReadContent() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://cts01:9000/");
		FileSystem fs = FileSystem.get(conf);
		
		FSDataInputStream in = fs.open(new Path("/qingshu.txt"));
		
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line=null;
		while((line=br.readLine())!=null) {
			System.out.println(line);
		}
		
		br.close();
		in.close();
		fs.close();
	}
	
	//  读取HDFS中的文件的指定偏移量范围内的数据内容
	@Test
	public void testReadContent2() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://cts01:9000/");
		FileSystem fs = FileSystem.get(conf);
		
		FSDataInputStream in = fs.open(new Path("/qingshu.txt"));
		in.seek(60);
		// 读/qingshu.txt中的从60偏移量开始的数据，总共20个字节
		byte[] b = new byte[4];
		int read = -1;
		long count = 0;
		while((read=in.read(b))!=-1) {
			System.out.print(new String(b,0,read));
			count += read;
			if(count==20) {
				break;
			}
		}
		
		in.close();
		fs.close();
	}
	
	
	
	//  写入数据内容到文件
	@Test
	public void testWriteDataToFile() throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://cts01:9000/");
		FileSystem fs = FileSystem.get(conf);
		
		FSDataOutputStream out = fs.create(new Path("/qingshu2.txt"));
		out.write("i love you 卢凯\n".getBytes());
		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		bw.write("i love you 朱璇 too");
		bw.newLine();
		bw.write("i love you all");
		
		bw.close();
		out.close();
		fs.close();
		
		
	}

}
