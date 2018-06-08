package day03.cn.edu360.data_analysis.wordcount;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MapTask {
	
	public static void main(String[] args) throws Exception {
		if(args==null || args.length<4) {
			System.err.println("Usage:请输入正确的参数。参数1：要处理的文件;参数2：要处理的范围的起始偏移量......");
			return;
		}
		
		// 获取本任务实例所需要的必要参数
		String path = args[0];
		long startOffset = Long.parseLong(args[1]);
		long length = Long.parseLong(args[2]);
		int taskNum = Integer.parseInt(args[3]);
		
		// 获取一个访问HDFS的客户端对象
		FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), new Configuration(), "root");
		
		// 打开目标文件
		FSDataInputStream in = fs.open(new Path(path));
		// 定位到指定的偏移量位置
		in.seek(startOffset);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
		// 准备输出目录
		Path mapOut = new Path("/wordcount/mapout/");
		if(!fs.exists(mapOut)) {
			fs.mkdirs(mapOut);
		}
		
		// 准备hdfs的文件输出流
		FSDataOutputStream out0 = fs.create(new Path("/wordcount/mapout/m-"+taskNum+"-0"));
		FSDataOutputStream out1 = fs.create(new Path("/wordcount/mapout/m-"+taskNum+"-1"));
		
		// 判断本任务的起始偏移量是否是文件头，如果不是文件头，说明本任务不是这个文件的第1个任务，要抛弃第一行
		if(startOffset!=0) {
			br.readLine();
		}
		
		
		// 开始逐行读取并处理
		String line = null;
		long count = 0;
		// 构造一个数据处理逻辑实现类
		Properties props = new Properties();
		props.load(MapTask.class.getClassLoader().getResourceAsStream("wc.properties"));
		Class<?> forName = Class.forName(props.getProperty("MAPPER.CLASS"));
		Mapper mapper = (Mapper) forName.newInstance();
		
		while((line=br.readLine())!=null) {
			mapper.map(line, out0, out1);
			// 累加所读的数据的总长度
			count += line.length()+1;
			// 如果读取的累计总长度已经超出自己的任务长度（相当于多读了一行），则停止读取
			if(count > length) {
				break;
			}
		}
		
		out0.close();
		out1.close();
		in.close();
		br.close();
		fs.close();
	}

}
