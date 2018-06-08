package day03.cn.edu360.data_analysis.wordcount;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class ReduceTask {
	
	public static void main(String[] args) throws Exception {
		
		HashMap<String, Integer> map = new HashMap<>();
		
		// 获取本任务的编号
		int taskNumb = Integer.parseInt(args[0]);
		// 获取目标输出路径
		String path = args[1];
		
		// 探测maptask输出目录中属于自己的文件
		FileSystem fs = FileSystem.get(new URI("hdfs://cts01:9000/"), new Configuration(), "root");
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/wordcount/mapout/"), false);
		while(files.hasNext()) {
			LocatedFileStatus file = files.next();
			if(file.getPath().getName().endsWith(taskNumb+"")) {
				FSDataInputStream in = fs.open(file.getPath());
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String line = null;
				while((line=br.readLine())!=null) {
					String[] split = line.split(",");
					Integer value = map.getOrDefault(split[0], 0);
					map.put(split[0], value+1);
				}
				in.close();
				br.close();
			}
		}
		
		// 准备输出目录 
		Path destOut = new Path(path);
		if(!fs.exists(destOut)) {
			fs.mkdirs(destOut);
		}
		
		// 输出统计结果
		FSDataOutputStream out = fs.create(new Path(path+"/r-"+taskNumb));
		Set<Entry<String, Integer>> entrySet = map.entrySet();
		for (Entry<String, Integer> entry : entrySet) {
			
			out.write((entry.getKey()+","+entry.getValue()+"\n").getBytes());
		}
		
		out.close();
		
		fs.close();
	}
	

}
