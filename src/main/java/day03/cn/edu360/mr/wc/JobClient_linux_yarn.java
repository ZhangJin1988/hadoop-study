package day03.cn.edu360.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * job客户端，跟mr程序本身没有任何逻辑关系
 * 它只是负责把mr程序所在的jar包等信息提交给yarn去运行
 * 它和mr程序之间的关系，就像： 运载火箭  和 卫星 的关系
 * @author hunter.d
 * @create_time 2018年4月11日
 * @copyright www.edu360.cn
 */
public class JobClient_linux_yarn {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); // 加载classpath中的hadoop配置文件
		// job api对象，在提交mrjob去运行时，有两种提交目的地选择：1.本地模拟器  2.yarn
		conf.set("mapreduce.framework.name", "yarn");  //  mapred-site.xml
		conf.set("fs.defaultFS", "hdfs://cts01:9000");  // core-site.xml
		Job job = Job.getInstance(conf);
		
		// 封装本mr程序相关到信息到job对象中
		//job.setJar("d:/wc.jar");
		job.setJarByClass(JobClient_linux_yarn.class);
		
		// 指定mapreduce程序用jar包中的哪个类作为Mapper逻辑类
		job.setMapperClass(WordcountMapper.class);
		// 指定mapreduce程序用jar包中的哪个类作为Reducer逻辑类
		job.setReducerClass(WordcountReducer.class);
		
		// 告诉mapreduce程序，我们的map逻辑输出的KEY.VALUE的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 告诉mapreduce程序，我们的reduce逻辑输出的KEY.VALUE的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 告诉mapreduce程序，我们的原始文件在哪里
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input/"));
		// 告诉mapreduce程序，结果数据往哪里写
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/output2/"));
		
		// 设置reduce task的运行实例数
		job.setNumReduceTasks(2); // 默认是1
		
		// 调用job对象的方法来提交任务
		//job.submit();
		boolean res = job.waitForCompletion(true);  // 阻塞方法
		System.exit(res?0:1);
		
	}

}
