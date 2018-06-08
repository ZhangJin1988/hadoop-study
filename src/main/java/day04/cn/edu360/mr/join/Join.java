package day04.cn.edu360.mr.join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 用mr实现sql中的join模型
 * @author hunter.d
 * @create_time 2018年4月12日
 * @copyright www.edu360.cn
 */
public class Join {
	
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, JoinBean>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			// 获取任务切片信息
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			// 从任务切片信息中获取文件路径
			String fileName = inputSplit.getPath().getName();
			
			String[] split = value.toString().split(",");
			JoinBean joinBean = new JoinBean();
			
			if(fileName.equals("user.txt")) {
				// u001,senge,18,male,angelababy
				joinBean.set("NULL", split[0], split[1], Integer.parseInt(split[2]), split[3], split[4]);
			}else {
				joinBean.set(split[0], split[1], "NULL", 0, "NULL", "NULL");
			}
			
			context.write(new Text(joinBean.getUid()), joinBean);
			
		}
	}
	
	
	public static class JoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<JoinBean> values,
				Reducer<Text, JoinBean, JoinBean, NullWritable>.Context context) throws IOException, InterruptedException {
			ArrayList<JoinBean> orderList = new ArrayList<>();
			JoinBean userBean = new JoinBean();
			
			for (JoinBean v : values) {
				if(v.getOid().equals("NULL")) {
					// 如果这条数据是用户数据
					userBean.set(v.getOid(), v.getUid(), v.getName(), v.getAge(), v.getGender(), v.getFriend());
				}else {
					// 如果这条数据是订单数据
					JoinBean newBean = new JoinBean();
					newBean.set(v.getOid(), v.getUid(), v.getName(), v.getAge(), v.getGender(), v.getFriend());
					orderList.add(newBean);
				}
			}
			
			// 拼接数据：
			for (JoinBean joinBean : orderList) {
				
				joinBean.setName(userBean.getName());
				joinBean.setAge(userBean.getAge());
				joinBean.setGender(userBean.getGender());
				joinBean.setFriend(userBean.getFriend());
				
				context.write(joinBean, NullWritable.get());
			}
			
		}
		
		
	}
	
	public static void main(String[] args) throws Exception {
		

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Join.class);
		
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JoinBean.class);
		
		job.setOutputKeyClass(JoinBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\mrdata\\join\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\mrdata\\join\\join"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
		
	}

}
