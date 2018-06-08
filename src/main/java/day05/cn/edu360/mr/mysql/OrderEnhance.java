package day05.cn.edu360.mr.mysql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderEnhance {

	public static class OrderEnhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		Connection conn = null;
		PreparedStatement prepareStatement = null;
		ResultSet rs = null;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			try {
				// 注册 JDBC 驱动
				Class.forName("com.mysql.jdbc.Driver");

				// 打开链接
				conn = DriverManager.getConnection("jdbc:mysql://cts01:3306/user", "root", "root");
			} catch (Exception e) {

			}

		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			try {
				String[] split = value.toString().split(",");

				// 执行查询
				prepareStatement = conn.prepareStatement("SELECT name, age, addr FROM t_user where id=?");
				prepareStatement.setString(1, split[0]);
				rs = prepareStatement.executeQuery();

				while (rs.next()) {
					String name = rs.getString("name");
					int age = rs.getInt("age");
					String addr = rs.getString("addr");
					// 输出数据
					context.write(new Text(value.toString() + "," + name + "," + age + "," + addr), NullWritable.get());
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			try {
				// 完成后关闭
				rs.close();
				prepareStatement.close();
				conn.close();
			} catch (SQLException se) {
				// 处理 JDBC 错误
				se.printStackTrace();
			} catch (Exception e) {
				// 处理 Class.forName 错误
				e.printStackTrace();
			} finally {
				// 关闭资源
				try {
					if (prepareStatement != null)
						prepareStatement.close();
				} catch (SQLException se2) {
				} // 什么都不做
				try {
					if (conn != null)
						conn.close();
				} catch (SQLException se) {
					se.printStackTrace();
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(OrderEnhance.class);

		job.setMapperClass(OrderEnhanceMapper.class);

		// 不需要reducetask阶段
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path("F:\\mrdata\\orderdata\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\mrdata\\orderdata\\output"));

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}

}
