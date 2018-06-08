package top.ganhoo.hbase.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/**
 * 生成HFILE文件，并bulkload到hbase
 * 测试数据见 testdata/hfiletest/a.txt
 * 
 * @author hunter.ganhoo
 *
 */
public class HFileGenerator {

	public static class HFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] items = line.split(",", -1);
			ImmutableBytesWritable rowkey = new ImmutableBytesWritable(items[0].getBytes());

			KeyValue kv = new KeyValue(Bytes.toBytes(items[0]), Bytes.toBytes(items[1]), Bytes.toBytes(items[2]),
					System.currentTimeMillis(), Bytes.toBytes(items[3]));
			if (null != kv) {
				context.write(rowkey, kv);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "hdp-01:2181,hdp-02:2181,hdp-03:2181");

		Job job = Job.getInstance(conf, "HFile bulk load test");
		job.setJarByClass(HFileGenerator.class);

		job.setMapperClass(HFileMapper.class);
		job.setReducerClass(KeyValueSortReducer.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(KeyValue.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		HTableDescriptor td = new HTableDescriptor(TableName.valueOf("wc"));
		Connection conn = ConnectionFactory.createConnection(conf);
		RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf("wc"));
		
		HFileOutputFormat2.configureIncrementalLoad(job, td, regionLocator);

		boolean res = job.waitForCompletion(true);
		
		// HFile生成完毕后，调用hbase的api将生成好的HFile导入hbase的wc表
		if (res) {
			System.out.println("HFILE生成完毕，开始导入.......");
			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
			Table table = conn.getTable(TableName.valueOf("wc"));
			Admin admin = conn.getAdmin();
			loader.doBulkLoad(new Path(args[1]), admin, table, regionLocator);
		}

	}
}
