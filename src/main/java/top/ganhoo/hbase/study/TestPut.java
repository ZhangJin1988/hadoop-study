package top.ganhoo.hbase.study;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.UUID;

public class TestPut {

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hdp-01:2181,hdp-02:2181,hdp-03:2181");

		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		boolean ex = admin.tableExists(TableName.valueOf("t_app_log"));

		if(!ex) {
			HTableDescriptor hd = new HTableDescriptor(TableName.valueOf("t_app_log"));
			HColumnDescriptor hc1 = new HColumnDescriptor("header");
			HColumnDescriptor hc2 = new HColumnDescriptor("events");
			hd.addFamily(hc1);
			hd.addFamily(hc2);
			admin.createTable(hd);
		}
		admin.close();


		Table table = conn.getTable(TableName.valueOf("t_app_log"));
		System.out.println("--成功连接......");

		String events = "{\"events\":\"1473367236143\\u00010\\u0001connectByQRCode\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000027\\u0001\\n1473367261933\\u00010\\u0001AppLaunch\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000028\\u0001\\n1473367280349\\u00010\\u0001connectByQRCode\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000029\\u0001\\n1473367331326\\u00010\\u0001AppLaunch\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000030\\u0001\\n1473367353310\\u00010\\u0001connectByQRCode\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000031\\u0001\\n1473367387087\\u00010\\u0001AppLaunch\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000032\\u0001\\n1473367402167\\u00010\\u0001connectByQRCode\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000033\\u0001\\n1473367451994\\u00010\\u0001AppLaunch\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000034\\u0001\\n1473367474316\\u00010\\u0001connectByQRCode\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000035\\u0001\\n1473367564181\\u00010\\u0001AppLaunch\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000036\\u0001\\n1473367589527\\u00010\\u0001connectByQRCode\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000037\\u0001\\n1473367610310\\u00010\\u0001AppLaunch\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000038\\u0001\\n1473367624647\\u00010\\u0001connectByQRCode\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000039\\u0001\\n1473368004298\\u00010\\u0001AppLaunch\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000040\\u0001\\n1473368017851\\u00010\\u0001connectByQRCode\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000041\\u0001\\n1473369599067\\u00010\\u0001AppLaunch\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000042\\u0001\\n1473369622274\\u00010\\u0001connectByQRCode\\u0001\\u00010\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u0001\\u00011609072239570000043\\u0001\\n\"";
		String header = "{\"header\":{\"cid_sn\":\"1501004207EE98AA\",\"mobile_data_type\":\"\",\"os_ver\":\"9\",\"mac\":\"88:1f:a1:03:7d:a8\",\"resolution\":\"2560x1337\",\"commit_time\":\"1473399829041\",\"sdk_ver\":\"103\",\"device_id_type\":\"mac\",\"city\":\"江门市\",\"android_id\":\"\",\"device_model\":\"MacBookPro11,1\",\"carrier\":\"中国xx\",\"promotion_channel\":\"1\",\"app_ver_name\":\"1.7\",\"imei\":\"\",\"app_ver_code\":\"23\",\"pid\":\"pid\",\"net_type\":\"3\",\"device_id\":\"m.88:1f:a1:03:7d:a8\",\"app_device_id\":\"m.88:1f:a1:03:7d:a8\",\"release_channel\":\"appstore\",\"country\":\"CN\",\"time_zone\":\"28800000\",\"os_name\":\"ios\",\"manufacture\":\"apple\",\"commit_id\":\"fde7ee2e48494b24bf3599771d7c2a78\",\"app_token\":\"XIAONIU_I\",\"account\":\"none\",\"app_id\":\"com.appid.xiaoniu\",\"build_num\":\"YVF6R16303000403\",\"language\":\"zh\"}}";
		System.out.println("events大小：" + events.length());
		System.out.println("header大小： "+header.length());
		long start = System.currentTimeMillis();
		System.out.println("开始时间：  " + start);
		int count = 1000*1000;
		for(int i=0;i<count;i++) {
			UUID uuid = UUID.randomUUID();
			Put put = new Put(Bytes.toBytes(uuid.toString()));
			put.addColumn("events".getBytes(), uuid.toString().getBytes(), events.getBytes());
			put.addColumn("header".getBytes(), uuid.toString().getBytes(), header.getBytes());

			table.put(put);
		}
		long end = System.currentTimeMillis();
		System.out.println("结束时间：  " + end);
		System.out.println(count + "条数据耗费时间,毫秒数： " + (end-start) + "  秒数： " + (end-start)/1000d);
		System.out.println("插入数据量： " + count*(events.length()+header.length())/1024/1024 + " M");
	}

}
