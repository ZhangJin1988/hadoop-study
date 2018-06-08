package top.ganhoo.hbase.usecase.app_favor;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.Gson;

/**
 * �û�appƫ�����ݴ���
 * 
 * @author hunter.ganhoo
 *
 */
public class AppFavorProcess {
	// args[0] = "2017-11-08";
	public static void main(String[] args) throws Exception {

		FileInputStream in = new FileInputStream("F:\\user-favor-log\\2017-11-09\\favor.log");

		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		// key--> u01:iqiyi,3 u01:doudizhu,2
		HashMap<String, HashMap<String, Integer>> uMap = new HashMap<>();

		// ���ж�ȡ�ļ���ͳ��ÿ���˶�ÿ��Ӧ�õ�ʹ�ô���
		String line = null;
		while ((line = br.readLine()) != null) {
			// 2017-11-08,u01,iqiyi
			String[] fields = line.split(",");
			String uid = fields[1];
			if (uMap.containsKey(uid)) {
				HashMap<String, Integer> appMap = uMap.get(uid);
				if (appMap.containsKey(fields[2])) {
					appMap.put(fields[2], appMap.get(fields[2]) + 1);
				} else {
					appMap.put(fields[2], 1);
				}

			} else {
				HashMap<String, Integer> appMap = new HashMap<>();
				appMap.put(fields[2], 1);
				uMap.put(uid, appMap);
			}
		}

		// ����һ��hbase�Ŀͻ���
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(Bytes.toBytes("t_user_favor")));

		// �ѵ����ƫ�����ݽ������HBASE��current_day����
		Gson gson = new Gson();
		Set<String> users = uMap.keySet();
		for (String uid : users) {
			HashMap<String, Integer> appMap = uMap.get(uid);
			String json1 = gson.toJson(appMap);

			// �洢����ƫ�����ݵ�HBASE
			Put put = new Put(Bytes.toBytes(uid));
			put.addColumn(Bytes.toBytes("current_day"), Bytes.toBytes(uid), Bytes.toBytes(json1));
			table.put(put);

			// ��HBASE��ȡ���û����ۼ�ƫ������
			Get get = new Get(Bytes.toBytes(uid));
			Result result = table.get(get);
			String json2 = null;
			byte[] jsonArray = result.getValue(Bytes.toBytes("composit"), Bytes.toBytes(uid));
			if (jsonArray == null) {
				Put put_tmp = new Put(Bytes.toBytes(uid));
				put_tmp.addColumn(Bytes.toBytes("composit"), Bytes.toBytes(uid), Bytes.toBytes(json1));
				table.put(put_tmp);
			} else {
				json2 = new String(jsonArray);

				// �ϲ������ƫ��ֵ���ۼ�ƫ��ֵ
				String resultJson = CombinerUtil.combinerFavor(json1, json2, gson);

				// ���ϲ��õ��ۼ�ƫ������ֵ�洢��HBASE
				Put put2 = new Put(Bytes.toBytes(uid));
				put2.addColumn(Bytes.toBytes("composit"), Bytes.toBytes(uid), Bytes.toBytes(resultJson));
				table.put(put2);
			}
		}

		table.close();
		conn.close();

	}

}
