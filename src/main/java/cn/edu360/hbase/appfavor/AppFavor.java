package cn.edu360.hbase.appfavor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
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

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class AppFavor {

	public static void main(String[] args) throws Exception {

		// 读数据
		BufferedReader br = new BufferedReader(new FileReader("D:\\dev-workspace\\jiaoxue\\hbase-29\\20180423.log"));
		String line = null;
		HashMap<String, HashMap<String, Integer>> appFavorMap = new HashMap<>();

		// 统计出当日偏好结果
		while ((line = br.readLine()) != null) {
			String[] split = line.split(",");
			if (appFavorMap.containsKey(split[0])) {
				HashMap<String, Integer> innerMap = appFavorMap.get(split[0]);
				innerMap.put(split[2], innerMap.getOrDefault(split[2], 0) + 1);
			} else {
				HashMap<String, Integer> innerMap = new HashMap<>();
				innerMap.put(split[2], 1);
				appFavorMap.put(split[0], innerMap);
			}
		}

		br.close();

		// 从HBASE获取用户的综合偏好数据
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "cts02:2181,cts03:2181,cts04:2181");
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf("app_favor"));

		Set<Entry<String, HashMap<String, Integer>>> entrySet = appFavorMap.entrySet();
		Gson gson = new Gson();

		for (Entry<String, HashMap<String, Integer>> entry : entrySet) {

			// 取出这个人的综合偏好数据
			Get get = new Get(entry.getKey().getBytes());
			get.addColumn("f2".getBytes(), "a".getBytes());
			Result result = table.get(get);
			// 拿到的是这个人的综合偏好数据：json串
			byte[] all_favor_bytes = result.getValue("f2".getBytes(), "a".getBytes());

			// 如果没有历史数据，则说明今日处理的数据是系统中的第一天，直接将今日统计结果作为这个人的综合偏好结果
			if (all_favor_bytes == null) {
				String cur_json = gson.toJson(entry.getValue());
				Put put1 = new Put(entry.getKey().getBytes());
				put1.addColumn("f1".getBytes(), "c".getBytes(), cur_json.getBytes());
				put1.addColumn("f2".getBytes(), "a".getBytes(), cur_json.getBytes());

				table.put(put1);

			} else {

				String json = new String(all_favor_bytes);

				// 将json串转成hashmap
				HashMap<String, Integer> all_favor_map = gson.fromJson(json, new TypeToken<HashMap<String, Integer>>() {
				}.getType());

				// 合并当日数据和综合数据
				combine(entry.getValue(), all_favor_map);

				// 将今日数据、综合数据，变成json串，更新回HBASE
				String cur_json = gson.toJson(entry.getValue());
				String all_json = gson.toJson(all_favor_map);

				Put put1 = new Put(entry.getKey().getBytes());
				put1.addColumn("f1".getBytes(), "c".getBytes(), cur_json.getBytes());
				put1.addColumn("f2".getBytes(), "a".getBytes(), all_json.getBytes());

				table.put(put1);
			}
		}

		// 关闭连接
		table.close();
		conn.close();

	}

	private static void combine(HashMap<String, Integer> cur, HashMap<String, Integer> all) {

		// 1当日有，历史没有，则添加到历史中

		// 2当日有，历史也有，则累加

		// 3当日没有，历史有，则历史值*0.8

		// 挑出历史中存在，但当日不存在的app，做特别处理 --》3
		// 然后遍历当日处理一遍 --》 1 & 2
		Set<String> all_app = all.keySet();

		HashSet<String> all_app_clone = new HashSet<>(all_app);
		Set<String> cur_app = cur.keySet();

		// 将历史数据中的app去掉今日也存在的app
		all_app_clone.removeAll(cur_app);

		for (String key : all_app_clone) {
			all.put(key, all.get(key) * 8 / 10);
		}

		// 遍历当日app，去累加到历史map中
		for (String app : cur_app) {
			all.put(app, all.getOrDefault(app, 0) + cur.get(app));
		}

	}

}
