package top.ganhoo.hbase.usecase.app_favor;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class CombinerUtil {
	
	public static String combinerFavor(String json1,String json2,Gson gson) {
		Type type = new TypeToken<Map<String, Integer>>() {}.getType(); 
		Map<String, Integer> map1 = gson.fromJson(json1, type); // ���յ�ƫ������
		
		Map<String, Integer> map2 = gson.fromJson(json2, type);  // �ۼ�ƫ������
		
		Set<String> appset1 = map1.keySet();
		Set<String> appset2 = map2.keySet();
		
		HashSet<String> tmpSet = new HashSet<>();
		tmpSet.addAll(appset2);
		
		// �õ��ۼ�ƫ�����ڵ���û���ֹ���app
		tmpSet.removeAll(appset1);
		// ������û���ֵ�app��ƫ��ֵ�ĳ�֮ǰ��0.7��
		for (String app : tmpSet) {
			int newValue = (int) (map2.get(app)*0.7);
			map2.put(app, newValue);
		}
		
		
		// �������ճ��ֹ���app
		for(String app:appset1) {
			
			if(map2.containsKey(app)) {
				map2.put(app, map2.get(app)+map1.get(app));
			}else {
				map2.put(app, map1.get(app));
			}
		}
		
		return gson.toJson(map2);
	}
	
	/**
	 * ����json--��map  map--��json
	 * @param args
	 */
	public static void main(String[] args) {
		Gson gson = new Gson();
		HashMap<String, Integer> map = new HashMap<>();
		map.put("iqiyi", 3);
		map.put("doudizhu", 2);
		
		String json = gson.toJson(map);
		
		Type type = new TypeToken<Map<String, Integer>>() {}.getType(); 
		Map<String, Integer> map1 = gson.fromJson(json, type);
		System.out.println(map1);
		
		
	}

}
