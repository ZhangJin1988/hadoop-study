package day03.cn.edu360.javase.log2object;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

public class SecondaryNameNode {
	
	public static void main(String[] args) throws Exception, IOException {
		
		// 加载镜像文件
		ObjectInputStream oin = new ObjectInputStream(new FileInputStream("d:/fsimage000000"));
		HashMap<String,String> metaData = (HashMap<String, String>) oin.readObject();
		oin.close();
		
		// 解析日志，并更新元数据对象metaData
		BufferedReader br = new BufferedReader(new FileReader("d:/edits_inprogress"));
		String line = null;
		while((line=br.readLine())!=null) {
			//add,/a.txt,aaa
			String[] split = line.split(",");
			if(split[0].equals("add")) {
				metaData.put(split[1], split[2]);
			}else {
				metaData.remove(split[1]);
			}
		}
		br.close();
		
		ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream("d:/fsimage000001"));
		os.writeObject(metaData);
		
		os.close();
		
		
	}

}
