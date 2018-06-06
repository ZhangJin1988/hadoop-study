package day03.cn.edu360.javase.log2object;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.util.HashMap;

public class NameNode {
	
	public static void main(String[] args) throws Exception {
		
		/*HashMap<String, String> metaData = new HashMap<>();
		ObjectOutputStream oout = new ObjectOutputStream(new FileOutputStream("d:/fsimage000000"));
		oout.writeObject(metaData);
		
		oout.close();*/
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("d:/edits_inprogress"));
		
		ObjectInputStream oin = new ObjectInputStream(new FileInputStream("d:/fsimage000001"));
		HashMap<String,String> metaData = (HashMap<String, String>) oin.readObject();
		
		// 开始接收客户端的请求，并进行相应对元数据更改
		bw.flush();
		bw.close();
		System.out.println(metaData);
		Thread.sleep(5000);
		throw new RuntimeException();
	}

}
