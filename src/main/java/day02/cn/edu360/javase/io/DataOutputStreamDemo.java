package day02.cn.edu360.javase.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.junit.Test;

public class DataOutputStreamDemo {
	
	@Test
	public void testWrite() throws Exception {
		DataOutputStream dataOut = new DataOutputStream(new FileOutputStream("D:\\dev-workspace\\jiaoxue\\hdfs31\\b.txt"));
		
		int a = 100000;
		String s = "abc";
		long b = 10101001010L;
		
		dataOut.writeInt(a);
		dataOut.writeUTF(s);
		dataOut.writeLong(b);
		
		dataOut.close();
		
	}
	
	@Test
	public void testRead() throws Exception {
		DataInputStream dataIn = new DataInputStream(new FileInputStream("D:\\dev-workspace\\jiaoxue\\hdfs31\\b.txt"));
		
		int a = dataIn.readInt();
		String s = dataIn.readUTF();
		long b = dataIn.readLong();
		
		System.out.println(a);
		System.out.println(b);
		
		dataIn.close();
		
	}

}
