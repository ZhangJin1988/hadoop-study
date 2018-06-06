package day02.cn.edu360.javase.io;

import java.io.DataInputStream;
import java.io.FileInputStream;

public class DataInputStreamDemo {
	
	public static void main(String[] args) throws Exception {
		
		DataInputStream dataIn = new DataInputStream(new FileInputStream("D:\\dev-workspace\\jiaoxue\\hdfs31\\a.txt"));
		// int read = dataIn.readInt();
		long read = dataIn.readLong();
		
		/*int read = dataIn.read();
		System.out.println(read);
		read = dataIn.read();*/
		System.out.println(read);
		
	}

}
