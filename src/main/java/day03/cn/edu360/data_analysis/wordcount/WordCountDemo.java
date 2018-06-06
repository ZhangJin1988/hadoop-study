package day03.cn.edu360.data_analysis.wordcount;

public class WordCountDemo {
	
	public static void main(String[] args) {
		String path = args[0];
		long start = Long.parseLong(args[1]);
		long length = Long.parseLong(args[2]);
		
		System.out.println("我负责的文件路径为：" + path);
		System.out.println("我负责的起始偏移量为：" + start);
		System.out.println("我负责的处理长度为：" + length);
	}

}
