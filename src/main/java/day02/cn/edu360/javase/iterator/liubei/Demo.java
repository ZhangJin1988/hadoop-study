package day02.cn.edu360.javase.iterator.liubei;

import java.util.ArrayList;
import java.util.Iterator;

public class Demo {

	
	public static void main(String[] args) {
		
		ArrayList<String> list = new ArrayList<>();
		list.add("a");
		list.add("b");
		list.add("d");
		list.add("c");
		
		/*Iterator<String> it = list.iterator();
		while(it.hasNext()) {
			System.out.println(it.next());
		}*/
		
		for (String s : list) {
			System.out.println(s);
		}
		
		
		
	}
}
