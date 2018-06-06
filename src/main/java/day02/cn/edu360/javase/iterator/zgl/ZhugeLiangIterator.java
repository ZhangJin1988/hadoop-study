package day02.cn.edu360.javase.iterator.zgl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * 迭代器：是为了让数据的使用者可以在不关心数据的存储位置，存储格式等细节的情况下，轻松获取数据的工具
 * 只不过，这一类工具，大家都遵循一个规范：Iterator,以便于任何一种迭代器，任何人拿到就会用
 * 
 * @author hunter.d
 * @create_time 2018年4月9日
 * @copyright www.edu360.cn
 */
public class ZhugeLiangIterator implements Iterator<Soldier> {
	BufferedReader br = null;
	String line = null;
	Soldier s = new Soldier();

	public ZhugeLiangIterator() {
		try {
			br = new BufferedReader(new FileReader("D:\\dev-workspace\\jiaoxue\\hdfs31\\sodiers.dat"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	@Override
	public boolean hasNext() {
		try {
			line = br.readLine();
			return line != null;

		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Soldier next() {
		// 1#关羽--38,男。50%28
		String[] split = line.split("#");
		String id = split[0];
		String[] split2 = split[1].split("--");
		String name =split2[0];
		String[] split3 = split2[1].split(",");
		int age = Integer.parseInt(split3[0]);
		String[] split4 = split3[1].split("。");
		String gender = split4[0];
		String[] split5 = split4[1].split("%");
		int killed = Integer.parseInt(split5[0]);
		int assistant = Integer.parseInt(split5[1]);
		
		s.set(id, name, gender, age, killed, assistant);
		return s;
	}

}
