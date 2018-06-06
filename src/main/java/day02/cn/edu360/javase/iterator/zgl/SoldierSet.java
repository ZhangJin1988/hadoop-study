package day02.cn.edu360.javase.iterator.zgl;

import java.util.Iterator;

public class SoldierSet implements Iterable<Soldier>{

	@Override
	public Iterator<Soldier> iterator() {
		
		return new ZhugeLiangIterator();
	}
	
}
