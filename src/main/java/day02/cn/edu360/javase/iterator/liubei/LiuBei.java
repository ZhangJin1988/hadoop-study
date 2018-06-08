package day02.cn.edu360.javase.iterator.liubei;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import day02.cn.edu360.javase.iterator.zgl.Soldier;
import day02.cn.edu360.javase.iterator.zgl.SoldierSet;

public class LiuBei {
	
	public static void main(String[] args) {
		// 自己new出诸葛亮的迭代器
		/*Iterator<Soldier> iter = new ZhugeLiangIterator();
		while(iter.hasNext()) {
			Soldier s = iter.next();
			System.out.println(s);
		}*/
		
		// 用诸葛亮的可迭代的“集合类”
		/*SoldierSet set = new SoldierSet();
		for (Soldier s : set) {
			System.out.println(s);
		}*/
		
		
		// 刘备想把士兵信息取出并按击杀人头数排序
		SoldierSet set = new SoldierSet();
		ArrayList<Soldier> sList = new ArrayList<>();
		for (Soldier s : set) {
			Soldier soldier = new Soldier();
			soldier.set(s.getId(), s.getName(), s.getGender(),s.getAge(), s.getKilled(), s.getAssistant());
			sList.add(soldier);
		}
		
		Collections.sort(sList, new Comparator<Soldier>() {

			@Override
			public int compare(Soldier o1, Soldier o2) {
				
				return o2.getKilled()-o1.getKilled();
			}
			
		});
		
		System.out.println(sList);
	}

}
