package day04.cn.edu360.mr.movie_topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * reduce task用来做分组比较的比较器
 * reduce task的迭代器就是用这个比较器来对比两个相邻的key，是否应该看成同一组
 * 
 * @author hunter.d
 * @create_time 2018年4月12日
 * @copyright www.edu360.cn
 */
public class MovieIdGroupingComparator extends WritableComparator{
	
	public MovieIdGroupingComparator() {
		super(MovieBean.class,true);
	}
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MovieBean key1 = (MovieBean) a;
		MovieBean key2 = (MovieBean) b;
		
		return key1.getMovie().compareTo(key2.getMovie());
	}

}
