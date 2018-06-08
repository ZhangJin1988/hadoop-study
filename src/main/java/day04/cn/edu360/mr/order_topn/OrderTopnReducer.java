package day04.cn.edu360.mr.order_topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class OrderTopnReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<OrderBean> values,
			Reducer<Text, OrderBean, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {
		
		ArrayList<OrderBean> beans = new ArrayList<>();
		
		// 用迭代器取出这一组数据，放入beans集合
		for (OrderBean orderBean : values) {
			
			OrderBean newBean = new OrderBean(orderBean.getUid(), orderBean.getOid(), orderBean.getItem(), orderBean.getNumb(), orderBean.getPrice());
			beans.add(newBean);
		}
		
		// 将数据在集合中排序
		Collections.sort(beans, new Comparator<OrderBean>() {
			@Override
			public int compare(OrderBean o1, OrderBean o2) {
				return o2.getNumb()*o2.getPrice() - o1.getNumb()*o1.getPrice()>0?1:-1;
			}
		});
		
		
		// 返回top2条数据给reduce task
		for(int i=0;i<2;i++) {
			context.write(beans.get(i), NullWritable.get());
		}
	}

}
