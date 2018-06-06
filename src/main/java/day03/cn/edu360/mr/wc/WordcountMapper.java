package day03.cn.edu360.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN ：表示输入的KEY的类型，输入的KEY是maptask所读取到的一行文本的起始偏移量，long
 * VALUEIN：表示输入的VALUE的类型，输入的VALUE是maptask所读取到的一行文本的内容，String
 * 
 * KEYOUT：我们将输出的KEY的类型，我们在本逻辑中输出单词做key，String
 * VALUEOUT:我们将输出的VALUE的类型，我们在本逻辑中输出1做value，Integer
 * 
 * 
 * 但是，在mapreduce中，maptask输出的key，value需要经过网络传给reducetask，所以，这些key对象，value对象，都要可以被序列化反序列化
 * 虽然Long、String等jdk中的数据类型都实现了Serializable接口，可以被序列化，但是Serializable序列化机制产生的序列化数据相当臃肿，会大大降低网路传输的效率
 * 所以hadoop专门自己设计了一套序列化机制，接口为Writable
 * 那么，在maptask输出给reducetask的key-value都必须实现Writable接口
 * 
 * Long -->  LongWritable
 * String --> Text
 * Integer --> IntWritable
 * Double --> DoubleWritable
 * .......
 * 
 * @author hunter.d
 * @create_time 2018年4月11日
 * @copyright www.edu360.cn
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	// 谁会调这个map方法？ --》 map task程序
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// 切分单词
		String[] words = value.toString().split(" ");
		
		for (String word : words) {
			context.write(new Text(word), new IntWritable(1));
		}
		
	}
	
	

}
