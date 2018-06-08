package day04.cn.edu360.mr.order_topn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class OrderBean implements Writable{
	
	// u01,order001,apple,5,8.5
	private String uid;
	private String oid;
	private String item;
	private int numb;
	private float price;
	
	public OrderBean() {
		
	}
	
	public OrderBean(String uid, String oid, String item, int numb, float price) {
		super();
		this.uid = uid;
		this.oid = oid;
		this.item = item;
		this.numb = numb;
		this.price = price;
	}
	
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public String getOid() {
		return oid;
	}
	public void setOid(String oid) {
		this.oid = oid;
	}
	public String getItem() {
		return item;
	}
	public void setItem(String item) {
		this.item = item;
	}
	public int getNumb() {
		return numb;
	}
	public void setNumb(int numb) {
		this.numb = numb;
	}
	public float getPrice() {
		return price;
	}
	public void setPrice(float price) {
		this.price = price;
	}

	@Override
	public String toString() {
		return "OrderBean [uid=" + uid + ", oid=" + oid + ", item=" + item + ", numb=" + numb + ", price=" + price
				+ "]";
	}

	// mr框架在将二进制数据反序列化成本类的对象时要调用的方法
	// 反序列化流程：先从本类反射出一个没有数据的对象，然后从二进制数据中反序列化出各个数据，然后set到这个反射出来的对象中
	@Override
	public void readFields(DataInput in) throws IOException {
		this.uid=in.readUTF();
		this.oid = in.readUTF();
		this.item = in.readUTF();
		this.numb = in.readInt();
		this.price = in.readFloat();
		
	}

	// mr框架在将本类的对象进行序列化操作时要调用的方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.uid);
		out.writeUTF(this.oid);
		out.writeUTF(this.item);
		out.writeInt(this.numb);
		out.writeFloat(this.price);
	}
	
}
