package day05.cn.edu360.mr.sequence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{
	
	private String phone;
	private long upFlow;
	private long dFlow;
	private long sumFlow;
	
	public void set(String phone, long upFlow, long dFlow) {
		this.phone = phone;
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow+dFlow;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getdFlow() {
		return dFlow;
	}

	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.phone = in.readUTF();
		this.upFlow = in.readLong();
		this.dFlow = in.readLong();
		this.sumFlow = this.upFlow + this.dFlow;
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		 out.writeUTF(this.phone);
		 out.writeLong(this.upFlow);
		 out.writeLong(this.dFlow);
	}
	
	
	@Override
	public String toString() {
		return this.phone+","+this.upFlow+","+this.dFlow+","+this.sumFlow;
	}



	@Override
	public int compareTo(FlowBean o) {
		
		return (int) (o.getSumFlow()-this.getSumFlow());
	}

}
