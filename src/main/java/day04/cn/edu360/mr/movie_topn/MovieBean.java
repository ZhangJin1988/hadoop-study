package day04.cn.edu360.mr.movie_topn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MovieBean implements WritableComparable<MovieBean>{
	
	// {"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
	private String movie;
	private int rate;
	private long timeStamp;
	private String uid;
	
	public MovieBean() {
	}
	
	public MovieBean(String movie, int rate, long timeStamp, String uid) {
		super();
		this.movie = movie;
		this.rate = rate;
		this.timeStamp = timeStamp;
		this.uid = uid;
	}
	public String getMovie() {
		return movie;
	}
	public void setMovie(String movie) {
		this.movie = movie;
	}
	public int getRate() {
		return rate;
	}
	public void setRate(int rate) {
		this.rate = rate;
	}
	public long getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		 out.writeUTF(this.movie);
		 out.writeInt(this.rate);
		 out.writeLong(this.timeStamp);
		 out.writeUTF(this.uid);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		 this.movie = in.readUTF();
		 this.rate = in.readInt();
		 this.timeStamp = in.readLong();
		 this.uid = in.readUTF();
	}

	@Override
	public int compareTo(MovieBean o) {
		// 先比movieid,再比评分大小 
		return this.movie.compareTo(o.getMovie())==0?(o.getRate()-this.rate):this.movie.compareTo(o.getMovie());
	}
	
	
	@Override
	public String toString() {
		 
		return this.movie +","+ this.rate  +","+ this.timeStamp +"," + this.uid;
	}
}
