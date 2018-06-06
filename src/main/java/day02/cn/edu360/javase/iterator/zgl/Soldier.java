package day02.cn.edu360.javase.iterator.zgl;

public class Soldier {
	
	private String id;
	private String name;
	private String gender;
	private int age;
	private int killed;
	private int assistant;
	
	public void set(String id, String name, String gender, int age, int killed, int assistant) {
		this.id = id;
		this.name = name;
		this.gender = gender;
		this.age = age;
		this.killed = killed;
		this.assistant = assistant;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public int getKilled() {
		return killed;
	}
	public void setKilled(int killed) {
		this.killed = killed;
	}
	public int getAssistant() {
		return assistant;
	}
	public void setAssistant(int assistant) {
		this.assistant = assistant;
	}

	@Override
	public String toString() {
		return "Soldier [id=" + id + ", name=" + name + ", gender=" + gender + ", age=" + age + ", killed=" + killed
				+ ", assistant=" + assistant + "]";
	}
	
	
}
