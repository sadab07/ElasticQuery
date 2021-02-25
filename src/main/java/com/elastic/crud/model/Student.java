package com.elastic.crud.model;

public class Student {
	
	private String name;
	private int age;
	private String ip;
	private String date;
	public Student() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Student(String name, int age, String ip, String date) {
		super();
		this.name = name;
		this.age = age;
		this.ip = ip;
		this.date = date;
	}
	@Override
	public String toString() {
		return "Student [name=" + name + ", age=" + age + ", ip=" + ip + ", date=" + date + "]";
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	
}
