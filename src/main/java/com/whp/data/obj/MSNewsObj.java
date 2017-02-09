package com.whp.data.obj;

public class MSNewsObj {
	private String date;
	private String program;
	private String category;
	private String id0;
	private int num;
	
	
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getProgram() {
		return program;
	}
	public void setProgram(String program) {
		this.program = program;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}
	public String getId0() {
		return id0;
	}
	public void setId0(String id0) {
		this.id0 = id0;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return String.format("%s\t%s\t%s\t%s\t%s", this.date, this.program, this.category, this.id0, this.num);
	}

	public MSNewsObj(String d, String p, String c, String i, int n){
		this.date = d;
		this.program = p;
		this.category = c;
		this.id0 = i;
		this.num = n;
	}
	
	static public MSNewsObj fromString(String s){
		String[] strs = s.split("\t");
		
		if (strs.length == 5) {
			return new MSNewsObj(strs[0], strs[1], strs[2], strs[3], Integer.parseInt(strs[4]));
		} else {
			return null;
		}
	}
	public MSNewsObj(){
		
	}
	

}
