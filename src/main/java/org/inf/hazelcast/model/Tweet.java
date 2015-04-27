package org.inf.hazelcast.model;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class Tweet implements DataSerializable{
	private String createdAt;
	private String id;
	private String text;
	private String lang;
	public String getCreatedAt() {
		return createdAt;
	}
	public void setCreatedAt(String createdAt) {
		this.createdAt = createdAt;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getLang() {
		return lang;
	}
	public void setLang(String lang) {
		this.lang = lang;
	}
	public String toString(){
		return id+ " [" + text + "]"; 
	}
	
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(createdAt);
		out.writeUTF(id);
		out.writeUTF(text);
		out.writeUTF(lang);
		
	}
	public void readData(ObjectDataInput in) throws IOException {
		createdAt = in.readUTF();
		id = in.readUTF();
		text = in.readUTF();
		lang = in.readUTF();
	}
}

