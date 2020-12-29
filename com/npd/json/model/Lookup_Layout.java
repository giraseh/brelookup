package com.npd.json.model;

import java.util.ArrayList;

public class Lookup_Layout {

	private String lookup_name;
	private String keys;
	private ArrayList<Lookup_Target> targets = new ArrayList<>();

	public String getLookup_name() {
		return lookup_name;
	}

	public void setLookup_name(String lookup_name) {
		this.lookup_name = lookup_name;
	}

	public String getKeys() {
		return keys;
	}

	public void setKeys(String keys) {
		this.keys = keys;
	}

	public ArrayList<Lookup_Target> getTargets() {
		return targets;
	}

	public void setTargets(ArrayList<Lookup_Target> targets) {
		this.targets = targets;
	}

	

	
}
