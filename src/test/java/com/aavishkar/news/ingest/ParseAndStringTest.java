package com.aavishkar.news.ingest;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

public class ParseAndStringTest extends TestCase {
	public void testMap() {
		Map<String,String> map = new HashMap<String, String>();
		map.put("student1", "Anish");
		map.put("student2", "Vivek");
		map.put("studnet3", "Priyal");
		map.put("student1", "Anusha");
		assertEquals(3, map.size());
	}
	
	public void testCapitalize() {
		String name = "AKHLAGHI, FATEMEH\nand\tok";
		String expected = "Akhlaghi, Fatemeh\nAnd\tOk";
		 name = NewsDocumentParser.capitalize(name);
		
		assertEquals(expected, name);
	}

}
