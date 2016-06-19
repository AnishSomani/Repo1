package com.aavishkar.news.ingest;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.google.gson.Gson;

import junit.framework.TestCase;

public class ParseAndStringTest extends TestCase {
	public void testMap() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("student1", "Anish");
		map.put("student2", "Vivek");
		map.put("studnet3", "Priyal");
		map.put("student1", "Anusha");
		assertEquals(3, map.size());
	}

	public void testCapitalize() {
		String name = "AKHLAGHI, FATEMEH\nand\tok";
		String expected = "Akhlaghi, Fatemeh\nAnd\tOk";
		assertEquals(expected, NewsDocumentParser.capitalize(name));

		name = "AKHLAGHI_FATEMEH\nand\tok";
		expected = "Akhlaghi_Fatemeh\nAnd\tOk";

		assertEquals(expected, NewsDocumentParser.capitalize(name));
	}

	public void testJsonFromMap() {
		Map<String, String> map = new TreeMap<String, String>();
		map.put("student1", "Anish");
		map.put("student2", "Vivek");
		map.put("student3", "Priyal");
		map.put("student1", "Anusha");
		Gson gson = new Gson();
		String string = gson.toJson(map);
		assertTrue(string.contains("Anusha"));
		assertTrue(string.contains("Vivek"));
		assertFalse(string.contains("Anish"));
	}

}
