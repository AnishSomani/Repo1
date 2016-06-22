package com.aavishkar.news.ingest;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import junit.framework.TestCase;

public class AbstractsDocumentParser extends TestCase {
	public static void testSAXParser() throws Exception {
		File inputFile = new File("src/test/resources/abstracts.xml");
		FileInputStream stream = new FileInputStream(inputFile);
		AbstractParseDocument sax = new AbstractParseDocument();
		Map<String, String> map = sax.processDocument(stream);
		for (String key: map.keySet()) {
			System.out.println(key+" Value: "+map.get(key));
		}
		
		assertEquals(3, map.size());
	}
}
