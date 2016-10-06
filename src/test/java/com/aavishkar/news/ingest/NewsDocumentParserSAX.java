package com.aavishkar.news.ingest;

import java.io.File;
import java.io.FileInputStream;
import java.util.TreeMap;

import junit.framework.TestCase;

public class NewsDocumentParserSAX extends TestCase {
	public static void testSAXParser() throws Exception {
		File inputFile = new File("src/test/resources/projects.xml");
		FileInputStream stream = new FileInputStream(inputFile);
		File inputFile2 = new File("src/test/resources/abstracts.xml");
		FileInputStream stream2 = new FileInputStream(inputFile2);
		AbstractParseDocument abs = new AbstractParseDocument();
		ParseProjectDocumentSAX sax = new ParseProjectDocumentSAX();
		TreeMap<String,String> treeMap = abs.processDocument(stream2);
		IngestNewsDocument ingestDoc = new IngestNewsDocument("test", "news", "http://localhost:9200", true , treeMap );
		System.out.println("In file: " + inputFile.getAbsolutePath() + ".");
		sax.processDocument(stream, ingestDoc);
	}
}
