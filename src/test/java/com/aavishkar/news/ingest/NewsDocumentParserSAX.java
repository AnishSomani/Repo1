package com.aavishkar.news.ingest;

import java.io.File;
import java.io.FileInputStream;

import junit.framework.TestCase;

public class NewsDocumentParserSAX extends TestCase {
	public static void testSAXParser() throws Exception {
		File inputFile = new File("src/test/resources/projects.xml");
		FileInputStream stream = new FileInputStream(inputFile);
		ParseProjectDocument sax = new ParseProjectDocumentSAX();
		IngestNewsDocument ingestDoc = new IngestNewsDocument("test", "news", "http://localhost:9200", true);
		sax.processDocument(stream, ingestDoc, "In file: " + inputFile.getAbsolutePath() + ".");
	}
}
