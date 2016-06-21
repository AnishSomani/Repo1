package com.aavishkar.news.ingest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.jdom2.JDOMException;

import junit.framework.TestCase;

public class NewsDocumentParserDOM extends TestCase {
	public void testParseDocument() {
		File inputFile = new File("src/test/resources/document.xml");

		ParseProjectDocumentDOM dom = new ParseProjectDocumentDOM();
		IngestNewsDocument ingestDocument = new IngestNewsDocument("test", "news", "http://ec2-52-41-129-32.us-west-2.compute.amazonaws.com:9200", true);
		
		try {
			dom.processDocument(new FileInputStream(inputFile), ingestDocument, "In file: "+inputFile.getAbsolutePath()+".");
		} catch (JDOMException e) {
			fail();
			e.printStackTrace();
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
		
	}
}