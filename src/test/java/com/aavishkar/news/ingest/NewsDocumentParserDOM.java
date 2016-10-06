package com.aavishkar.news.ingest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.TreeMap;

import javax.xml.parsers.ParserConfigurationException;

import org.jdom2.JDOMException;
import org.xml.sax.SAXException;

import junit.framework.TestCase;

public class NewsDocumentParserDOM extends TestCase {
	public void testParseDocument() throws IOException, SAXException, ParserConfigurationException {
		File inputFile = new File("src/test/resources/document.xml");
		File inputFile2 = new File("src/test/resources/abstracts.xml");
		FileInputStream stream2 = new FileInputStream(inputFile2);
		AbstractParseDocument abs = new AbstractParseDocument();
		TreeMap<String,String> treeMap = abs.processDocument(stream2);
		ParseProjectDocumentDOM dom = new ParseProjectDocumentDOM();
		IngestNewsDocument ingestDocument = new IngestNewsDocument("test", "news", "http://ec2-52-41-129-32.us-west-2.compute.amazonaws.com:9200", true, treeMap);
		
		try {
			System.out.println("In file: "+inputFile.getAbsolutePath()+".");
			dom.processDocument(new FileInputStream(inputFile), ingestDocument);
		} catch (JDOMException e) {
			fail();
			e.printStackTrace();
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}
		
	}
}