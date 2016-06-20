package com.aavishkar.news.ingest;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import junit.framework.TestCase;

public class NewsDocumentParser extends TestCase {
	public void testParseDocument() throws JDOMException, IOException {
		File inputFile = new File("src/test/resources/document.xml");

		SAXBuilder saxBuilder = new SAXBuilder();

		Document document = saxBuilder.build(inputFile);

		assertEquals("PROJECTS", document.getRootElement().getName());

		Element docElement = document.getRootElement();

		List<Element> rowList = docElement.getChildren();
		
		int count = 0;
		ParseProjectDocumentDOM dom = new ParseProjectDocumentDOM();
		IngestNewsDocument ingestDocument = new IngestNewsDocument("test", "news", "http://localhost:9200");
		
		for (Element row : rowList) {
			Map<String, String> map = dom.parseElement(row);
			String applicationId = ingestDocument.storeToElasticSearch(map, true);
			System.out.println("Done with application id: "+applicationId);
			count++;
		}
		
		assertEquals(3, count);
	}
}