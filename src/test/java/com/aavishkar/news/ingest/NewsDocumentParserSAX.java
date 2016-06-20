package com.aavishkar.news.ingest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class NewsDocumentParserSAX {
	public static void main(String[] args) {
		try {
			File inputFile = new File("src/test/resources/document.xml");
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			NewsHandler userhandler = new NewsHandler();
			saxParser.parse(inputFile, userhandler);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static class NewsHandler extends DefaultHandler {
		private String currentElement = null;
		private Map<String, String> map = new TreeMap<String, String>();
		private List<String> piNames = new ArrayList<String>();
		private List<String> terms = new ArrayList<String>();
		private IngestNewsDocument ingestDocument = new IngestNewsDocument("test", "news", "http://localhost:9200");
		
		private List<String> elementsOfInterest = NewsHandler.getElementsOfInterest();
		
		private static List<String> getElementsOfInterest() {
			String[] elementsOfInterest = new String[] { "APPLICATION_ID", "IC_NAME", "ORG_CITY", "ORG_DEPT",
					"ORG_COUNTRY", "ORG_NAME", "ORG_STATE", "PHR",  "PROGRAM_OFFICER_NAME", "PROJECT_START", "PROJECT_END",
					"PROJECT_TITLE", "FUNDING_MECHANISM", "STUDY_SECTION_NAME", "PI_NAME", "TERM"};
			List<String> list = new ArrayList<String>();
			for (String element: elementsOfInterest) {
				list.add(element);
			}
			return list;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			currentElement = qName;
		}
		
		private void finishElement() {
			map.clear();
			piNames.clear();
			terms.clear();
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			if (qName.equals("row")  ) {
//				System.out.println("Current row finished.  Ingesting document to elastic search"+map);
				try {
					ingestDocument.storeToElasticSearch(map, true);
				} catch (IOException e) {
					throw new SAXException(e);
				}
				finishElement();
			} else if (qName.equals("PIS")) {
				boolean first = true;
				StringBuilder sb = new StringBuilder();
				for (String name: piNames) {
					if (!first) {
						sb.append(";");
					}
					first = false;
					sb.append(StringUtils.capitalize(name));
				}
				map.put(StringUtils.capitalize("PI_NAME"), sb.toString());
			} else if (qName.equals("PROJECT_TERMSX")) {
				map.put(StringUtils.capitalize("PROJECT_TERMSX"), terms.toString());
			}
			currentElement = null;
		}

		@Override
		public void characters(char ch[], int start, int length) throws SAXException {
			if (currentElement != null) {
				if (elementsOfInterest.contains(currentElement)) {
					String elementValue = new String(ch, start, length);
					if ("PI_NAME".equals(currentElement)) {
						piNames.add(StringUtils.capitalize(elementValue));
					} else if ("TERM".equals(currentElement)) {
						terms.add("\""+StringUtils.capitalize(elementValue)+"\"");
					} else {
						map.put(StringUtils.capitalize(currentElement), StringUtils.capitalize(elementValue));
					}
				}
			}
		}
	}
}


