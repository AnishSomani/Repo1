package com.aavishkar.news.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class ParseProjectDocumentSAX implements ParseProjectDocument {

	/**
	 * 
	 * 
	 * @see com.aavishkar.news.ingest.ParseProjectDocument#processDocument(java.io.InputStream, com.aavishkar.news.ingest.IngestNewsDocument, java.lang.String)
	 */
	@Override
	public void processDocument(InputStream xmlStream, IngestNewsDocument ingestDoc, String message) throws IOException, SAXException, ParserConfigurationException {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser saxParser = factory.newSAXParser();
		NewsHandler userhandler = new NewsHandler(ingestDoc, message);
		saxParser.parse(xmlStream, userhandler);
	}
	
	private static class NewsHandler extends DefaultHandler {
		private String currentElement = null;
		private LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
		private List<String> piNames = new ArrayList<String>();
		private List<String> terms = new ArrayList<String>();
		private IngestNewsDocument ingestDoc;
		private int documentNumber;
		private String message;
		
		private NewsHandler(IngestNewsDocument ingestDoc, String message) {
			this.ingestDoc = ingestDoc;
			this.documentNumber = 0;
			this.message = message;
		}
		
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
				documentNumber++;
				System.out.println("Ingesting document "+documentNumber+".  "+message);
				try {

					ingestDoc.storeToElasticSearch(map);
				} catch (IOException e) {
				//	throw new SAXException(e);
				}
				finishElement();
			} else if (qName.equals("PIS")) {
				if (piNames.size() > 0) {
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
				}
			} else if (qName.equals("PROJECT_TERMSX")) {
				if (terms.size() > 0) {
					map.put(StringUtils.capitalize("PROJECT_TERMSX"), terms.toString());
				}
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


