package com.aavishkar.news.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class AbstractParseDocument {

	public Map<String, String> processDocument(InputStream xmlStream) throws IOException, SAXException, ParserConfigurationException {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser saxParser = factory.newSAXParser();
		AbstractHandler userhandler = new AbstractHandler();
		saxParser.parse(xmlStream, userhandler);
		return userhandler.map;
	}
	
	private static class AbstractHandler extends DefaultHandler {
		private String currentElement = null;
		private Map<String, String> map = new TreeMap<String, String>();
		private String appId;
		private String absTxt;
		private Integer documentNumber;
		
		private AbstractHandler() {
			this.documentNumber = 0;
		}
		
		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			currentElement = qName;
		}
		
		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			if (qName.equals("row")  ) {
				documentNumber++;
				map.put (StringUtils.capitalize(appId) , StringUtils.capitalize(absTxt));
				System.out.println("Documents done is " + documentNumber);
				appId = null;
				absTxt = null;
			} 
			currentElement = null;
		}

		@Override
		public void characters(char ch[], int start, int length) throws SAXException {
			if (currentElement != null) {
				if (currentElement.equals("APPLICATION_ID")) {
					String elementValue = new String(ch, start, length);
					 appId = elementValue;
				}
				else if(currentElement.equals("ABSTRACT_TEXT")){
					String elementValue = new String(ch, start, length);
					String removeIfExists = "?   ";
					if (elementValue.startsWith(removeIfExists)){
						elementValue =	elementValue.substring(removeIfExists.length());
					}
					removeIfExists = "DESCRIPTION (provided by applicant): ";
					if (elementValue.startsWith(removeIfExists)){
						elementValue =	elementValue.substring(removeIfExists.length());
					}
					absTxt = elementValue;
				}
			}
		}
	}
}


