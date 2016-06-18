package com.aavishkar.news.ingest;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

public class NewsDocumentParser {
	public static void main(String[] args) {
		try {
			File inputFile = new File("src/test/resources/document.xml");

			SAXBuilder saxBuilder = new SAXBuilder();

			Document document = saxBuilder.build(inputFile);

			System.out.println("Root element :" + document.getRootElement().getName());

			Element docElement = document.getRootElement();

			Map<String, String> map = new LinkedHashMap<String, String>();

			List<Element> rowList = docElement.getChildren();
			System.out.println("----------------------------");

			for (Element row: rowList) {
				readAndWriteToMap(row, map, new String[]{"APPLICATION_ID", "IC_NAME", "ORG_CITY", "ORG_DEPT",
						"ORG_COUNTRY", "ORG_NAME", "PHR" , 
				});
				List<Element> list = row.getChild("PIS").getChildren("PI");
				
				StringBuilder sb = new StringBuilder();
				boolean first = true;
				for (Element elem: list) {
					if (!first) {
						sb.append(";");
					}
					first = false;
					String childText1 = elem.getChildText("PI_NAME");
					capitalize(childText1);
					
					sb.append(elem.getChildText("PI_NAME"));
				}
				
				map.put("PI_NAME", sb.toString());
                readAndWriteToMap(row, map, new String[]{"PROGRAM_OFFICER_NAME","PROJECT_START","PROJECT_END"});
				System.out.println(map);
				map.clear();
			}
		} catch (JDOMException e) {
			e.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	public static void readAndWriteToMap(Element row, Map<String, String> map, String ... keys) {
		for (String key: keys) {
			String childText = row.getChildText(key);
			childText = capitalize(childText);

			map.put(key, childText);
		}
	}
	
	public static String capitalize(String name) {
		if (name == null) {
			return null;
		}
		
		name = name.toLowerCase();
		StringBuilder name1 = new StringBuilder();
		for (int x = 0 ; x < name.length(); x++)
		{
			if (x == 0 || name.charAt(x-1) == ' ' || name.charAt(x-1) == '\t' || name.charAt(x-1) == '\n'){
                 name1.append(Character.toUpperCase(name.charAt(x)));		
			}
			else {
			     name1.append(name.charAt (x));	
			};
		};
		return name1.toString();
	}
}