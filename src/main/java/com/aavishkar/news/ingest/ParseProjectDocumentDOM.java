package com.aavishkar.news.ingest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

public class ParseProjectDocumentDOM implements ParseProjectDocument {
	public void processDocument(InputStream xmlStream, IngestNewsDocument ingestDoc) throws JDOMException, IOException {
		SAXBuilder saxBuilder = new SAXBuilder();

		Document document = saxBuilder.build(xmlStream);

		Element docElement = document.getRootElement();
		if (docElement.getName().equals("PROJECTS")) {
			List<Element> rowList = docElement.getChildren();
			int size = rowList.size();
			for (int index = 0; index < size; index++) {
				Element row = rowList.get(index);
				if (row.getName().equals("row")) {
					LinkedHashMap<String, String> map = parseElement(row);
					ingestDoc.storeToElasticSearch(map);
					System.out.println("Ingested "+(index + 1)+"/"+size+" documents. " +
							" Application id: "+map.get("Application_Id"));
				}
			}
		}
	}
	
	private LinkedHashMap<String, String> parseElement(Element row) throws IOException {
		LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();

		readAndWriteToMap(row, map, new String[] { "APPLICATION_ID", "IC_NAME", "ORG_CITY", "ORG_DEPT",
				"ORG_COUNTRY", "ORG_NAME", "ORG_STATE", "PHR", });
		List<Element> list = row.getChild("PIS").getChildren("PI");

		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Element elem : list) {
			if (!first) {
				sb.append(";");
			}
			first = false;
			String childText1 = elem.getChildText("PI_NAME");

			sb.append(StringUtils.capitalize(childText1));
		}

		map.put(StringUtils.capitalize("PI_NAME"), sb.toString());

		readAndWriteToMap(row, map, new String[] { "PROGRAM_OFFICER_NAME", "PROJECT_START", "PROJECT_END", });

		List<String> list2 = new ArrayList<String>();
		for (Element elem : row.getChild("PROJECT_TERMSX").getChildren("TERM")) {
			list2.add("\"" + StringUtils.capitalize(elem.getText()) + "\"");
		}
		map.put(StringUtils.capitalize("PROJECT_TERMSX"), list2.toString());

		readAndWriteToMap(row, map,
				new String[] { "PROJECT_TITLE", "FUNDING_MECHANISM", "STUDY_SECTION_NAME" });

		return map;
	}

	public void readAndWriteToMap(Element row, Map<String, String> map, String... keys) {
		for (String key : keys) {
			String childText = row.getChildText(key);
			map.put(StringUtils.capitalize(key), StringUtils.capitalize(childText));
		}
	}


}
