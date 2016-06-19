package com.aavishkar.news.ingest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import com.google.gson.Gson;

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

			for (Element row : rowList) {
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

					sb.append(capitalize(childText1));
				}

				map.put(capitalize("PI_NAME"), sb.toString());

				readAndWriteToMap(row, map, new String[] { "PROGRAM_OFFICER_NAME", "PROJECT_START", "PROJECT_END", });

				List<String> list2 = new ArrayList<String>();
				for (Element elem : row.getChild("PROJECT_TERMSX").getChildren("TERM")) {
					list2.add("\"" + capitalize(elem.getText()) + "\"");
				}
				map.put(capitalize("PROJECT_TERMSX"), list2.toString());

				readAndWriteToMap(row, map,
						new String[] { "PROJECT_TITLE", "FUNDING_MECHANISM", "STUDY_SECTION_NAME" });

				storeToElasticSearch("data", "news", map);

				// System.out.println(map);

				map.clear();
			}
		} catch (JDOMException e) {
			e.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private static void storeToElasticSearch(String index, String type, Map<String, String> data) throws IOException {
		String hostPort = "http://localhost:9200";
		String applicationId = data.get("Application_Id");
		if (applicationId == null) {
			System.out.println("Application id missing for " + data);
			return;
		}

		URL url = new URL(hostPort + "/" + index + "/" + type + "/" + applicationId);
		
		//Delete existing document
		HttpURLConnection httpCon1 = (HttpURLConnection) url.openConnection();
		httpCon1.setDoOutput(true);
		httpCon1.setRequestMethod("DELETE");
		OutputStreamWriter out1 = new OutputStreamWriter(httpCon1.getOutputStream());
		out1.close();
		try {
			httpCon1.getInputStream();
		} catch (FileNotFoundException e) {
			//Ignore, document doesn't exist
		}
		
		//Insert new document
		HttpURLConnection httpCon2 = (HttpURLConnection) url.openConnection();
		httpCon2.setDoOutput(true);
		httpCon2.setRequestMethod("PUT");
		OutputStreamWriter out2 = new OutputStreamWriter(httpCon2.getOutputStream());
		Gson gson = new Gson();
		String string = gson.toJson(data);
		out2.write(string);
		out2.close();
		httpCon2.getInputStream();

		System.out.println("Completed ingest of application id: " + applicationId);
	}

	public static void readAndWriteToMap(Element row, Map<String, String> map, String... keys) {
		for (String key : keys) {
			String childText = row.getChildText(key);
			map.put(capitalize(key), capitalize(childText));
		}
	}

	public static String capitalize(String name) {
		if (name == null) {
			return null;
		}

		name = name.toLowerCase();
		StringBuilder name1 = new StringBuilder();
		for (int x = 0; x < name.length(); x++) {
			if (x == 0 || name.charAt(x - 1) == ' ' || name.charAt(x - 1) == '\t' || name.charAt(x - 1) == '\n'
					|| name.charAt(x - 1) == '_') {
				name1.append(Character.toUpperCase(name.charAt(x)));
			} else {
				name1.append(name.charAt(x));
			}
			;
		}
		;
		return name1.toString();
	}
}