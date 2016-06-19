package com.aavishkar.news.ingest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.jdom2.Element;

import com.google.gson.Gson;

public class IngestNewsDocument {
	
	private final String indexName;
	private final String typeName;
	private final String hostPort;

	public IngestNewsDocument(String indexName, String typeName, String hostPort) {
		this.indexName = indexName;
		this.typeName = typeName;
		this.hostPort = hostPort;
	}
	
	public String ingestDocumentRow(Element row, boolean delete) throws IOException {
		Map<String, String> map = new LinkedHashMap<String, String>();

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

		String applicationId = storeToElasticSearch(map, delete);

		map.clear();
		
		return applicationId;
	}

	public String storeToElasticSearch(Map<String, String> data, boolean delete) throws IOException {
		String applicationId = data.get("Application_Id");
		if (applicationId == null) {
			System.out.println("Application id missing for " + data);
			return null;
		}
	
		URL url = new URL(hostPort + "/" + indexName + "/" + typeName + "/" + applicationId);
		
		if (delete) {
			//Delete existing document
			HttpURLConnection httpCon1 = (HttpURLConnection) url.openConnection();
			httpCon1.setDoOutput(true);
			httpCon1.setRequestMethod("DELETE");
			try {
				InputStream inputStream = httpCon1.getInputStream();
				IOUtils.toByteArray(inputStream);
				IOUtils.closeQuietly(inputStream);
			} catch (FileNotFoundException e) {
				//Ignore, document doesn't exist
			}
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
		InputStream inputStream = httpCon2.getInputStream();
		IOUtils.toByteArray(inputStream);
		IOUtils.closeQuietly(inputStream);
	
		return applicationId;
	}

	public void readAndWriteToMap(Element row, Map<String, String> map, String... keys) {
		for (String key : keys) {
			String childText = row.getChildText(key);
			map.put(StringUtils.capitalize(key), StringUtils.capitalize(childText));
		}
	}

}
