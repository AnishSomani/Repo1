package com.aavishkar.news.ingest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;

public class IngestNewsDocument {

	private final String indexName;
	private final String typeName;
	private final String hostPort;
	private final boolean deleteOld;
	private final boolean useBatch;
	private Map<String, String> abstractMap;
	private int currentDoc;
	private StringBuilder builder;

	public IngestNewsDocument(String indexName, String typeName, String hostPort, boolean deleteOld,
			Map<String, String> abstractMap) {
		this.indexName = indexName;
		this.typeName = typeName;
		this.hostPort = hostPort;
		this.deleteOld = deleteOld;
		this.abstractMap = abstractMap;
		this.useBatch = true;
		this.currentDoc = 0;
		builder = new StringBuilder();
	}

	public String storeToElasticSearch(LinkedHashMap<String, String> data) throws IOException {
		String applicationId = data.get("Application_Id");
		if (applicationId == null) {
			System.out.println("Application id missing for " + data);
			return null;
		}
		String abstractText = abstractMap.get(applicationId);
		if (abstractText != null) {
			data.put("Abstract_Text", abstractText);
//			System.out.println(data);
		}
		
		if (useBatch) {
			if (deleteOld) {
				builder.append("{ \"delete\" : { \"_index\" : \""+indexName+"\", \"_type\" : \""+typeName+"\", \"_id\" : \""+applicationId+"\" } }");
				builder.append("\n");
			}
			builder.append("{ \"index\" : { \"_index\" : \""+indexName+"\", \"_type\" : \""+typeName+"\", \"_id\" : \""+applicationId+"\" } }");
			builder.append("\n");
			Gson gson = new Gson();
			String value = gson.toJson(data);
			builder.append(value);
			builder.append("\n");
			currentDoc++;
			if (currentDoc % 1000 == 0) {
				System.out.println("*** Batch ingesting "+currentDoc+" documents");
				URL url = new URL(hostPort + "/_bulk");
				currentDoc = 0;
				httpWrite(url, "POST", builder.toString());
				builder = new StringBuilder();
			}
		} else {
			URL url = new URL(hostPort + "/" + indexName + "/" + typeName + "/" + applicationId);
			if (deleteOld) {
				// Delete existing document
				HttpURLConnection httpCon1 = (HttpURLConnection) url.openConnection();
				httpCon1.setDoOutput(true);
				httpCon1.setRequestMethod("DELETE");
				try {
					InputStream inputStream = httpCon1.getInputStream();
					IOUtils.toByteArray(inputStream);
					IOUtils.closeQuietly(inputStream);
				} catch (FileNotFoundException e) {
					// Ignore, document doesn't exist
				}
			}
			// Insert new document
			Gson gson = new Gson();
			String string = gson.toJson(data);
			httpWrite(url, "PUT", string);
		}

		return applicationId;
	}
	
	private void httpWrite(URL url, String method, String data) throws IOException {
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod(method);
		OutputStreamWriter out = new OutputStreamWriter(httpCon.getOutputStream(), "UTF-8");
		out.write(data);
		out.close();
		InputStream inputStream = httpCon.getInputStream();
		IOUtils.toByteArray(inputStream);
		IOUtils.closeQuietly(inputStream);
	}
	
	public void finish() throws IOException {
		if (useBatch && currentDoc > 0) {
			System.out.println("*** Batch ingesting "+currentDoc+" documents");
			URL url = new URL(hostPort + "/_bulk");
			currentDoc = 0;
			httpWrite(url, "POST", builder.toString());
			builder = new StringBuilder();
		}
	}

}
