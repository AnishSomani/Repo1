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
	private boolean deleteOld;
	private Map<String, String> abstractMap;

	public IngestNewsDocument(String indexName, String typeName, String hostPort, boolean deleteOld,
			Map<String, String> abstractMap) {
		this.indexName = indexName;
		this.typeName = typeName;
		this.hostPort = hostPort;
		this.deleteOld = deleteOld;
		this.abstractMap = abstractMap;
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

}
