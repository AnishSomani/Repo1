package com.aavishkar.news.ingest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.IOUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

public class XmlDocumentReadAndParse {
	public static void main(String ... args) throws IOException, JDOMException {
		String urlStr = "https://exporter.nih.gov/XMLData/final/RePORTER_PRJ_X_FY2016_037.zip";
		XmlDocumentReadAndParse parse = new XmlDocumentReadAndParse();
		String hostPort;
//		hostPort = "http://ec2-52-36-251-19.us-west-2.compute.amazonaws.com:9200/";
		hostPort = "http://localhost:9200";
		
		parse.processZipFile(hostPort, urlStr);
	}
	
	public void processZipFile(String hostPort, String urlStr) throws IOException, JDOMException {
		URL url = new URL(urlStr);
		String file2 = url.getFile();
		int lastIndexOf = file2.lastIndexOf('/');
		String name = file2.substring(lastIndexOf + 1);
		//Insert new document
		HttpURLConnection httpCon2 = (HttpURLConnection) url.openConnection();
		httpCon2.setDoOutput(true);
		httpCon2.setRequestMethod("GET");
		InputStream inputStream = httpCon2.getInputStream();
		File file = new File("src/main/resources/"+name);
		file.deleteOnExit();
		FileOutputStream stream = new FileOutputStream(file);
		IOUtils.copy(inputStream, stream);
		inputStream.close();
		stream.close();
		
		ZipFile zip = new ZipFile(file);
		Enumeration<? extends ZipEntry> entries = zip.entries();
		
		IngestNewsDocument ingestDoc = new IngestNewsDocument("data", "news", hostPort);
		
		while (entries.hasMoreElements()) {
			ZipEntry entry = entries.nextElement();
			if (entry.getName().endsWith(".xml")) {
				InputStream xmlStream = zip.getInputStream(entry);
				SAXBuilder saxBuilder = new SAXBuilder();

				Document document = saxBuilder.build(xmlStream);

				Element docElement = document.getRootElement();
				if (docElement.getName().equals("PROJECTS")) {
					List<Element> rowList = docElement.getChildren();
					int size = rowList.size();
					for (int index = 0; index < size; index++) {
						Element row = rowList.get(index);
						if (row.getName().equals("row")) {
							String applicationId = ingestDoc.ingestDocumentRow(row, false);
							System.out.println("Done "+(index + 1)+"/"+size+" documents.  Application id: "+applicationId);
						}
					}
				}

			}
		}
		zip.close();
	}

}
