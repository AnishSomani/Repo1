package com.aavishkar.news.ingest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
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
		XmlDocumentReadAndParse parse = new XmlDocumentReadAndParse();

		List<String> list = parse.getAllXmlZips();

		String hostPort;
//		hostPort = "http://ec2-52-36-251-19.us-west-2.compute.amazonaws.com:9200/";
		hostPort = "http://localhost:9200";
		String indexName = "projects";
		String indexType = "nih";
		
		//String urlStr = "https://exporter.nih.gov/XMLData/final/RePORTER_PRJ_X_FY2016_037.zip";
		for (int index = 0; index < list.size(); index++) {
			String urlStr = "https://exporter.nih.gov/"+list.get(index);
			if (index < 24) {
				//We have already ingested first 23 xml docs.  
				continue;
			}
			if (urlStr.endsWith("https://exporter.nih.gov/XMLData/final/RePORTER_PRJ_X_FY2015.zip")) {
				parse.processZipFile(hostPort, indexName, indexType, urlStr, (index + 1), list.size());
			}
		}
	}
	
	public List<String> getAllXmlZips() throws IOException {
		File file = new File("src/main/resources/NihProjects.html");
		FileReader reader = new FileReader(file);
		String doc = IOUtils.toString(reader);
//		doc = "foobar <a href=\"XMLData/final/RePORTER_PRJ_X_FY2016_037.zip\"";
		int starting = 0;
		String substring = "<a href=\"XMLData/final/";
		int position = doc.indexOf(substring, starting);
		List<String> list = new ArrayList<String>();
		while (position > 0) {
			int ending = doc.indexOf("\"", position+substring.length());
			String xml = doc.substring(position + 9, ending);
			list.add(xml);
			starting = ending;
			position = doc.indexOf(substring, starting);
		}
		return list;
	}
	
	public void processZipFile(String hostPort, String indexName, String indexType, String urlStr, int fileNumber, int totalFiles) 
			throws IOException, JDOMException {
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
		
		IngestNewsDocument ingestDoc = new IngestNewsDocument(indexName, indexType, hostPort);
		
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
							System.out.println("Ingested "+(index + 1)+"/"+size+" documents in "+fileNumber+"/"+totalFiles+
									" files. Application id: "+applicationId);
						}
					}
				}

			}
		}
		zip.close();
	}

}
