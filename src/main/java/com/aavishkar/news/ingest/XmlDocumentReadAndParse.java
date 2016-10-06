package com.aavishkar.news.ingest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.jdom2.JDOMException;
import org.xml.sax.SAXException;

public class XmlDocumentReadAndParse {
	public static void main(String ... args) throws IOException, JDOMException, SAXException, ParserConfigurationException {
		long begin = System.currentTimeMillis();
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
				parse.processZipFile(hostPort, indexName, indexType, urlStr, (index + 1), list.size());
			
//			if (urlStr.endsWith("https://exporter.nih.gov/XMLData/final/RePORTER_PRJ_X_FY2015.zip")) {
//				parse.processZipFile(hostPort, indexName, indexType, urlStr, (index + 1), list.size());
//			}
		}
		
		System.out.println("It took "+(System.currentTimeMillis() - begin)+ " ms to complete it");
	}
	
	public List<String> getAllXmlZips() throws IOException {
		String doc = readURL("https://exporter.nih.gov/ExPORTER_Catalog.aspx?sid=1&index=0");
		//File file = new File("src/main/resources/NihProjects.html");
		//FileReader reader = new FileReader(file);
		//String doc = IOUtils.toString(reader);
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
		Collections.reverse(list);
		return list;
	}
	
	public void processZipFile(String hostPort, String indexName, String indexType, String urlStr, int fileNumber, int totalFiles) 
			throws IOException, JDOMException, SAXException, ParserConfigurationException {
			
		InputStream xmlStream = getZipInputSteamForXml(urlStr);
		Map<String, String> abstractMap = getAbstractMap(urlStr);
		
		if (xmlStream != null && abstractMap != null) {
			//ParseProjectDocument dom = new ParseProjectDocumentDOM();
			 ParseProjectDocumentSAX parser = new ParseProjectDocumentSAX();
			IngestNewsDocument ingestDoc = new IngestNewsDocument(indexName, indexType, hostPort, false, abstractMap);
			System.out.println("Total number of documents in "+urlStr+" are: "+abstractMap.size());
			parser.processDocument(xmlStream, ingestDoc, "In file "+fileNumber+" of "+totalFiles+".");

		} else {
			System.err.println("No xmlstream found for "+urlStr);
		}
		
	}
	
	private Map<String, String> getAbstractMap(String urlStr) throws IOException, SAXException, ParserConfigurationException {
		String abstractStr = urlStr.replace("_PRJ_X_", "_PRJABS_X_");
		InputStream xmlStream = getZipInputSteamForXml(abstractStr);
		if (xmlStream != null) {
			Map<String, String> map = new AbstractParseDocument().processDocument(xmlStream);
			return map;
		}
		return null;
	}
	
	private String readURL(String urlStr) throws MalformedURLException, IOException{
		URL url = new URL(urlStr);
		//Insert new document
		HttpURLConnection httpCon2 = (HttpURLConnection) url.openConnection();
		httpCon2.setDoOutput(true);
		httpCon2.setRequestMethod("GET");
		InputStream inputStream = httpCon2.getInputStream();
		StringWriter writer = new StringWriter();
		IOUtils.copy(inputStream, writer);
		inputStream.close();
		writer.close();
		return writer.toString();
	};
	
	private InputStream getZipInputSteamForXml(String urlStr) throws IOException {
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
		while (entries.hasMoreElements()) {
			ZipEntry entry = entries.nextElement();
			if (entry.getName().endsWith(".xml")) {
				InputStream xmlStream = zip.getInputStream(entry);
				return xmlStream;
			}
		}
		
		zip.close();
		return null;
	}

}
