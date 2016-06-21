package com.aavishkar.news.ingest;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.ParserConfigurationException;

import org.jdom2.JDOMException;
import org.xml.sax.SAXException;

public interface ParseProjectDocument {

	void processDocument(InputStream xmlStream, IngestNewsDocument ingestDoc, String message)
			throws JDOMException, IOException, SAXException, ParserConfigurationException;

}