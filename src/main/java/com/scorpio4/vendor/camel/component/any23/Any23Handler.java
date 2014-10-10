package com.scorpio4.vendor.camel.component.any23;

import com.scorpio4.fact.FactSpace;
import org.apache.any23.Any23;
import org.apache.any23.ExtractionReport;
import org.apache.any23.configuration.DefaultConfiguration;
import org.apache.any23.configuration.ModifiableConfiguration;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.extractor.ExtractionParameters;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.source.StringDocumentSource;
import org.apache.any23.writer.*;
import org.apache.camel.Exchange;
import org.apache.camel.Handler;
import org.apache.camel.InvalidPayloadException;
import org.apache.camel.util.ExchangeHelper;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Scorpio (c) 2014
 * Module: com.scorpio4.vendor.camel.component.any23
 * @author lee
 * Date  : 24/06/2014
 * Time  : 10:29 PM
 */
public class Any23Handler  {
	static protected final Logger log = LoggerFactory.getLogger(Any23Handler.class);
	Map<String, Object> params;
	ModifiableConfiguration any23Conf = DefaultConfiguration.copy();

	FactSpace factSpace;

	public Any23Handler(FactSpace factSpace, Map<String, Object> params) {
		this.factSpace= factSpace;
		this.params=params;

		any23Conf.setProperty("any23.http.user.agent.default", "x-scorpio4-any23");
		any23Conf.setProperty("any23.http.client.timeout", "5000");
		any23Conf.setProperty("any23.http.client.max.connections", "3");
		for(String name: params.keySet()) {
			any23Conf.setProperty("any23."+name, ""+params.get(name));
		}
	}

	@Handler
	public void handle(Exchange exchange) throws Exception {
		log.debug("ANY23: "+exchange);
		Any23 any23 = new Any23(any23Conf);

		String contentType = ExchangeHelper.getContentType(exchange);
		String contextURI = factSpace==null?exchange.getFromEndpoint().getEndpointUri()+"#"+exchange.getExchangeId():factSpace.getIdentity()+"#"+exchange.getExchangeId();

		log.debug("\tmime: "+contentType);
		log.debug("\tfrom: "+contextURI);

		if (factSpace==null) handleBody(any23, contextURI, exchange);
		else handleFactSpace(any23, contextURI, exchange);

	}

	private void handleBody(Any23 any23, String contextURI, Exchange exchange) throws TripleHandlerException, UnsupportedEncodingException, RepositoryConfigException, InvalidPayloadException {
		String body = exchange.getIn().getMandatoryBody(String.class);
		String contentType = ExchangeHelper.getContentType(exchange);
		contentType = contentType==null?(String) params.get(Exchange.CONTENT_TYPE):contentType;

		log.debug("\thead: "+exchange.getIn().getHeaders());

		DocumentSource source = new StringDocumentSource(body, contextURI, contentType);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		TripleHandler handler = new NTriplesWriter(out);
		ExtractionParameters extractionParameters = ExtractionParameters.newDefault();

		try {
			ExtractionReport extracted = any23.extract(extractionParameters, source, handler);
			log.debug("Extracted: "+extracted);
		} catch(Exception e) {
			log.error("Extraction ("+contentType+") failed @ "+contextURI, e);
		} finally {
			handler.close();
		}
		exchange.getOut().setBody(out.toString("UTF-8"));

	}

	private void handleFactSpace(Any23 any23, String contextURI, Exchange exchange) throws RepositoryConfigException, RepositoryException, IOException, ExtractionException, InvalidPayloadException {
		String body = exchange.getIn().getMandatoryBody(String.class);
		String contentType = ExchangeHelper.getContentType(exchange);
		contentType = contentType==null?(String) params.get(Exchange.CONTENT_TYPE):contentType;
		log.debug("\tpersist: "+contentType+" @ "+contextURI);

		RepositoryConnection connection = factSpace.getConnection();
		connection.begin();
		DocumentSource source = new StringDocumentSource(body, contextURI, contentType);

		ExtractionParameters extractionParameters = ExtractionParameters.newDefault();
		extractionParameters.setProperty(extractionParameters.EXTRACTION_CONTEXT_URI_PROPERTY, contextURI);

		RepositoryWriter repositoryWriter = new RepositoryWriter(connection, new URIImpl(contextURI));
		ExtractionReport extracted = any23.extract(extractionParameters, source, new ReportingTripleHandler(repositoryWriter));
		log.debug("Extracted: "+extracted);

		connection.commit();
		connection.close();
	}


}


