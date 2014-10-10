package com.scorpio4.vendor.camel.curate;

import com.scorpio4.curate.Curator;
import com.scorpio4.fact.stream.FactStream;
import com.scorpio4.oops.FactException;
import com.scorpio4.oops.IQException;
import com.scorpio4.vendor.camel.CurateComponent;
import com.scorpio4.vendor.sesame.stream.SesameStreamWriter;
import org.apache.camel.Exchange;
import org.apache.camel.Handler;
import org.apache.camel.Message;
import org.apache.camel.component.bean.BeanProcessor;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

/**
 * scorpio4-oss (c) 2014
 * Module: com.scorpio4.vendor.camel.component.curate
 * @author lee
 * Date  : 6/07/2014
 * Time  : 5:49 PM
 */
public class CurateHandler extends BeanProcessor {
	private static final Logger log = LoggerFactory.getLogger(CurateHandler.class);
	private CurateComponent component;
	private Curator curator;
	private String streamSpec;
	private Map<String, Object> parameters;

	public CurateHandler(CurateComponent component, Curator curator, String streamSpec, Map<String, Object> parameters) {
		super(curator, component.getCamelContext());
		this.component=component;
		this.curator=curator;
		this.streamSpec=streamSpec;
		this.parameters=parameters;
	}

	@Handler
	public void handle(Exchange exchange) throws MalformedQueryException, RepositoryException, QueryResultHandlerException, QueryEvaluationException, IOException, FactException, IQException, RepositoryConfigException, SQLException {
		Message in = exchange.getIn();
		if (curator!=null) {
			doCurate(curator, in.getBody());
		}
	}

	private void doCurate(Curator curator, Object body) throws RepositoryException, RepositoryConfigException, FactException, IQException, SQLException {
		if (!curator.canCurate(body)) return;
		String identity = component.getEngine().getIdentity();
		log.debug("Curated: "+curator.getClass().getSimpleName()+" @ "+identity+"\n"+body);
		Repository repository = component.getEngine().getRepositoryManager().getRepository(identity);
		RepositoryConnection connection = repository.getConnection();
		FactStream stream = new SesameStreamWriter(connection, identity);
		connection.begin();
		curator.curate(stream, body);
		connection.commit();
		connection.close();
	}
}
