package com.scorpio4.vendor.camel.component.any23;

import com.scorpio4.fact.FactSpace;
import com.scorpio4.oops.IQException;
import com.scorpio4.runtime.ExecutionEnvironment;
import org.apache.camel.Endpoint;
import org.apache.camel.component.bean.BeanEndpoint;
import org.apache.camel.component.bean.BeanProcessor;
import org.apache.camel.component.bean.ClassComponent;
import org.apache.camel.util.IntrospectionSupport;
import org.openrdf.repository.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Scorpio (c) 2014
 * Module: com.scorpio4.vendor.camel.component
 * @author lee
 * Date  : 24/06/2014
 * Time  : 2:43 PM
 */
public class Any23Component extends ClassComponent {
	static protected final Logger log = LoggerFactory.getLogger(Any23Component.class);
	ExecutionEnvironment engine;

	public Any23Component(ExecutionEnvironment engine) {
		this.engine=engine;
	}

	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		Map<String, Object> params = IntrospectionSupport.extractProperties(parameters, "any23.");
		log.debug("Any23: "+uri+" @ "+remaining);

		if (remaining.equals("body")) {
			// handle as a string body (NULL repository)
			return new BeanEndpoint(remaining, this, new BeanProcessor(new Any23Handler(null, params), getCamelContext()));
		} else if (remaining.equals("self") && engine!=null) {
			// write to own repository
			Repository repository = engine.getRepository();
			FactSpace factSpace = new FactSpace(engine.getIdentity(), repository);
			return new BeanEndpoint(remaining, this, new BeanProcessor(new Any23Handler(factSpace, params), getCamelContext()));
		} else if (remaining.contains(":") ) {
			Repository repository = engine.getRepositoryManager().getRepository(remaining);
			FactSpace factSpace = new FactSpace(remaining, repository);
			return new BeanEndpoint(remaining, this, new BeanProcessor(new Any23Handler(factSpace, params), getCamelContext()));
		} else throw new IQException("Any23 qualifier unknown: "+remaining);
	}
}
