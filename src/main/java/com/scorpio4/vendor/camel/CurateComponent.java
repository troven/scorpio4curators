package com.scorpio4.vendor.camel;

import com.scorpio4.ExecutionEnvironment;
import com.scorpio4.curate.Curator;
import com.scorpio4.curate.rdf.EmailCurator;
import com.scorpio4.curate.rdf.FileSystemCurator;
import com.scorpio4.curate.rdfs.BeanCurator;
import com.scorpio4.curate.rdfs.JDBCCurator;
import com.scorpio4.curate.rdfs.XLSCurator;
import com.scorpio4.vendor.camel.curate.CurateHandler;
import org.apache.camel.Endpoint;
import org.apache.camel.component.bean.BeanEndpoint;
import org.apache.camel.component.bean.ClassComponent;

import java.util.Map;

/**
 * scorpio4-oss (c) 2014
 * Module: com.scorpio4.vendor.camel.component
 * User  : lee
 * Date  : 6/07/2014
 * Time  : 5:46 PM
 */
public class CurateComponent extends ClassComponent {
	protected ExecutionEnvironment engine;

	public CurateComponent(ExecutionEnvironment engine) {
		this.engine=engine;
	}

	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		Curator curator = null;
		if (remaining.startsWith("email:")) {
			curator = new EmailCurator(parameters);
		} else if (remaining.startsWith("jdbc:")) {
			curator = new JDBCCurator(remaining);
		} else if (remaining.startsWith("file:")) {
			curator = new FileSystemCurator();
		} else if (remaining.startsWith("xls:")) {
			curator = new XLSCurator();
		} else if (remaining.startsWith("bean:")) {
			curator = new BeanCurator();
		}
		CurateHandler handler = new CurateHandler(this, curator, remaining, parameters);
		return (curator == null) ? null : new BeanEndpoint(uri, this, handler);
	}

	public ExecutionEnvironment getEngine() {
		return engine;
	}
}
