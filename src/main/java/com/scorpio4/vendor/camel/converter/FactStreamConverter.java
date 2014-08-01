package com.scorpio4.vendor.camel.converter;

import com.scorpio4.fact.stream.FactStream;
import com.scorpio4.fact.stream.N3Stream;
import com.scorpio4.vocab.COMMONS;
import org.apache.camel.Converter;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;

/**
 * scorpio4-oss (c) 2014
 * Module: com.scorpio4.vendor.camel.converter
 * User  : lee
 * Date  : 31/07/2014
 * Time  : 10:43 PM
 */
@Converter
public class FactStreamConverter {

	public FactStreamConverter() {
	}

	@Converter
	public static FactStream toFactStream(GraphQueryResult result) throws QueryEvaluationException {
		N3Stream stream = new N3Stream("bean:"+FactStreamConverter.class.getCanonicalName());
		while (result.hasNext()) {
			Statement stmt = result.next();
			if (stmt.getObject() instanceof Literal) {
				Literal value = (Literal)stmt.getObject();
				String dataType = COMMONS.XSD+"string";
				if (value.getDatatype()!=null) dataType = value.getDatatype().toString();
				stream.fact(stmt.getSubject().toString(), stmt.getPredicate().toString(), value.getLabel(), dataType);
			} else {
				stream.fact(stmt.getSubject().toString(), stmt.getPredicate().toString(), stmt.getObject().toString());
			}
		}
		result.close();
		return stream;
	}

	@Converter
	public static FactStream toFactStream(GraphQuery query) throws QueryEvaluationException {
		return toFactStream(query.evaluate());
	}

}
