package com.scorpio4.curate.rdfs;

import com.scorpio4.util.Identifiable;
import com.scorpio4.util.string.PrettyString;
import org.apache.camel.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lee on 12/09/2014.
 */

@Converter
public class JDBCCuratorSupport implements Identifiable {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected String jdbcPrefix = "bean:"+getClass().getCanonicalName()+"#";
	protected String baseURI = null;

	protected String dot = ".", quote = "'";
	protected String catalog = null, schemaPattern = null, tablePattern = "%";
	protected String excludeRegEx = "sys|INFORMATION_SCHEMA";
	protected String pathSeparator = ":";
	protected long startTimestamp = System.currentTimeMillis();

	public JDBCCuratorSupport() {
	}

	@Override
	public String getIdentity() {
		return this.baseURI;
	}

	public void setIdentity(String baseURI) {
		if (baseURI.endsWith("#") || baseURI.endsWith("/") || baseURI.endsWith(":")) this.baseURI = baseURI;
		else this.baseURI = baseURI+"#";
	}


	public String getTableURI(String catalog, String schema, String table) {
		return globalize(catalog)+getPathSeparator()+PrettyString.sanitize(schema==null?"":schema)+getPathSeparator()+PrettyString.sanitize(table);
	}

	public String getColumnURI(String tableURI, String columnName) {
		return tableURI+ getPathSeparator()+sanitize(columnName);
	}

	public String globalize(String local) {
		return getIdentity()+sanitize(local);
	}

	public String sanitize(String text) {
		return PrettyString.sanitize(text);
	}

	public String quote(String text) {
		if (text==null) return "";
		return quote+text+quote;
	}

	public String prefix(String local) {
		return jdbcPrefix +PrettyString.lamaCase(local);
	}

	public String typeOf(String local) {
		return jdbcPrefix +PrettyString.camelCase(local);
	}

	public void setPrefix(String prefix) {
		this.jdbcPrefix = prefix;
	}

	public String getSchemaPattern() {
		return schemaPattern;
	}

	public void setSchemaPattern(String schemaPattern) {
		this.schemaPattern = schemaPattern;
	}

	public String getTablePattern() {
		return tablePattern;
	}

	public void setTablePattern(String tablePattern) {
		this.tablePattern = tablePattern;
	}

	public String getCatalog() {
		return catalog;
	}

	public void setCatalog(String catalogName) {
		this.catalog = catalogName;
	}

	public String getPathSeparator() {
		return pathSeparator;
	}

	public void setPathSeparator(String pathSeparator) {
		this.pathSeparator= pathSeparator;
	}

	public boolean isYes(String yorn) {
		return (yorn!=null&&(yorn.equalsIgnoreCase("YES")||yorn.equalsIgnoreCase("TRUE")));
	}

}
