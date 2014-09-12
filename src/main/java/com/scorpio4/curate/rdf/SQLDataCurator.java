package com.scorpio4.curate.rdf;

import com.scorpio4.curate.Curator;
import com.scorpio4.curate.rdfs.JDBCCuratorSupport;
import com.scorpio4.fact.stream.FactStream;
import com.scorpio4.oops.FactException;
import com.scorpio4.oops.IQException;
import com.scorpio4.util.IdentityHelper;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by lee on 11/09/2014.
 */

public class SQLDataCurator extends JDBCCuratorSupport implements Curator {

	public SQLDataCurator(String baseURI) throws IQException, FactException {
		setIdentity(baseURI);
	}


	public SQLDataCurator(String baseURI, Connection connection, FactStream learner) throws IQException, FactException, SQLException {
		setIdentity(baseURI);
		curate(learner, connection);
	}

	@Override
	public void curate(FactStream stream, Object curated) throws FactException, IQException{
		if (!canCurate(curated)) throw new IQException("Can't Curate "+curated);
		try {
			curate(stream, (Connection)curated);
		} catch (SQLException e) {
			throw new IQException("SQL Curate Failed",e);
		}
	}

	public void curate(FactStream stream, Connection connection) throws FactException, IQException, SQLException {
		DatabaseMetaData metaData = connection.getMetaData();
		if (getIdentity()==null) setIdentity(metaData.getURL());

		if (getCatalog()!=null) {
			curateCatalog(stream, metaData, getCatalog());
		} else {
			curateCatalogs(stream, metaData);
		}
	}


	protected void curateCatalogs(FactStream learn, DatabaseMetaData metaData) throws SQLException, FactException, IQException {
		ResultSet catalogs = metaData.getCatalogs();
		log.debug("Curating catalogs: " + metaData.getURL());
		int count = 0;
		while(catalogs.next()) {
			String catalog = catalogs.getString("TABLE_CAT");
			curateCatalog(learn, metaData, catalog);
			count++;
		}
		catalogs.close();
		log.debug("SQL Curated "+count+" catalogs in "+((System.currentTimeMillis()-startTimestamp)/1000)+"s");
	}

	private void curateCatalog(FactStream learn, DatabaseMetaData metaData, String catalog) throws FactException, SQLException, IQException {
		String[] tableTypes = null;
		String catalogURI = globalize(catalog);

		log.debug("Curating catalog: " + catalog+" @ "+catalogURI);

		ResultSet tables = metaData.getTables(catalog, getSchemaPattern(), getTablePattern(), tableTypes);
		int count = 0;
		while(tables.next()) {
			String schema = tables.getString("TABLE_SCHEM");
			if (schema==null) schema = "";
			if ( !Pattern.matches(excludeRegEx, schema) ) {
				curate(learn, metaData, tables);
				count++;
			} else {
				log.trace("Skip excluded schema: "+schema);
			}
		}
		log.debug("SQL Curated "+count+" tables in "+((System.currentTimeMillis()-startTimestamp)/1000)+"s");
		tables.close();
	}

	private void curate(FactStream learn, DatabaseMetaData metaData, ResultSet tables) throws SQLException, FactException, IQException {
		String catalog = tables.getString("TABLE_CAT");
		String schema = tables.getString("TABLE_SCHEM");
		String table = tables.getString("TABLE_NAME");
		curate(learn, metaData, catalog, schema, table);
	}


	public void curate(FactStream learn, DatabaseMetaData metaData, String catalog, String schema, String table) throws FactException, IQException, SQLException {

		Map<String,String> fkeys = getImportedKeyMap(metaData, catalog, schema, table);

		Collection<String> pkeys = getPrimaryKeys(metaData, catalog, schema, table);

		String sql = "SELECT * FROM "+table;
		Connection connection = metaData.getConnection();
		PreparedStatement statement = connection.prepareStatement(sql);
		ResultSet resultSet = statement.executeQuery();
		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
		while(resultSet.next()) {
			curate(learn, metaData, catalog, schema, table, resultSetMetaData, resultSet, pkeys, fkeys);
		}
		resultSet.close();
	}

	private Collection<String> getPrimaryKeys(DatabaseMetaData metaData, String catalog, String schema, String table) throws SQLException {
		Collection<String> pkeys = new ArrayList();
		ResultSet primaryKeys = metaData.getPrimaryKeys(catalog, schema, table);
		while(primaryKeys.next()) {
			pkeys.add(primaryKeys.getString("COLUMN_NAME"));
		}
		return pkeys;
	}

	private Map<String, String> getImportedKeyMap(DatabaseMetaData metaData, String catalog, String schema, String table) throws SQLException {
		Map<String,String> fkeys = new HashMap();
		ResultSet exportedKeys = metaData.getImportedKeys(catalog, schema, table);
		while(exportedKeys.next()) {
			String pktable_cat = exportedKeys.getString("PKTABLE_CAT");
			String pktable_schem = exportedKeys.getString("PKTABLE_SCHEM");
			String pktable_name = exportedKeys.getString("PKTABLE_NAME");
			String pkcolumn_name = exportedKeys.getString("PKCOLUMN_NAME");
			String tableURI = getTableURI(pktable_cat, pktable_schem, pktable_name);
			String fkcolumn_name = exportedKeys.getString("FKCOLUMN_NAME");
			String columnURI = tableURI+"@"+pkcolumn_name+"=";
			fkeys.put(fkcolumn_name, columnURI);
		}
		return fkeys;
	}

	public void curate(FactStream learn, DatabaseMetaData metaData, String catalog, String schema, String table, ResultSetMetaData resultSetMetaData, ResultSet resultSet, Collection<String> pkeys, Map<String, String> fkeys) throws SQLException, FactException {
		String tableURI = getTableURI(catalog, schema, table);

		//pkeys
		String rowURI = getRowURI(tableURI, pkeys, resultSet);
		for (int i=1;i<resultSetMetaData.getColumnCount();i++) {
			String columnName = resultSetMetaData.getColumnName(i);
			String columnURI = getColumnURI(tableURI, columnName);

//			String fkURI = fkeys.get(columnName);
			String fkURI = null;
			String value = resultSet.getString(i);
			if (fkURI!=null) {
//				log.debug("FK: "+table+" @ "+columnName+" -> "+fkURI);
				if (value!=null) learn.fact(rowURI, columnURI, fkURI+ value);
			} else {
				String columnClassName = resultSetMetaData.getColumnTypeName(i);
				String typeURI = "bean:com.scorpio4.curate.rdfs.JDBCCurator#"+columnClassName;
				learn.fact(rowURI, columnURI, value, typeURI);
			}
		}
	}

	private String getRowURI(String tableURI, Collection<String> pkeys, ResultSet resultSet) throws SQLException {
		String rowURI = null;
		if (pkeys == null || pkeys.isEmpty()) {
			rowURI = IdentityHelper.uuid(tableURI+"@");
		} else {
			StringBuilder keyURI = new StringBuilder();
			for(String pKey: pkeys) {
				if (keyURI.length()!=0) keyURI.append(";");
				keyURI.append(pKey).append("=").append(resultSet.getString(pKey));
			}
			rowURI = tableURI+"@"+keyURI.toString();
		}
		return rowURI;
	}

	@Override
	public boolean canCurate(Object curated) {
		if (curated==null) return false;
		return Connection.class.isInstance(curated);
	}

}
