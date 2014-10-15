package com.scorpio4.curate.rdfs;
/*
 *   Scorpio4 - CONFIDENTIAL
 *   Copyright (c) 2009-2014 Lee Curtis, All Rights Reserved.
 *
 *
 */
import com.scorpio4.curate.Curator;
import com.scorpio4.fact.stream.FactStream;
import com.scorpio4.fact.stream.N3Stream;
import com.scorpio4.oops.FactException;
import com.scorpio4.oops.IQException;
import com.scorpio4.util.Identifiable;
import com.scorpio4.util.string.PrettyString;
import org.apache.camel.Converter;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.RDFS;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Scorpio4 (c) Lee Curtis 2012-2013
 * @author lee
 * Date: 14/03/13
 * Time: 9:44 AM
 * <p/>
 * Generates N3 representation of a JDBC Connection's DatabaseMetaData
 */
@Converter
public class JDBCCurator extends JDBCCuratorSupport implements Curator, Identifiable {
	public JDBCCurator() {
	}

    public JDBCCurator(String baseURI) {
        setIdentity(baseURI);
    }

	public JDBCCurator(String baseURI, Connection connection, FactStream learner) throws IQException, FactException {
		setIdentity(baseURI);
		curate(learner, connection);
	}

	public JDBCCurator(Connection connection, FactStream learner) throws IQException, SQLException, FactException {
		String uri = connection.getMetaData().getURL();
		setIdentity(uri);
		if (learner!=null) curate(learner, connection);
	}

    @Override
    public void curate(FactStream learn, Object curated) throws FactException, IQException {
        if (!canCurate(curated)) throw new IQException("urn:scorpio4:curate:rdfs:jdbc:oops:cant-curate#"+(curated==null?"null":curated.getClass()));
        try {
	        Connection connection = (Connection)curated;
            DatabaseMetaData metaData = connection.getMetaData();
	        if (getIdentity()==null) setIdentity(metaData.getURL());
            quote = metaData.getIdentifierQuoteString();
            curateDatabase(learn, metaData);
	        if (getCatalog()!=null) {
		        curateCatalog(learn, metaData, getCatalog());
	        } else {
		        curateCatalogs(learn, metaData);
	        }
        } catch (SQLException e) {
            throw new IQException("SQL failed: "+e.getMessage(), e);
        }
    }

    @Override
    public boolean canCurate(Object curated) {
        if (curated==null) return false;
        return Connection.class.isInstance(curated);
    }

/*

def TYPES = [
	"VARCHAR": "string", "MEDIUMTEXT": "string", "TEXT": "string", "LONGTEXT": "string",
	"DATE":"date", "DATETIME":"datetime",
	"TIME":"time", "TIMESTAMP":"time",
	"BIT": "integer", "BOOLEAN": "boolean",
	"DOUBLE UNSIGNED": "decimal", "DOUBLE":"decimal",
	"FLOAT UNSIGNED": "decimal", "FLOAT": "decimal",
	"INT":"integer", "SMALLINT UNSIGNED": "integer", "TINYINT UNSIGNED": "integer", "INT UNSIGNED": "integer",
	"ENUM":"lookup"
]
*/

	protected void curateDatabase(FactStream learn, DatabaseMetaData metaData) throws SQLException, FactException {
		String metaURI = getIdentity();

        learn.fact(metaURI, A, typeOf("Database") );
        log.debug("Curating SQL database: "+metaURI);
        learn.fact(metaURI, prefix("product"),metaData.getDatabaseProductName(), "string");
        learn.fact(metaURI, prefix("version"),metaData.getDatabaseProductVersion(), "string");
        learn.fact(metaURI, prefix("ansi92Entry"),metaData.supportsANSI92EntryLevelSQL(), "boolean");
        learn.fact(metaURI, prefix("ansi92Entry"),metaData.supportsANSI92EntryLevelSQL(), "boolean");
        learn.fact(metaURI, prefix("ansi92Full"),metaData.supportsANSI92FullSQL(), "boolean");
        learn.fact(metaURI, prefix("ansi92Intermediate"),metaData.supportsANSI92IntermediateSQL(), "boolean");
	}

	protected void curateCatalogs(FactStream learn, DatabaseMetaData metaData) throws SQLException, FactException {

        ResultSet catalogs = metaData.getCatalogs();
		log.debug("Curating catalogs: " + metaData.getURL());
		int count = 0;
        while(catalogs.next()) {
            String catalog = catalogs.getString("TABLE_CAT");
	        curateCatalog(learn, metaData, catalog);
	        count++;
        }
        catalogs.close();
        log.debug("JDBC Curated "+count+" catalogs");
	}

	private void curateCatalog(FactStream learn, DatabaseMetaData metaData, String catalog) throws FactException, SQLException {
		String[] tableTypes = null;
		String catalogURI = globalize(catalog);

		log.debug("Curating catalog: " + catalog+" @ "+catalogURI);

		learn.fact(getIdentity(), prefix("catalog"), catalogURI);
		learn.fact(catalogURI, A, typeOf("Catalog"));
		learn.fact(catalogURI, prefix("name"), catalog, "string");
		learn.fact(catalogURI, LABEL, PrettyString.humanize(catalog), "string");
		Map seenSchema = new HashMap();
		ResultSet tables = metaData.getTables(catalog, getSchemaPattern(), getTablePattern(), tableTypes);
		int count = 0;
		while(tables.next()) {
			// handle schema (including NULL schema)
			String schema = tables.getString("TABLE_SCHEM");
			if (schema==null) schema = "";
			if ( !Pattern.matches(excludeRegEx, schema) ) {
				if (!seenSchema.containsKey(schema)) {
					curateSchema(learn, catalog, catalogURI, schema);
					seenSchema.put(schema, true);
				}
				curateTable(learn, metaData, tables);
				count++;
			} else {
				log.debug("Skip excluded schema: "+schema);
			}
		}
		log.debug("JDBC Curated "+count+" tables");
		tables.close();
	}

	protected void curateSchema(FactStream learn, String catalog, String catalogURI, String schema) throws SQLException, FactException {
        if (schema==null) schema = "";
        String schemaURI = catalogURI+getPathSeparator()+schema;

        if (schema.equals("")) log.debug("Curating "+catalog+" Default Schema");
        else log.debug("Curating "+catalog+" Schema: "+schema );

		String label = schema.equals("") ? "default" : PrettyString.humanize(schema);

        learn.fact(catalogURI, prefix("schema"), schemaURI);
		learn.fact(schemaURI, prefix("name"), schema.equals("")?"default":schema, "string");
        learn.fact(schemaURI, A, typeOf("Schema") );
        learn.fact(schemaURI, LABEL, label, "string");
        learn.fact(schemaURI, DCTERMS.MODIFIED.stringValue(), "" + startTimestamp, "string");
        learn.fact(schemaURI, CURATED_BY, "bean:"+getClass().getCanonicalName() );
    }

    protected void curateTable(FactStream learn, DatabaseMetaData metaData, ResultSet tableMeta) throws SQLException, FactException {
        String catalog = tableMeta.getString("TABLE_CAT");
        String catalogURI = globalize(catalog);

        String schema = tableMeta.getString("TABLE_SCHEM");
	    boolean hasSchema = (schema!=null)&&!schema.equals("");
        if (schema==null) schema = "";
        String schemaURI = catalogURI+":"+schema;

        String tableName = tableMeta.getString("TABLE_NAME");

//        String tableID = sanitize(tableName);
        String tableType = PrettyString.camelCase(tableMeta.getString("TABLE_TYPE"));
        String label = PrettyString.humanize(tableName).trim();
        String comments = tableMeta.getString("REMARKS");
        if (comments == null ||comments.trim().equals("")) comments = PrettyString.humanize(catalog)+" "+(PrettyString.humanize(schema)+" "+label+" "+PrettyString.lamaCase(tableType)).trim();
        else comments = comments.trim();

        String tableURI = getTableURI(catalog, schema, tableName);

        String fqSchema = hasSchema?quote(schema)+dot:"";
        String fqTable = fqSchema+quote(tableName);

        log.debug("Curating table: " + catalog+" "+tableName);

	    learn.fact(catalogURI, prefix("schema"), schemaURI);
	    learn.fact(schemaURI, prefix("table"), tableURI);

	    learn.fact(tableURI, A, typeOf(tableType));
	    learn.fact(tableURI, prefix("name"), tableName, "string");
        learn.fact(tableURI, LABEL, label, "string");
        learn.fact(tableURI, COMMENT, comments, "string");

        learn.fact(tableURI, prefix("name"), tableName, "string");
        learn.fact(tableURI, RDFS.ISDEFINEDBY.stringValue(), schemaURI);
        curateColumns(learn, metaData, catalog, schema, tableName, tableURI, schemaURI);
	    curatePrimaryKeys(learn, metaData, catalog, schema, tableName, tableURI, schemaURI);
        curateConstraints(learn, metaData, catalog, schema, tableName, tableURI, schemaURI);
	    curateUniqueIndices(learn, metaData, catalog, schema, tableName, tableURI, schemaURI);
    }

	private void curatePrimaryKeys(FactStream learn, DatabaseMetaData metaData, String catalog, String schema, String tableName, String tableURI, String schemaURI) throws SQLException, FactException {
		ResultSet rs = metaData.getPrimaryKeys(catalog, schema, tableName);
		while(rs.next()) {
			String keySeq = rs.getString("KEY_SEQ");
			String columnName = rs.getString("COLUMN_NAME");
			if (columnName!=null) {
				String columnURI = tableURI+ getPathSeparator()+sanitize(columnName);
				learn.fact(tableURI, prefix("primaryKey"), columnURI);
			}
		}
	}

	private void curateUniqueIndices(FactStream learn, DatabaseMetaData metaData, String catalog, String schema, String tableName, String tableURI, String schemaURI) throws SQLException, FactException {
		ResultSet rs = metaData.getIndexInfo(catalog, schema, tableName, true, true);
		while(rs.next()) {
			String indexName = rs.getString("INDEX_NAME");
			String table = rs.getString("TABLE_NAME");
			String columnName = rs.getString("COLUMN_NAME");
			if (columnName!=null) {
				String columnURI = tableURI+ getPathSeparator()+sanitize(columnName);
				String indexURI = columnURI+getPathSeparator()+indexName;

				learn.fact(columnURI, prefix("uniqueIndex"), indexURI);
				learn.fact(indexURI, prefix("name"), indexName, "string");
			}
		}
	}

	protected void curateColumns(FactStream learn, DatabaseMetaData metaData, String catalog, String schema, String table, String tableURI, String schemaURI) throws SQLException, FactException {
        log.debug("Curating columns for: "+catalog+" "+table);
		ResultSet columns = metaData.getColumns(catalog, schema, table, "%");
		while(columns.next()) {
			String columnName = columns.getString("COLUMN_NAME");
			String label = PrettyString.humanize(columnName);
			String comments = columns.getString("REMARKS");
			String type = columns.getString("TYPE_NAME");
            if (comments==null||comments.equals("")) comments = "["+PrettyString.humanize(catalog)+"] "+PrettyString.humanize(table)+" "+label;

			String columnSize= columns.getString("COLUMN_SIZE");
			String allowNulls = columns.getString("IS_NULLABLE");
			String defaultValue = columns.getString("COLUMN_DEF");
			short ordinalPosition = columns.getShort("ORDINAL_POSITION");
			String isAutoincrement= columns.getString("IS_AUTOINCREMENT");
//			String isGeneratedcolumn= columns.getString("IS_GENERATEDCOLUMN");
			String columnID = sanitize(table)+"_"+sanitize(columnName);

			String columnURI = getColumnURI(tableURI, columnName);
//			String fqColumn = (schema==null||schema.equals("")?"":quote(schema)+dot)+quote(table)+dot+quote(columnName);

			log.debug("\t"+columnName+": "+type+" ->"+columnURI);

            log.debug(comments);
            learn.fact(columnURI, A, typeOf("Column") );
			learn.fact(columnURI, prefix("name"), columnName, "string");
			learn.fact(columnURI, prefix("type"), type, "string");
            learn.fact(columnURI, LABEL, label, "string");
            learn.fact(columnURI, COMMENT, comments, "string");
			learn.fact(columnURI, RDFS.DOMAIN.stringValue(), tableURI);
			learn.fact(columnURI, RDFS.RANGE.stringValue(), prefix(type));
			learn.fact(columnURI, RDFS.ISDEFINEDBY.stringValue(), schemaURI);
            learn.fact(columnURI, prefix("as"), columnID, "string");
            learn.fact(columnURI, prefix("name"), columnName, "string");
			learn.fact(columnURI, prefix("size"), columnSize, "integer");

			learn.fact(columnURI, prefix("order"), ordinalPosition, "integer");
            learn.fact(columnURI, prefix("autoIncrement"), isYes(isAutoincrement), "boolean");
			learn.fact(columnURI, prefix("optional"), isYes(allowNulls), "boolean");
//			learn.fact(columnURI, prefix("generatedColumn"), isYes(isGeneratedcolumn), "boolean");
            if (defaultValue!=null) learn.fact(columnURI, prefix("default"), defaultValue, "string");
		}
		columns.close();
	}

	protected void curateConstraints(FactStream learn, DatabaseMetaData metaData, String catalog, String schema, String table, String tableURI, String schemaURI) throws SQLException, FactException {
        log.debug("Curating Constraints for: "+catalog+" "+table);
        Map<String, Map> fkJoins = new HashMap();
		ResultSet joins = metaData.getImportedKeys(catalog, schema, table);

        String[] joinAttribs = new String[] { "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "FK_NAME", "PK_NAME", "KEY_SEQ" };
		while (joins.next()) {
            String fkName = joins.getString("FK_NAME");
            Map seqs = fkJoins.get(fkName);
            if (seqs==null) {
                seqs = new HashMap();
                fkJoins.put(fkName, seqs);
            }
            String seq = joins.getString("KEY_SEQ");
            Map<String,String> joinSeq = (Map)seqs.get(seq);
            if (joinSeq==null) {
                joinSeq = new HashMap();
                seqs.put(seq, joinSeq);
            }

            for(int i=0;i<joinAttribs.length;i++) {
                joinSeq.put(joinAttribs[i], joins.getString(joinAttribs[i]));
            }
        }
        joins.close();

        Map doneJoins = new HashMap();
        for(String key: fkJoins.keySet()) {
            Map fkSeqs = fkJoins.get(key);

            for(Object fkSeq: fkSeqs.keySet() ) {
                Map fkJoin = (Map)fkSeqs.get(fkSeq);
                String pkCatalog = (String)fkJoin.get("PKTABLE_CAT");
                String pkSchema = (String)fkJoin.get("PKTABLE_SCHEM");
                String pkTable = (String)fkJoin.get("PKTABLE_NAME");
                String pkField = (String)fkJoin.get("PKCOLUMN_NAME");
                String pkURI = getTableURI(pkCatalog, pkSchema, pkTable);
                String pkFieldURI = pkURI+":"+pkField;

                String fkCatalog = (String)fkJoin.get("FKTABLE_CAT");
                String fkSchema = (String)fkJoin.get("FKTABLE_SCHEM");
                String fkTable = (String)fkJoin.get("FKTABLE_NAME");
                String fkField = (String)fkJoin.get("FKCOLUMN_NAME");
                String fkURI = getTableURI(fkCatalog, fkSchema, fkTable);
                String fkFieldURI = fkURI+":"+fkField;


                String fkName = (String)fkJoin.get("FK_NAME");
                String pkName = (String)fkJoin.get("PK_NAME");

                String joinURI = globalize(catalog)+":"+schema+":"+fkName;
                String joinSeqURI = globalize(catalog)+":"+schema+":"+fkName+":"+fkSeq;

                if (!doneJoins.containsKey(joinURI)) {
                    log.debug("Constraint "+pkTable+" by "+fkTable);
                    learn.fact(joinURI, A, typeOf("Constraint"));
                    learn.fact(joinURI, LABEL, PrettyString.humanize(pkTable), "string");
                    learn.fact(joinURI, COMMENT, PrettyString.humanize(pkTable), "string");
//                    learn.fact(joinURI, RDFS.DOMAIN.stringValue(), fkURI);
//                    learn.fact(joinURI, RDFS.RANGE.stringValue(), pkURI);
	                learn.fact(joinURI, RDFS.ISDEFINEDBY.stringValue(), schemaURI);
                    doneJoins.put(joinURI, joinURI);
                }
                learn.fact(joinURI,    prefix("constrainedBy"), joinSeqURI);
                learn.fact(joinSeqURI, prefix("pk"), pkFieldURI);
                learn.fact(joinSeqURI, prefix("fk"), fkFieldURI);
            }
		}
	}

	@Converter
	public static FactStream curate(Connection connection) throws IQException, FactException, SQLException {
		N3Stream stream = new N3Stream(connection.getMetaData().getURL());
		JDBCCurator curator = new JDBCCurator();
		curator.curate(stream,connection);
		return stream;
	}
}
