package com.scorpio4.curate.rdfs;
/*
 *   Fact:Core - CONFIDENTIAL
 *   Unpublished Copyright (c) 2009-2014 Lee Curtis, All Rights Reserved.
 *
 *   NOTICE:  All information contained herein is, and remains the property of Lee Curtis. The intellectual and technical concepts contained
 *   herein are proprietary to Lee Curtis and may be covered by Australian, U.S. and Foreign Patents, patents in process, and are protected by trade secret or copyright law.
 *   Dissemination of this information or reproduction of this material is strictly forbidden unless prior written permission is obtained
 *   from Lee Curtis.  Access to the source code contained herein is hereby forbidden to anyone except current Lee Curtis employees, managers or contractors who have executed
 *   Confidentiality and Non-disclosure agreements explicitly covering such access.
 *
 *   The copyright notice above does not evidence any actual or intended publication or disclosure  of  this source code, which includes
 *   information that is confidential and/or proprietary, and is a trade secret, of Lee Curtis.   ANY REPRODUCTION, MODIFICATION, DISTRIBUTION, PUBLIC  PERFORMANCE,
 *   OR PUBLIC DISPLAY OF OR THROUGH USE  OF THIS  SOURCE CODE  WITHOUT  THE EXPRESS WRITTEN CONSENT OF LEE CURTIS IS STRICTLY PROHIBITED, AND IN VIOLATION OF APPLICABLE
 *   LAWS AND INTERNATIONAL TREATIES.  THE RECEIPT OR POSSESSION OF  THIS SOURCE CODE AND/OR RELATED INFORMATION DOES NOT CONVEY OR IMPLY ANY RIGHTS
 *   TO REPRODUCE, DISCLOSE OR DISTRIBUTE ITS CONTENTS, OR TO MANUFACTURE, USE, OR SELL ANYTHING THAT IT  MAY DESCRIBE, IN WHOLE OR IN PART.
 *
 */
import com.scorpio4.util.Identifiable;
import com.scorpio4.curate.Curator;
import com.scorpio4.fact.stream.FactStream;
import com.scorpio4.oops.FactException;
import com.scorpio4.oops.IQException;
import com.scorpio4.util.Steps;
import com.scorpio4.util.string.PrettyString;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Fact:Core (c) Lee Curtis 2012-2013
 * User: lee
 * Date: 14/03/13
 * Time: 9:44 AM
 * <p/>
 * Generates N3 representation of a JDBC Connection's DatabaseMetaData
 */
public class JDBCCurator implements Curator, Identifiable {
	private static final Logger log = LoggerFactory.getLogger(JDBCCurator.class);

	private String prefix = "sql:", dot = ".", quote = "'";
	private String baseURI = "bean:"+getClass().getCanonicalName();
	private Connection connection = null;
    private long startTimestamp = System.currentTimeMillis();

	public JDBCCurator() {
	}

    public JDBCCurator(String baseURI) {
        setIdentity(baseURI);
    }

	public JDBCCurator(String baseURI, Connection connection, FactStream learner) throws IQException, FactException {
		setIdentity(baseURI);
		connect(connection);
		curate(learner,connection);
	}

	public JDBCCurator(Connection connection, FactStream learner) throws IQException, SQLException, FactException {
		String uri = connection.getMetaData().getURL();
		String prefix = Steps.local(uri);
		setIdentity(uri);
		connect(connection);
		if (learner!=null) curate(learner, connection);
	}

	@Override
	public String getIdentity() {
		return this.baseURI;
	}

	public void setIdentity(String baseURI) {
		if (baseURI.endsWith("#") || baseURI.endsWith("/") || baseURI.endsWith(":")) this.baseURI = baseURI;
		else this.baseURI = baseURI+"#";
	}

	public void connect(Connection connection) throws IQException {
		this.connection = connection;
		try {
			if (connection.isClosed()) throw new IQException("Connection is closed");
		} catch (SQLException e) {
			throw new IQException("SQL connection failed",e);
		}
	}

    @Override
    public void curate(FactStream learn, Object curated) throws FactException, IQException {
        if (!canCurate(curated)) throw new IQException("self:learn:schema:jdbc:oops:cant-curate#"+(curated==null?"null":curated.getClass()));
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            quote = metaData.getIdentifierQuoteString();
            curateDatabase(learn, metaData);
            curateTables(learn, metaData);
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
		String metaURI = metaData.getURL();

        learn.fact(metaURI, CURATOR+"by", getIdentity() );
        learn.fact(getIdentity(), A, typeOf("Database") );
        log.debug("Curating SQL database: "+metaURI);
        learn.fact(metaURI, prefix("product"),metaData.getDatabaseProductName(), "string");
        learn.fact(metaURI, prefix("version"),metaData.getDatabaseProductVersion());
        learn.fact(metaURI, prefix("ansi92Entry"),metaData.supportsANSI92EntryLevelSQL());
        learn.fact(metaURI, prefix("ansi92Entry"),metaData.supportsANSI92EntryLevelSQL());
        learn.fact(metaURI, prefix("ansi92Full"),metaData.supportsANSI92FullSQL());
        learn.fact(metaURI, prefix("ansi92Intermediate"),metaData.supportsANSI92IntermediateSQL());
	}

	protected void curateTables(FactStream learn, DatabaseMetaData metaData) throws SQLException, FactException {
		String schemaPattern = null;
		String tablePattern = "%";
		String[] types = null;

        ResultSet catalogs = metaData.getCatalogs();
        while(catalogs.next()) {
            String catalog = catalogs.getString("TABLE_CAT");
            String catalogURI = globalize(catalog);

            log.debug("Curating catalog: " + catalog);
            log.debug("Curating catalog: " + catalog);

            learn.fact(getIdentity(), prefix("catalog"), catalogURI);
            learn.fact(catalogURI, A, typeOf("Catalog"));
            learn.fact(catalogURI, LABEL, PrettyString.humanize(catalog));
            Map seenSchema = new HashMap();
            ResultSet tables = metaData.getTables(catalog, schemaPattern, tablePattern, types);
            while(tables.next()) {

                // handle schema (including NULL schema)
                String schema = tables.getString("TABLE_SCHEM");
                if (schema==null) schema = "";
                if (!seenSchema.containsKey(schema)) {
                    curateSchema(learn, metaData, catalog, catalogURI, schema);
                    seenSchema.put(schema, true);
                }
                curateTable(learn, metaData, tables);
            }
            tables.close();
        }
        catalogs.close();
        log.debug("Finished curating JDBC");
	}

    protected void curateSchema(FactStream learn, DatabaseMetaData metaData, String catalog, String catalogURI, String schema) throws SQLException, FactException {
        if (schema==null) schema = "";
        String schemaURI = catalogURI+":"+schema;

        if (schema.equals("")) log.debug("Curating "+catalog+" Default Schema");
        else log.debug("Curating "+catalog+" Schema: "+schema );

        learn.fact(catalogURI, prefix("schema"), schemaURI);
        learn.fact(schemaURI, A, typeOf("Schema") );
        learn.fact(schemaURI, LABEL, schema.equals("") ? "default" : PrettyString.humanize(schema), "string");
        learn.fact(schemaURI, DCTERMS.MODIFIED.stringValue(), "" + startTimestamp, "string");
        learn.fact(schemaURI, CURATED_BY, "urn:java:"+getClass().getCanonicalName(), "string" );
        learn.fact(schemaURI, prefix("name"), schema, "string");

    }

    protected void curateTable(FactStream learn, DatabaseMetaData metaData, ResultSet tableMeta) throws SQLException, FactException {
        String catalog = tableMeta.getString("TABLE_CAT");
        String catalogURI = globalize(catalog);

        String schema = tableMeta.getString("TABLE_SCHEM");
        if (schema==null) schema = "";
        String schemaURI = catalogURI+":"+schema;

        String tableName = tableMeta.getString("TABLE_NAME");
        String tableID = sanitize(tableName);
        String tableType = tableMeta.getString("TABLE_TYPE");
        String label = PrettyString.humanize(tableName).trim();
        String comments = tableMeta.getString("REMARKS");
        if (comments == null ||comments.trim().equals("")) comments = PrettyString.humanize(catalog)+" "+(PrettyString.humanize(schema)+" "+label+" "+PrettyString.lamaCase(tableType)).trim();
        else comments = comments.trim();

        String tableURI = getTableURI(catalog, schema, tableName);

        boolean hasSchema = (schema!=null)&&!schema.equals("");
        String fqSchema = hasSchema?quote(schema)+dot:"";
        String fqTable = fqSchema+quote(tableName);

        log.debug("Curating table: " + catalog+" "+tableName);
        log.debug("Curating table: " + catalog+" "+tableName);

        learn.fact(tableURI, A, typeOf(tableType));
        learn.fact(tableURI, LABEL, label, "string");
        learn.fact(tableURI, COMMENT, comments, "string");
//        n3s.append(prefix("id"), tableID);
//        n3s.append(prefix("fq"), fqTable);
        learn.fact(tableURI, prefix("name"), tableName, "string");
        learn.fact(tableURI, RDFS.ISDEFINEDBY.stringValue(), schemaURI);

        curateColumns(learn, metaData, catalog, schema, tableName, tableURI, schemaURI);

        curateJoins(learn, metaData, catalog, schema, tableName, tableURI, schemaURI);
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
			String columnID = sanitize(table)+"_"+sanitize(columnName);

			String columnURI = tableURI+":"+sanitize(columnName);
			String fqColumn = (schema==null||schema.equals("")?"":quote(schema)+dot)+quote(table)+dot+quote(columnName);

			log.debug("\t"+columnName+": "+type+" ->"+columnURI);

            log.debug(comments);
            learn.fact(columnURI, A, typeOf("Column") );

            learn.fact(columnURI, LABEL, label, "string");
            learn.fact(columnURI, COMMENT, comments, "string");
			learn.fact(columnURI, RDFS.DOMAIN.stringValue(), tableURI);
			learn.fact(columnURI, RDFS.RANGE.stringValue(), prefix(type));
//            learn.fact(VOCAB.RDFS + "isDefinedBy", schemaURI);
//			n3s.append(prefix("fq"), fqColumn);
            learn.fact(columnURI, prefix("as"), columnID, "string");
            learn.fact(columnURI, prefix("name"), columnName, "string");

            learn.fact(columnURI, prefix("size"), columnSize, "integer");
            learn.fact(columnURI, prefix("required"), (allowNulls != null), "boolean");
            if (defaultValue!=null) learn.fact(columnURI, prefix("default"), defaultValue, "string");
		}
		columns.close();
	}

	protected void curateJoins(FactStream learn, DatabaseMetaData metaData, String catalog, String schema, String table, String tableURI, String schemaURI) throws SQLException, FactException {
        log.debug("Curating joins for: "+catalog+" "+table);
        Map<String, Map> fkJoins = new HashMap();
		ResultSet joins = metaData.getImportedKeys(catalog, schema, table);

        String[] joinAttribs = new String[] { "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "FK_NAME", "PK_NAME", "KEY_SEQ" };
		while (joins.next()) {
            String fkName = joins.getString("FK_NAME");
            Map seqs = fkJoins.get(fkName);
            if (seqs==null) {
                int fkSeqNum = joins.getInt("KEY_SEQ");
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
                    learn.fact(joinURI, RDFS.DOMAIN.stringValue(), fkURI);
                    learn.fact(joinURI, RDFS.RANGE.stringValue(), pkURI);
                    doneJoins.put(joinURI, joinURI);
                }
                learn.fact(joinURI,    prefix("constrainedBy"), joinSeqURI);
                learn.fact(joinSeqURI, prefix("pk"), pkFieldURI);
                learn.fact(joinSeqURI, prefix("fk"), fkFieldURI);
            }
		}
	}

    public String getTableURI(String catalog, String schema, String table) {
        return globalize(catalog)+":"+PrettyString.sanitize(schema==null?"":schema)+":"+PrettyString.sanitize(table);
    }

	private String globalize(String local) {
		return getIdentity()+sanitize(local);
	}

	private String sanitize(String text) {
		return PrettyString.sanitize(text);
	}

	private String quote(String text) {
		if (text==null) return "";
		return quote+text+quote;
	}

	private String prefix(String local) {
		return prefix+PrettyString.lamaCase(local);
	}

    private String typeOf(String local) {
        return prefix+PrettyString.camelCase(local);
    }

    public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

}
