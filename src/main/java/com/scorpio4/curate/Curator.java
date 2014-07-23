package com.scorpio4.curate;
/*
 *   Scorpio4 - Apache Licensed
 *   Copyright (c) 2009-2014 Lee Curtis, All Rights Reserved.
 *
 *
 */

import com.scorpio4.fact.stream.FactStream;
import com.scorpio4.oops.FactException;
import com.scorpio4.oops.IQException;
import com.scorpio4.util.Identifiable;
import com.scorpio4.vocab.COMMON;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

/**
 * Scorpio4 (c) Lee Curtis 2012
 * @author lee
 * Date: 15/03/13
 * Time: 6:56 PM
 *
 * A Curator is responsible for populating a FactStream with by inspecting a curated Object.
 * The method of curation and meaning of the facts are left to the implementation.
 * The implementation must correctly identify a curatable object in the isCuratable() method.
 *
 */
public interface Curator extends Identifiable {

	public static final String CURATOR = COMMON.CORE+"curator/";
	public static final String CURATED_BY = CURATOR+"curatedBy";
	public static final String LABEL = RDFS.LABEL.stringValue();
	public static final String COMMENT = RDFS.LABEL.stringValue();
	public static final String A = RDF.TYPE.stringValue();
	public static final String DOMAIN = RDFS.DOMAIN.stringValue();
	public static final String RANGE = RDFS.RANGE.stringValue();
	public static final String DEFINED_BY = RDFS.NAMESPACE+"isDefinedBy";
	public static final String FILE = COMMON.CORE+"file/";

	public void curate(FactStream stream, Object curated) throws FactException, IQException;

    public boolean canCurate(Object curated);
}
