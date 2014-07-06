package com.scorpio4.curate;
/*
 *   Scorpio4 - CONFIDENTIAL
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

import com.scorpio4.fact.stream.FactStream;
import com.scorpio4.oops.FactException;
import com.scorpio4.oops.IQException;
import com.scorpio4.util.Identifiable;
import com.scorpio4.vocab.COMMON;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

/**
 * Scorpio4 (c) Lee Curtis 2012
 * User: lee
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
