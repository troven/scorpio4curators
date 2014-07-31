package com.scorpio4.curate.rdfs;
/*
 *   Fact:Core - CONFIDENTIAL
 *   Copyright (c) 2009-2014 Lee Curtis, All Rights Reserved.
 *
 *
 */

import com.scorpio4.curate.Curator;
import com.scorpio4.fact.stream.FactStream;
import com.scorpio4.fact.stream.N3Stream;
import com.scorpio4.oops.FactException;
import com.scorpio4.oops.IQException;
import com.scorpio4.util.DateXSD;
import com.scorpio4.util.Stopwatch;
import com.scorpio4.util.string.PrettyString;
import com.scorpio4.vocab.COMMONS;
import org.apache.camel.Converter;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.AreaReference;
import org.apache.poi.ss.util.CellReference;
import org.openrdf.model.vocabulary.DCTERMS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Fact:Core (c) 2010-2013
 * @author lee
 * Date: 17/01/13
 * Time: 8:26 PM
 * <p/>
 * Import a URL/File reference to a Spreadsheet and export an N3 representation.
 */
@Converter
public class XLSCurator implements Curator {
	private static final Logger log = LoggerFactory.getLogger(XLSCurator.class);

    public static String NS = "self:curator:xls:";
    
	String[] IGNORED_RANGES = new String[]{"Print_Area","Excel_","BuiltIn_"};
	DateXSD dateXSD = new DateXSD();
	Map<String, Map<String, Map>> rangeHeaders = new HashMap();
	boolean explicitHeading = true;
	String baseURI = null;
	URL url = null;
	private Stopwatch stopwatch = new Stopwatch();

	public XLSCurator() {
	}

	public XLSCurator(String baseURI, URL xls) {
		load(baseURI, xls);
	}

	public XLSCurator(String baseURI, File xls) throws MalformedURLException {
		load(baseURI, xls.toURI().toURL());
	}

	public void load(String baseURI, URL xls) {
		this.baseURI = baseURI.trim();
		this.url = xls;
	}

	public String toString() {
        return getIdentity();
	}

    public void curate(FactStream learn, URL xls) throws IQException {
		try {
			Workbook workbook = WorkbookFactory.create(xls.openStream());
			curate(learn, workbook);
            this.baseURI = xls.toExternalForm();
			log.debug("# Elapsted time:"+stopwatch.toString());
		} catch(Exception e) {
			throw new IQException("Error parsing Spreadsheet", e);
		}
	}

	public void curate(FactStream learn, Workbook workbook) throws Exception {
		int named_ranges = workbook.getNumberOfNames();
//		if (named_ranges<1) {
//			throw new CRUDException("No named ranges");
//		}
		learn.fact(getIdentity(), A, NS+"WorkBook");
        learn.fact(getIdentity(), DCTERMS.CREATED.stringValue(), dateXSD.format(), "dateTime" );
/*
		for(int nr=0; nr<named_ranges; nr++) {
			Name namedRange = workbook.getNameAt(nr);
			if (!isIgnored(namedRange)) curate(learn, workbook, namedRange);
		}
*/
        for(int i=0;i<workbook.getNumberOfSheets();i++) {
            curate(learn, workbook, workbook.getSheetAt(i));
        }
	}

    public void curate(FactStream learn, Workbook workbook, Sheet sheet) throws FactException {
        String sheetURI = baseURI+sheet.getSheetName();
        learn.fact(baseURI, NS+"hasSheet", sheetURI);

        learn.fact(baseURI, LABEL, sheet.getSheetName());

        for(int r=0;r<sheet.getPhysicalNumberOfRows();r++) {
            Row row = sheet.getRow(r);
            if (row!=null) {
                for(Cell cell: row) {
                    curate(learn, workbook, sheet, cell);
                }
            }
        }
    }

    private void curate(FactStream learn, Workbook workbook, Sheet sheet, Cell cell) throws FactException {
        String sheetURI = baseURI+sheet.getSheetName();

        learn.fact(baseURI, CURATOR+"by", getIdentity() );
        learn.fact(baseURI, NS+"hasSheet", sheetURI);

        String rowURI = sheetURI+"#"+cell.getRowIndex();
        String colURI = sheetURI+"#"+CellReference.convertNumToColString(cell.getColumnIndex());

        String cellURI = sheetURI+"!"+CellReference.convertNumToColString(cell.getColumnIndex())+cell.getRowIndex();

        String cellType = getCellXSD(cell, cell.getCellType());
        String cellValue = getCellValue(cell, cell.getCellType());
        Comment comment = cell.getCellComment();
        CellStyle cellStyle = cell.getCellStyle();

        log.debug("\n<"+sheetURI+"> cell <"+cellURI+">");
        learn.fact(sheetURI, NS+"hasCell", cellURI);

        learn.fact(cellURI, NS+"hasColumn ", colURI);
        learn.fact(cellURI, NS+"hasRow", rowURI);
        learn.fact(cellURI, NS+"hasValue", cellValue, cellType);
        if (comment!=null) {
            learn.fact(cellURI, COMMENT, comment.getString(), "string");
            learn.fact(cellURI, DCTERMS.CREATOR.stringValue(), comment.getAuthor(), "string");
        }
        if (cell.getCellType()==Cell.CELL_TYPE_FORMULA) {
            learn.fact(cellURI, NS+"formula", StringEscapeUtils.escapeJava(cell.getCellFormula()), "string");
        }
        if (cellStyle!=null) {
            learn.fact(cellURI, NS+"alignment", NS+PrettyString.capitalise(getAlignment(cellStyle)) + "Alignment");
            String cellFormat = StringEscapeUtils.escapeJava(cellStyle.getDataFormatString());
            learn.fact(cellURI, NS+"format", cellFormat, "string");
            if (cellStyle.getLocked()) {
                learn.fact(cellURI, NS+"locked", cellStyle.getLocked(), "boolean");
//                learn.fact(cellURI, VOCAB.A, NS+"LockedCell");
            }
            if (cellStyle.getHidden()) {
                learn.fact(cellURI, NS+"hidden", cellStyle.getHidden(), "boolean");
//                learn.fact(cellURI, VOCAB.A, NS+"HiddenCell");
            }
        }
    }

    public void curate(FactStream learn, Workbook workbook, Sheet sheet, Name namedRange) throws Exception {
		AreaReference[] refs = AreaReference.generateContiguous(namedRange.getRefersToFormula());

		String rangeName = PrettyString.sanitize(namedRange.getNameName());
		String range_label = PrettyString.humanize(namedRange.getNameName());
		String rangeURI = baseURI+rangeName;

        learn.fact(baseURI, NS+"hasRange", rangeURI);
        learn.fact(rangeURI, A, NS+"SheetRange");

        learn.fact(rangeURI, LABEL,range_label, "string");
		if (refs.length<=0) {
			log.debug("\trdfs:comment \"No cells were found in the Sheet Range\"");
			return;
		}
		for(int i=0;i<refs.length;i++) {
//			curate(learn, workbook, namedRange, refs[i]);
		}

		log.debug("\n# Column Headers \n");
		Map<String, Map> headers = getHeaders(rangeURI);
		for(Map.Entry<String, Map> heading: headers.entrySet()) {
            Map header = heading.getValue();
            String headerURI = (String) header.get("this");
			learn.fact(rangeURI, NS+"hasHeader" , headerURI);
			learn.fact(headerURI, "rdfs:label", header.get("label"), "string");
			learn.fact(headerURI, COMMENT, header.get("comment"), "string");
			learn.fact(headerURI, NS+"hasFormat", header.get("format"), "string");
			learn.fact(headerURI, NS+"required", header.get("required"), "boolean");
			String alignment = PrettyString.capitalise(header.get("alignment") + "Alignment");
			learn.fact(headerURI, NS+"alignment", NS+"" + alignment);

		}
	}

	public void curate(FactStream learn, Workbook workbook, AreaReference areaRef) throws FactException {
        for(CellReference ref: areaRef.getAllReferencedCells()) {
            Sheet sheet = workbook.getSheet(ref.getSheetName());
            curate(learn,workbook, sheet, ref);
        }
	}

    private void curate(FactStream learn, Workbook workbook, Sheet sheet, CellReference column_ref) throws FactException {
        String sheetName = column_ref.getSheetName();
        String rangeURI = baseURI+"/"+sheetName;

        int row_num = column_ref.getRow();
        int col_num = column_ref.getCol();
        String colURI = rangeURI+":row_"+row_num+":column_"+col_num;
        String rowURI = rangeURI+":row_"+row_num;
        Row row = sheet.getRow(row_num);

        String headerURI = rangeURI+":header:column_"+col_num;

        Map header = getHeader(rangeURI, headerURI);
        Cell cell = row!=null?row.getCell(col_num):null;
        boolean isHeaderRow = (row_num==0 && explicitHeading);

        if (cell!=null && !isHeaderRow) {
            curate(learn, workbook, sheet, cell);
        }

        if (row_num==0 && cell!=null) {
        String cellType = getCellXSD(cell, cell.getCellType());
        String value = getCellValue(cell, cell.getCellType());
        Comment comment = cell.getCellComment();

        String id = PrettyString.camelCase(sheetName)+"_"+PrettyString.lamaCase(value);
        header.put("this", headerURI);
        header.put("id", id);
        header.put("label", value);
        header.put("identity", headerURI);
        header.put("renders", headerURI);
        header.put("required", true); // default's to required
        header.put("type", cellType);

        if (comment!=null) {
            header.put("comment", comment.getString().toString());
        } else {
            header.put("comment", PrettyString.humanize(sheetName) + " " + PrettyString.humanize(value));
        }

        CellStyle cellStyle = cell.getCellStyle();
        if (cellStyle!=null) {
            header.put("format", cellStyle.getDataFormatString());
            header.put("alignment", getAlignment(cellStyle));
            header.put("locked", cellStyle.getLocked());
            header.put("visible", cellStyle.getHidden());
        }

//				learn.fact(rowURI, NS+"hasColumn ", headerURI );
//				learn.fact(  +colURI, NS+"hasValue", value, "string");
    } else {
        header.put("required", false);
        learn.fact(colURI, NS+"hasValue", "", COMMONS.XSD_NULL);
    }
}

    protected String getCellXSD(Cell cell, int column_type) {
		switch(column_type) {
			case Cell.CELL_TYPE_BLANK:
				return "simpleType";
			case Cell.CELL_TYPE_BOOLEAN:
				return "boolean";
			case Cell.CELL_TYPE_NUMERIC:
				// Excel date-handling is horrible
				if (HSSFDateUtil.isCellDateFormatted(cell)) {
					Date date = HSSFDateUtil.getJavaDate(cell.getNumericCellValue());
					if (date!=null) {
						return "date";
					}
				}
				return "numeric";
			case Cell.CELL_TYPE_STRING:
				return "string";
			case Cell.CELL_TYPE_FORMULA:
				return getCellXSD(cell, cell.getCachedFormulaResultType() );
			default:
				return "simpleType";
		}
	}

	protected String getCellValue(Cell cell) {
		return StringEscapeUtils.escapeJava(cell.getStringCellValue());
	}

	protected String getCellValue(Cell cell, int column_type) {
		try {
			switch(column_type) {
				case Cell.CELL_TYPE_BLANK:
					return "";
				case Cell.CELL_TYPE_BOOLEAN:
					return ""+cell.getBooleanCellValue();
				case Cell.CELL_TYPE_NUMERIC:
					// Excel date-handling is horrible
					if (HSSFDateUtil.isCellDateFormatted(cell)) {
						Date date = HSSFDateUtil.getJavaDate(cell.getNumericCellValue());
						if (date!=null) {
							return ""+dateXSD.format(date);
						}
					}
					return ""+cell.getNumericCellValue();
				case Cell.CELL_TYPE_STRING:
					return StringEscapeUtils.escapeJava(cell.getStringCellValue());
				case Cell.CELL_TYPE_FORMULA:
					return getCellValue(cell, cell.getCachedFormulaResultType());
				default:
					return getCellValue(cell)+"\n rdfs:comment \"unknown data type: "+column_type+"\"";
			}
		} catch(Exception e) {
			log.debug("Error converting: "+cell.toString()+" as type: "+column_type, e);
		}
		return getCellValue(cell);
	}

	protected boolean isIgnored(Name range) {
		if (range.isFunctionName()) return true;
		if (range.getNameName().startsWith("_")) return true;
		boolean ignored = false;
		for(int i=0;i< IGNORED_RANGES.length;i++) {
			ignored = ignored?ignored:range.getNameName().toLowerCase().contains(IGNORED_RANGES[i].toLowerCase());
		}
		return ignored;
	}

	protected String getAlignment(CellStyle style) {
		if (style.getAlignment()==CellStyle.ALIGN_CENTER) return "center";
		if (style.getAlignment()==CellStyle.ALIGN_LEFT) return "left";
		if (style.getAlignment()==CellStyle.ALIGN_JUSTIFY) return "justify";
		if (style.getAlignment()==CellStyle.ALIGN_RIGHT) return "right";
		return "left";
	}

	private Map<String,Map> getHeaders(String rangeKey) {
		Map<String, Map> headers = this.rangeHeaders.get(rangeKey);
		if (headers==null) {
			headers = new HashMap();
			this.rangeHeaders.put(rangeKey, headers);
		}
		return headers;
	}


	protected Map getHeader(String rangeKey, String headerKey) {
		Map<String, Map> headers = getHeaders(rangeKey);
        Map header = headers.get(headerKey);
		if (header==null) {
			header = new HashMap();
			header.put("format", "GENERAL");
			headers.put(headerKey, header);
		}
		return header;
	}

    @Override
	public String getIdentity() {
		return this.baseURI;
	}

    @Override
    public void curate(FactStream stream, Object curated) throws FactException, IQException {
        if (!canCurate(curated)) throw new IQException("self:learn:email:oops:cant-curate#"+curated);
        try {
            if (File.class.isInstance(curated)) curate(stream, ((File)curated).toURI().toURL());
            if (URL.class.isInstance(curated)) curate(stream, (URL)curated);
            if (Workbook.class.isInstance(curated)) curate(stream, (Workbook)curated);
        } catch(Exception e) {
            throw new IQException("self:learn:xls:oops:curate-failed#"+e.getMessage(),e);
        }
    }

    @Override
    public boolean canCurate(Object curated) {
        if (curated==null) return false;
        return (URL.class.isInstance(curated)) || (Workbook.class.isInstance(curated) || File.class.isInstance(curated));
    }

	@Converter
	public static FactStream curate(Workbook workbook) throws Exception {
		N3Stream stream = new N3Stream();
		XLSCurator curator = new XLSCurator();
		curator.curate(stream,workbook);
		return stream;
	}

	@Converter
	public static Workbook curate(File xls) throws Exception {
		FileInputStream inputStream = new FileInputStream(xls);
		Workbook workbook = WorkbookFactory.create(inputStream);
		inputStream.close();
		return workbook;
	}

	@Converter
	public static Workbook curate(InputStream xls) throws Exception {
		Workbook workbook = WorkbookFactory.create(xls);
		xls.close();
		return workbook;
	}

	@Converter
	public static Workbook curate(URL url) throws Exception {
		return WorkbookFactory.create(url.openStream());
	}

	@Converter
	public static Workbook curate(URI url) throws Exception {
		return curate(url.toURL());
	}
}
