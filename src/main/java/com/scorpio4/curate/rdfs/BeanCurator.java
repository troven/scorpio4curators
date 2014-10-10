package com.scorpio4.curate.rdfs;
/*
 *   Scorpio4 - CONFIDENTIAL
 *   Copyright (c) 2009-2014 Lee Curtis, All Rights Reserved.
 *
 *
 */

import com.scorpio4.curate.CORE;
import com.scorpio4.curate.Curator;
import com.scorpio4.fact.stream.FactStream;
import com.scorpio4.fact.stream.N3Stream;
import com.scorpio4.oops.FactException;
import com.scorpio4.oops.IQException;
import com.scorpio4.util.Identifiable;
import com.scorpio4.util.string.PrettyString;
import com.scorpio4.vocab.COMMONS;
import org.apache.camel.Converter;
import org.limewire.collection.CharSequenceKeyAnalyzer;
import org.limewire.collection.PatriciaTrie;
import org.limewire.collection.Trie;
import org.openrdf.model.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.*;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Scorpio4 (c) 2013-2014
 * Module: com.scorpio4.curator
 * @author lee
 * Date  : 2/12/2013
 * Time  : 5:47 PM
 */
@Converter
public class BeanCurator implements Curator, Identifiable {
    public static final Logger log = LoggerFactory.getLogger(BeanCurator.class);
    String identity = "self:learn:schema:bean#";
    Trie ns2pkg = new PatriciaTrie(new CharSequenceKeyAnalyzer());
    Trie pkg2ns = new PatriciaTrie(new CharSequenceKeyAnalyzer());
    Map classesMapped = new HashMap();
    boolean recurseClasses = false;

    public BeanCurator() throws IQException {
    }

    public BeanCurator(String pkg, String ns) throws IQException {
        addPackage(pkg,ns);
    }

    public void defaults() throws IQException {
        addPackage("com.vaadin.ui.", "self:vui:");
        addPackage("java.util.", "self:java:util:");
    }

    public void addPackage(String pkg, String ns) throws IQException {
        if (pkg==null||ns==null||pkg.length()<4||ns.length()<4) throw new IQException("self:curate:rdfs:bean:oops:invalid-package-or-namespace");
        if (!pkg.endsWith(".")) throw new IQException("self:curate:rdfs:bean:oops:not-a-package#"+pkg);
        ns2pkg.put(ns, pkg);
        pkg2ns.put(pkg, ns);
    }

    public String getNamespace(Class clazz) {
        return (String) pkg2ns.select(clazz.getCanonicalName());
    }

    public String getPackage(String ns) {
        return (String) ns2pkg.select(ns);
    }

    /*    public String getClassURI(Object obj) {
            if (obj==null) return null;
            Class clazz = clazz;
            return getClassURI(clazz);
        }
    */
    public String getClassURI(Class clazz) {
        if (clazz==null) return null;
        String ns = getNamespace(clazz);
        String pkg = getPackage(ns);
        log.debug("->"+clazz.getCanonicalName()+" @ "+pkg+" --> "+ns);
        if (clazz.getCanonicalName().startsWith(pkg)) {
            log.debug("Localized Class: " + clazz.getCanonicalName() + " -> " + pkg);
            return "bean:"+ns+clazz.getCanonicalName().substring(pkg.length());
        } else {
            log.error("Canonical Class: "+clazz.getCanonicalName()+" -> "+pkg);
            return "bean:"+clazz.getCanonicalName();
        }
    }

    @Override
    public boolean canCurate(Object curated) {
        return (Class.class.isInstance(curated));
    }

    @Override
    public void curate(FactStream stream, Object pojo) throws FactException, IQException {
        if (Class.class.isInstance(pojo)) curate(stream, (Class)pojo);
    }

    public void curate(FactStream stream, Class clazz) throws IQException, FactException {
        String pkg = getNamespace(clazz);
        if (pkg==null) return;
        if (classesMapped.containsKey(clazz)) return;

        BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(clazz);
        } catch (IntrospectionException e1) {
            throw new IQException("self:curate:rdfs:bean:oops:config#"+clazz.getCanonicalName(),e1);
        }
        String classURI = getClassURI(clazz);
        if (classURI==null) {
            log.debug("Class not recognized: "+clazz.getCanonicalName());
            return;
        }
//            throw new IQException("self:vendor:vaadin:oops:config:missing-class#"+clazz.getCanonicalName());
        log.debug("Inspecting: "+clazz.getCanonicalName()+" -> "+classURI);

        stream.fact(classURI, RDF.TYPE.stringValue(), COMMONS.RDFS_TYPE);
//        stream.fact(classURI, CORE.CURATOR+"by", getIdentity());
        classesMapped.put(classURI, true);
	    
	    for(Class iface:clazz.getInterfaces()) {
            stream.fact( classURI, CORE.RDFS_TYPE, getClassURI(iface));
        }
        stream.fact( classURI, CORE.RDFS_SUBCLASS, getClassURI(clazz.getSuperclass()));

        stream.fact(classURI, COMMONS.LABEL, clazz.getSimpleName(), "string");
        stream.fact(classURI, COMMONS.COMMENT, clazz.getCanonicalName(), "string");

        stream.fact(classURI, COMMONS.ISDEFINEDBY, pkg);
        PropertyDescriptor pds[] = beanInfo.getPropertyDescriptors();
        for (int i = 0; i < pds.length; i++) {
            curate(stream, clazz, pds[i]);
        }

        MethodDescriptor mds[] = beanInfo.getMethodDescriptors();
        for (int i = 0; i < mds.length; i++) {
            curate(stream, clazz, mds[i]);
        }
    }

    private void curate(FactStream stream, Class clazz, MethodDescriptor md) throws FactException {
        String name = md.getName();
        String classURI = getClassURI(clazz);
        String methodURI = classURI+"#"+name;
        String pkg = getNamespace(clazz);
        Method method = md.getMethod();
        stream.fact(methodURI, RDF.TYPE.stringValue(), getIdentity()+"$"+method.getName());
        curate(stream, methodURI, pkg, "", method );
    }

    public void curate(FactStream stream, Class clazz, PropertyDescriptor propertyDescriptor) throws FactException, IQException {
        String name = propertyDescriptor.getName();
        Method writeMethod = propertyDescriptor.getWriteMethod();
        Method readMethod = propertyDescriptor.getReadMethod();
        Class type = propertyDescriptor.getPropertyType();
        if (type==null) {
            log.debug("No PropertyType for: " + name + " r: " + writeMethod + ", w:" + writeMethod);
            return;
        }
        String classURI = getClassURI(clazz);
        String propertyURI = classURI+"#"+name;
        String pkg = getNamespace(clazz);

        log.trace(">"+name+" -> "+type.getCanonicalName());
        if (writeMethod!=null) log.trace("\tW: "+writeMethod.getName());
        if (readMethod!=null) log.trace("\tR: "+readMethod.getName()+" -> "+ Arrays.toString(readMethod.getParameterTypes()));
	    
	    if (type.isPrimitive()) {
            stream.fact(propertyURI, RDF.TYPE.stringValue(), COMMONS.DATATYPE);
            stream.fact(propertyURI, COMMONS.LABEL, propertyDescriptor.getDisplayName(), "string");
            stream.fact(propertyURI, COMMONS.COMMENT, humanize(propertyDescriptor.getShortDescription())+" ("+propertyDescriptor.getPropertyType().getSimpleName()+")", "string");
            stream.fact(propertyURI, COMMONS.DOMAIN, classURI);
            stream.fact(propertyURI, COMMONS.RANGE, COMMONS.XSD+propertyDescriptor.getPropertyType().getSimpleName());
            stream.fact(propertyURI, pkg+"editor", getClassURI(propertyDescriptor.getPropertyEditorClass()));

            if (readMethod!=null) {
                Class<?>[] types = readMethod.getParameterTypes();
                for(Class ptype: types) stream.fact(propertyURI, pkg+"readParameter", COMMONS.XSD+ptype.getSimpleName());
                stream.fact(propertyURI, pkg+"readReturnType", getClassURI(readMethod.getReturnType()));
            }
            if (writeMethod!=null) {
                Class<?>[] types = writeMethod.getParameterTypes();
                for(Class ptype: types) stream.fact(propertyURI, pkg+"writeParameter", COMMONS.XSD+ptype.getSimpleName());
                stream.fact(propertyURI, pkg+"writeReturnType", getClassURI(writeMethod.getReturnType()));
            }

            stream.fact( propertyURI, COMMONS.ISDEFINEDBY, pkg);
        } else {
            String rangeClassURI = getClassURI(type);
            if (rangeClassURI!=null) {
                stream.fact(propertyURI, RDF.TYPE.stringValue(), COMMONS.DATATYPE);
                stream.fact(propertyURI, COMMONS.LABEL, propertyDescriptor.getDisplayName(), "string");
                stream.fact(propertyURI, COMMONS.COMMENT, humanize(propertyDescriptor.getShortDescription()), "string");
                stream.fact(propertyURI, COMMONS.DOMAIN, classURI);
                stream.fact(propertyURI, COMMONS.RANGE, rangeClassURI);
                stream.fact(propertyURI, pkg+"editor", getClassURI(propertyDescriptor.getPropertyEditorClass()));

                curate(stream, propertyURI,pkg, "read", readMethod);
                curate(stream, propertyURI,pkg, "write", writeMethod);

                stream.fact( propertyURI, COMMONS.ISDEFINEDBY, pkg);
                if (recurseClasses && !type.isPrimitive()) {
                    log.debug("Recurse: "+type);
                    this.curate(stream, type);
                }
            }
        }
    }

    private void curate(FactStream stream, String propertyURI, String pkg, String paramType, Method readMethod) throws FactException {
        if (readMethod==null) return;
        Class<?>[] types = readMethod.getParameterTypes();
        for(Class ptype: types) stream.fact(propertyURI, pkg+paramType+"Parameter", COMMONS.XSD+ptype.getSimpleName());
        stream.fact(propertyURI, pkg+paramType+"ReturnType", getClassURI(readMethod.getReturnType()));
    }

    private String humanize(String label) {
        return PrettyString.humanize(label).toLowerCase();
    }

    public boolean isRecurseClasses() {
        return recurseClasses;
    }

    public void setRecurseClasses(boolean recurseClasses) {
        this.recurseClasses = recurseClasses;
    }

    @Override
    public String getIdentity() {
        return identity;
    }

	@Converter
	public static FactStream toFactStream(Class t) throws IQException, FactException {
		N3Stream stream = new N3Stream("bean:"+BeanCurator.class.getCanonicalName());
		BeanCurator curator = new BeanCurator();
		curator.curate(stream,t);
		return stream;
	}

}
