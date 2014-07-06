package com.scorpio4.curate.rdf;

import com.scorpio4.curate.Curator;
import com.scorpio4.fact.stream.FactStream;
import com.scorpio4.oops.FactException;
import com.scorpio4.oops.IQException;
import com.scorpio4.util.DateXSD;
import com.scorpio4.vocab.COMMON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;

import static java.nio.file.FileVisitResult.CONTINUE;

/**
 * Scorpio4 (c) 2013-2014
 * Module: com.scorpio4.learn
 * User  : lee
 * Date  : 27/10/2013
 * Time  : 12:31 AM
 */
public class FileSystemCurator implements Curator {
    protected static final Logger log = LoggerFactory.getLogger(FileSystemCurator.class);
    boolean curateRDF = false;
    String identity = "bean:"+getClass().getCanonicalName();

    public FileSystemCurator() {
    }

    public void setImportRDF(boolean curateRDF) {
        this.curateRDF=curateRDF;
    }

    @Override
    public boolean canCurate(Object curated) {
        if (curated==null) return false;
        return (FileSystem.class.isInstance(curated)) || (Path.class.isInstance(curated)) || File.class.isInstance(curated);
    }

    @Override
    public void curate(FactStream stream, Object curated) throws FactException, IQException {
        if (canCurate(curated)) throw new IQException("self:learn:file:oops:cant-curate#"+curated);
        try {
            if (FileSystem.class.isInstance(curated)) curate(stream, (FileSystem)curated);
            if (Path.class.isInstance(curated)) curate(stream, (Path)curated);
            if (File.class.isInstance(curated)) curate(stream, (File)curated);
        } catch(Exception e) {
            throw new IQException("self:learn:file:oops:cant-curate#"+e.getMessage(),e);
        }
    }

    public void curate(FactStream learn, File file) throws FileSystemException, FactException, IOException {
        String fileURI = file.toURI().toString();
        log.debug("Curate File: "+fileURI);
        curate(learn, Paths.get(file.toURI()));
    }

    public void curate(FactStream learn, FileSystem fileSystem) throws FileSystemException, FactException, IOException {
        log.debug("Curate FileSystem: "+fileSystem);
        Iterable<Path> rootDirectories = fileSystem.getRootDirectories();
        for(Path path: rootDirectories) {
            DirectoryStream<Path> ds = Files.newDirectoryStream(path);
            curate(learn,ds);
        }
    }

    public void curate(FactStream learn, Iterable<Path> pathes) throws FactException, IOException, FileSystemException {
        PathWalker pathWalker = new PathWalker(this,learn);
        for (Path path : pathes) {
			log.debug("Curate Path: "+path.toUri().toString());
            Files.walkFileTree( path, pathWalker );
        }
    }

    public void curate(FactStream learn, Path path) throws FactException, IOException, FileSystemException {
        PathWalker pathWalker = new PathWalker(this,learn);
        log.debug("Curate Path: "+path.toUri().toString());
        Files.walkFileTree( path, pathWalker );
    }

    @Override
    public String getIdentity() {
        return identity;
    }
}

class PathWalker extends SimpleFileVisitor<Path> {
    protected static final Logger log = LoggerFactory.getLogger(PathWalker.class);
    Curator curator = null;
    FactStream learn = null;
    DateXSD dateXSD = new DateXSD();

    public PathWalker(Curator curator, FactStream learn) {
        this.curator = curator;
        this.learn = learn;
    }

    public void curate(Path file, BasicFileAttributes attr) throws FactException, IOException {
        String fileURI = file.toUri().toString();

        learn.fact(fileURI, curator.LABEL, file.getFileName(), "string");
        learn.fact(fileURI, curator.FILE+"creationTime", attr.creationTime(), "dateTime");
        learn.fact(fileURI, curator.FILE+"lastModifiedTime", attr.lastModifiedTime(), "dateTime");
        learn.fact(fileURI, curator.FILE+"lastAccessTime", attr.lastAccessTime(), "dateTime");
        learn.fact(fileURI, curator.FILE + "size", attr.size(), "integer");
		String contentType = Files.probeContentType(file);
        if (contentType!=null)
    		learn.fact(fileURI, curator.FILE + "contentType", COMMON.MIME_TYPE+contentType);

        UserPrincipal owner = Files.getOwner(file);
        learn.fact(fileURI, curator.FILE + "owner", owner, COMMON.MIME_TYPE+"string");

        curatePermissions(learn, fileURI, file);
        log.debug("Curated Path: "+fileURI);
    }

    public void curatePermissions(FactStream learn, String fileURI, Path file) throws IOException, FactException {
        Set<PosixFilePermission> posixFilePermissions = Files.getPosixFilePermissions(file);
        for(PosixFilePermission perm:posixFilePermissions) {
            learn.fact(fileURI, curator.FILE + "can:"+perm.name().toLowerCase(), "true", COMMON.MIME_TYPE+"boolean");
        }

        learn.fact(fileURI, curator.FILE + "is:hidden", Files.isHidden(file), COMMON.MIME_TYPE+"boolean");
        learn.fact(fileURI, curator.FILE + "is:executable", Files.isExecutable(file), COMMON.MIME_TYPE+"boolean");
        learn.fact(fileURI, curator.FILE + "is:readable", Files.isReadable(file), COMMON.MIME_TYPE+"boolean");
        learn.fact(fileURI, curator.FILE + "is:symbolic", Files.isSymbolicLink(file), COMMON.MIME_TYPE+"boolean");

    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attr) {
        String fileURI = file.toUri().toString();
        log.debug("Visit File: "+fileURI);
        try {
            learn.fact(fileURI, curator.A, curator.FILE+"File");
            learn.fact(fileURI, curator.FILE+"parent", file.getParent().toUri().toString());

            curate(file, attr);
        } catch (FactException e) {
            FileSystemCurator.log.debug(e.getMessage(),e);
        } catch (IOException e) {
            FileSystemCurator.log.debug(e.getMessage(),e);
		}
		return CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException ioe) throws IOException {
        return CONTINUE;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path folder,  BasicFileAttributes attr) throws IOException {
        String fileURI = folder.toUri().toString();
        log.debug("Visit Folder: "+fileURI);
        try {
            learn.fact(fileURI, curator.A, curator.FILE+"Folder");
            learn.fact(fileURI, curator.FILE+"parent", folder.getParent().toUri().toString());

            curate(folder,attr);
            curatePermissions(learn, fileURI, folder);

        } catch (FactException e) {
            FileSystemCurator.log.debug(e.getMessage(),e);
        }
        return CONTINUE;
    }
}