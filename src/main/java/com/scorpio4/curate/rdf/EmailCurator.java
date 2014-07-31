package com.scorpio4.curate.rdf;
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
import com.scorpio4.util.Identifiable;
import com.scorpio4.util.string.PrettyString;
import com.scorpio4.vocab.COMMONS;
import com.sun.mail.util.BASE64DecoderStream;
import org.apache.camel.Converter;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.io.IOUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.semarglproject.vocab.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.search.FlagTerm;
import java.io.*;
import java.util.*;

//import net.sf.classifier4J.summariser.SimpleSummariser;

/**
 * Scorpio4 (c) 2013-2014
 * Module: com.scorpio4.fact.mail
 * User  : lee
 * Date  : 25/10/2013
 * Time  : 9:42 PM
 */
@Converter
public class EmailCurator implements Identifiable, Curator {
	private static final Logger log = LoggerFactory.getLogger(EmailCurator.class);
	HashMap config = new HashMap();
	Session session = null;
	Store store = null;
	boolean hideMessageBody = false, saveAttachments = true;
	String storeType = "imaps", server = null, username = null, password = null;
	int port = 993;
	public static final String NS_MAIL = "self:mail:";
	File home = null;
	Map<String,String> headersToLearn = new HashMap();
//	SimpleSummariser summariser = new SimpleSummariser();
	int summaryLines = 3, nextMessageNumber = 0;
	DateXSD dateXSD = new DateXSD();
	private FlagTerm query;

	protected EmailCurator() {
	}

	public EmailCurator(Map props, String server, String type, int port, String username, String password) {
		setConfig(props);
		this.server = server;
		this.storeType = type;
		this.port = port;
		this.username = username;
		this.password = password;
		boot();
	}

	public EmailCurator(Map props, String server, String username, String password) {
		setConfig(props);
		this.server = server;
		this.username = username;
		this.password = password;
		boot();
	}

	public EmailCurator(Map props) {
		setConfig(props);
		server = (String) props.get("server");
		username = (String) props.get("username");
		password = (String) props.get("password");
		boot();
	}

	public void setHideMessageBody(boolean hideMessageBody) {
		this.hideMessageBody = hideMessageBody;
	}

	public void addHeader(String header) {
		headersToLearn.put(header, header);
	}

	public void addHeader(String header, String rewrite) {
		headersToLearn.put(header, rewrite);
	}

	public void boot() {
		// http://docs.oracle.com/javaee/6/api/javax/mail/internet/package-summary.html#mail.mime.base64.ignoreerrors

		config.put("mail.mime.base64.ignoreerrors", true); // Handle decode errors
		config.put("mail.mime.allowencodedmessages", true); // Microsoft Outlook will incorrectly encode message attachments
		config.put("mail.imap.partialfetch", false); //Certain IMAP servers do not implement the IMAP Partial FETCH functionality properly.
		addHeader("User-Agent");
		addHeader("X-mailer", "User-Agent");
		addHeader("X-Forwarded-For");
		addHeader("Organization");
		addHeader("Organisation", "Organization");
		addHeader("X-Confirm-Reading-To");
		addHeader("Thread-Index");
	}

	@Override
	public void curate(FactStream learn, Object curated) throws IQException, FactException {
		if (!canCurate(curated)) throw new IQException("self:learn:email:oops:cant-curate#"+curated);

		try {
			if (Folder.class.isInstance(curated)) {
				Folder inbox = (Folder)curated;
				FlagTerm query = new FlagTerm(new Flags( Flags.Flag.SEEN), true);
				curateFolders(query, learn, inbox, inbox.list());
			}
			if (Store.class.isInstance(curated)) {
				Store store = (Store)curated;
				Folder inbox = store.getDefaultFolder();
				FlagTerm query = new FlagTerm(new Flags( Flags.Flag.SEEN), true);
				curateFolders(query, learn, inbox, inbox.list());
			}

			curateInbox(learn, false);

		} catch (MessagingException e) {
			throw new IQException(e);
		} catch (IOException e) {
			throw new IQException(e);
		}
	}

	@Override
	public boolean canCurate(Object curated) {
		if (curated==null) return false;
		return Folder.class.isInstance(curated) || Store.class.isInstance(curated);
	}

	public FlagTerm setSeen(boolean seen) {
		return this.query = new FlagTerm(new Flags( Flags.Flag.SEEN), seen);
	}

	public void curateInbox(FactStream learn, boolean seen) throws MessagingException, IOException, FactException {
		setSeen(seen);
		Folder inbox = this.store.getDefaultFolder();
		curateFolders(query, learn, inbox, inbox.list());
	}

	public void curateFolders(FlagTerm query, FactStream learn, Folder parent, Folder[] folders) throws IOException, FactException {
		if (folders!=null) {
			for(Folder folder:folders) {
				try {
					String parentURI = parent.getURLName().toString();
					String folderURI = folder.getURLName().toString();
					learn.fact(parentURI, NS_MAIL+"contains", folderURI);
					curateFolder(query, learn, folder);
				} catch (MessagingException e) {
					log.error("Folder Failed: " + e.getMessage(), e);
				}
			}
		}
	}

	public void curateFolder(FactStream learn, String folder, boolean seen) throws MessagingException, IOException, FactException {
		Folder inbox = this.store.getFolder(folder);
		FlagTerm query = new FlagTerm(new Flags( Flags.Flag.SEEN), seen);
		curateFolder(query, learn, inbox);
	}

	public void curateFolder(FlagTerm query, FactStream learn, Folder inbox) throws MessagingException, IOException, FactException {
		log.debug("Learn Folder: "+inbox.getName()+" messages: "+inbox.getMessageCount()+" new: "+inbox.getNewMessageCount()+" unread: "+inbox.getUnreadMessageCount());
		// Set the mode to the read-only mode
		inbox.open(Folder.READ_ONLY);
		// +" mode: "+inbox.getMode()

		String folderURI = inbox.getURLName().toString();
		learn.fact(getIdentity(), NS_MAIL+"stores", folderURI);
		learn.fact(getIdentity(), LABEL, server, "string");
		learn.fact(folderURI, CURATOR+"by", getIdentity() );

		learn.fact(folderURI, A, NS_MAIL + "Folder");
		learn.fact(folderURI, LABEL, inbox.getName(), "string");
		learn.fact(folderURI, COMMENT, inbox.getFullName(), "string");
		Flags flags = inbox.getPermanentFlags();
		if (flags.contains(Flags.Flag.ANSWERED)) learn.fact(folderURI, NS_MAIL+"flag:answered", "true", "boolean");
		if (flags.contains(Flags.Flag.DELETED)) learn.fact(folderURI, NS_MAIL+"flag:deleted", "true", "boolean");
		if (flags.contains(Flags.Flag.DRAFT)) learn.fact(folderURI, NS_MAIL+"flag:draft", "true", "boolean");
		if (flags.contains(Flags.Flag.FLAGGED)) learn.fact(folderURI, NS_MAIL+"flag:flagged", "true", "boolean");
		if (flags.contains(Flags.Flag.RECENT)) learn.fact(folderURI, NS_MAIL+"flag:recent", "true", "boolean");
		if (flags.contains(Flags.Flag.SEEN)) learn.fact(folderURI, NS_MAIL+"flag:seen", "true", "boolean");
//        if (flags.contains(Flags.Flag.USER)) learn.fact(folderURI, NS_MAIL+"flag:user", "true", "boolean");
		String[] userFlags = flags.getUserFlags();
		for(String userFlag:userFlags) {
			learn.fact(folderURI, NS_MAIL+"flag:user", userFlag, "string");
		}
		learn.fact(folderURI, NS_MAIL+"readable", "true", "boolean");
		learn.fact(folderURI, NS_MAIL + "writable", ((inbox.getMode() & Folder.READ_WRITE)>0 ? "true" : "false"), "boolean");

		// Get messages
		Message messages[] = null;
		if (query!=null) {
			messages = inbox.search(query);
			log.debug("Searching found: " + messages.length);
		}
		else {
			if (nextMessageNumber>=0) inbox.getMessages(nextMessageNumber, inbox.getMessageCount());
			else messages = inbox.getMessages();
			log.debug("Retrieving found: " + messages.length+" @ "+nextMessageNumber);
		}

		// hint that we want to pre-fetch headers
/*
        FetchProfile fetchProfile = new FetchProfile();
        fetchProfile.add(FetchProfile.Item.ENVELOPE);
        inbox.fetch(messages, fetchProfile);
        log.debug("Fetched profiles: " + fetchProfile.getHeaderNames());
*/
		for(Message message:messages) {
			curateMessage(learn, message);
		}
		inbox.close(false);
	}

	public void curateMessage(FactStream learn, Message message) throws MessagingException, IOException, FactException {
		String folderURI = message.getFolder().getURLName().toString();
		String msgURI = folderURI+"#"+message.getMessageNumber();

		log.debug("\nLearn Message: "+message.getMessageNumber()+": "+message.getSubject()+" --> "+msgURI);

		// message IDs
		String[] msgIDs = message.getHeader("Message-ID");
		for(String msgID:msgIDs) {
			msgID = "message://"+msgID.substring(1, msgID.length()-1);
			learn.fact(msgURI, NS_MAIL+"id", msgID);
		}

		// Message Basics
		learn.fact(folderURI, NS_MAIL+"contains", msgURI);
		learn.fact(msgURI, A, NS_MAIL + "Message");
		String subject = message.getSubject();
		subject = subject==null?"":subject;
		learn.fact(msgURI, LABEL, subject, XSD.STRING);
		learn.fact(msgURI, NS_MAIL+"number", message.getMessageNumber(), "integer");

		learn.fact(msgURI, NS_MAIL+"sent", dateXSD.format(message.getSentDate()), XSD.DATE_TIME);
		learn.fact(msgURI, NS_MAIL+"received", dateXSD.format(message.getReceivedDate()), XSD.DATE_TIME);
		learn.fact(msgURI, NS_MAIL+"curated", dateXSD.format(new Date()), XSD.DATE_TIME);

		learn.fact(msgURI, NS_MAIL + "size", message.getSize(), XSD.INTEGER);

		// Body
		try {
			curateBodyPart(learn, msgURI, message, message);
		} catch(Exception e) {
			log.error("Message Body Failed: "+msgURI,e);
		}

		// FROM addresses
		for (Address address: message.getFrom()) {
			InternetAddress internetAddress = (InternetAddress)address;
			learn.fact(msgURI, NS_MAIL+"from", toEmail(internetAddress));
		}

		// TO addresses (aka mail:party)
		curateRecipient(learn, msgURI, message, Message.RecipientType.BCC);
		curateRecipient(learn, msgURI, message, Message.RecipientType.CC);
		curateRecipient(learn, msgURI, message, Message.RecipientType.TO);

		// Reply-To addresses
		for (Address address: message.getReplyTo()) {
			InternetAddress internetAddress = (InternetAddress)address;
			learn.fact(msgURI, NS_MAIL+"replyTo", toEmail(internetAddress));
		}

		// Message Headers
		curateHeaders(learn, msgURI, message.getAllHeaders());
	}

	public void curateMimePart(FactStream learn, String msgURI, Message message, Part part) throws MessagingException, IOException, FactException {
//        learn.fact( msgURI, NS_MAIL+"type", toMimeType(part.getContentType() ) );
		learn.fact( msgURI, LABEL, part.getDescription(), "string" );

		curateHeaders(learn, msgURI, part.getAllHeaders());
		curateBodyPart(learn, msgURI, message, part);
	}

	protected void curateBodyPart(FactStream learn, String msgURI, Message message, Part part) throws MessagingException, FactException, IOException {
		if (hideMessageBody) return;
		Object body = part.getContent();
		if (body==null) {
			log.error("Empty Body: "+msgURI);
			return;
		}

		String mimeType = toMimeType(part.getContentType());
		learn.fact( msgURI, NS_MAIL+"type", toMimeType(part.getContentType()) );
		learn.fact( msgURI, NS_MAIL+"contentType", part.getContentType(), "string" );

		String filename = toFilename(part.getFileName());
		if (part.isMimeType("text/*")) {
			boolean textIsHtml = part.isMimeType("text/html");
			log.debug("Learn : "+(textIsHtml?"HTML":"TXT")+" -> "+msgURI);

			if (body instanceof InputStream) {
				StringWriter writer = new StringWriter();
				IOUtils.copy((InputStream)body, writer, "UTF-8");
				learn.fact(msgURI, NS_MAIL + "body", writer.toString(), mimeType);
				log.warn("Learn Text Stream: " + part.getFileName() + " as " + mimeType);
				return;
			} else if (!(body instanceof String)) {
				log.warn("Unknown: Body: " + body);
			}
			if (textIsHtml) {
				Document doc = Jsoup.parse(body.toString());
				body = doc.text();
			}
//			learn.fact( msgURI, COMMENT, summariser.summarise(body.toString(), summaryLines), "string" );
			learn.fact( msgURI, NS_MAIL+"body", body.toString(), "string" );

		} else if (part.isMimeType("multipart/*")) {

//            learn(learn, msgURI, message, (Multipart) body);

			if (body instanceof Message) {

				log.debug("Learn Inner Message: "+msgURI+" -> "+filename);
				curateMessage(learn, (Message) body);

			} else if (body instanceof String) {

				log.debug("Learn Body: "+msgURI+" -> "+filename);
				learn.fact( msgURI, NS_MAIL+"body", "<body>"+body.toString()+"</body>", XSD.STRING );
//				learn.fact( msgURI, COMMENT, summariser.summarise(body.toString(), summaryLines), XSD.STRING );

			} else if (body instanceof MimeMultipart) {

				log.debug("Learn Multi-Part Body: " + msgURI + " -> " + mimeType);
				curateMultiPart(learn, msgURI, message, (Multipart) body);

			} else {
				log.error("Unknown Multi-Part Body: " + msgURI + " -> " + mimeType);
			}
//        } else if (Part.ATTACHMENT.equalsIgnoreCase(part.getDisposition()) ) {
//            log.debug("Learn Disposition: " + filename + "->" + msgURI);
		} else {
			if (filename==null) return;

			if (body instanceof InputStream || body instanceof BASE64DecoderStream) {
				String partURI = msgURI;
				log.debug("Learn Attachment: " + filename + "->" + partURI);
				learn.fact( partURI, LABEL, filename, "string" );
				learn.fact( partURI, NS_MAIL+"size", part.getSize(), "integer" );
				if (!saveAttachments) return;

				if (home!=null && home.exists()) {
					File msgHome = new File(home, PrettyString.sanitize(partURI));
					msgHome.mkdirs();
					final File attachFile = new File(msgHome,filename);
					final String attachURI = attachFile.toURI().toASCIIString();
					final MimeBodyPart mimePart = (MimeBodyPart)part;

					if ( attachFile.exists() )  {
						log.debug("Skipped: Existing Attachment: " + attachURI + "->" + attachFile.getAbsolutePath());
					} else {
						saveAttachment(learn, attachURI, mimePart, attachFile);
						learn.fact( partURI, NS_MAIL+"file", attachURI);
					}

				} else {
					log.error("Skipped: Nowhere to save attachment: " + filename);
				}

			} else {
				log.error("Skipped: Unsupported Attachment: " + msgURI + " -> " + part.getContentType());
			}
		}
	}

	// Learn about a multi-part email
	protected void curateMultiPart(FactStream learn, String partURI, Message message, Multipart multipart) throws MessagingException, IOException, FactException {
		log.debug("Learn Multi-Part: " + multipart.getContentType() + ", size: " + multipart.getCount());
		for (int i = 0; i < multipart.getCount(); i++) {
			String subPartURI = partURI+":"+i;
			Part bp = multipart.getBodyPart(i);
			learn.fact(partURI, NS_MAIL + "part", subPartURI);
			curateMimePart(learn, subPartURI, message, bp);
		}
	}

	// learn recipients
	protected void curateRecipient(FactStream learn, String msgURI, Message message, Message.RecipientType recipient) throws MessagingException, IOException, FactException {
		Address[] addresses = message.getRecipients(recipient);
		if (addresses==null) return;
		String type = recipient.toString().toLowerCase();
		for(Address address:addresses) {
			InternetAddress internetAddress = (InternetAddress)address;
			learn.fact(msgURI, NS_MAIL + type, toEmail(internetAddress));
			learn.fact( toEmail(internetAddress), NS_MAIL+"personal", internetAddress.getPersonal(), "string" );
		}
	}

	// learn headers
	protected void curateHeaders(FactStream learn, String msgURI, Enumeration<Header> headers) throws FactException {
		if (headers==null) return;

		while (headers.hasMoreElements()) {
			Header header = headers.nextElement();
			String name = headersToLearn.get(header.getName());
			if ( name != null ) {
				// (raw header names are mapped to canonical header names)
				log.debug("Learn Header: "+header.getName()+" -> "+name+" := "+header.getValue());
				learn.fact(msgURI, NS_MAIL + "header:" + name, header.getValue(), "string");
				learn.fact( NS_MAIL+name, A, NS_MAIL+"header" );
			} else {
				log.info("Skip Header: " + header.getName() + " := " + header.getValue());
			}
		}
	}

	protected void saveAttachmentAsync(FactStream learn, final String attachURI, final MimeBodyPart mimePart, final File attachFile) {
		Thread downloader = new Thread() {
			public void run() {
				try {
					log.debug("Saving ("+mimePart.getSize()+" chars): " + attachURI + "->" + attachFile.getAbsolutePath());
					mimePart.saveFile(attachFile);
					log.debug("Saved ("+attachFile.length()+" bytes) Attachment: " + attachURI + "->" + attachFile.getAbsolutePath());
				} catch(IOException e) {
					log.error("Failed to save attachment: "+attachFile.getName()+" for: "+attachURI, e);
					attachFile.delete();
				} catch (MessagingException e) {
					log.error("Failed to read attachment: "+attachFile.getName()+" for: "+attachURI, e);
					attachFile.delete();
				}
			}
		};
		downloader.start();
	}

	protected void saveAttachment(FactStream learn, final String attachURI, final MimeBodyPart mimePart, final File attachFile) {
		try {
			log.debug("Saving ("+mimePart.getSize()+" chars): " + attachURI + "->" + attachFile.getAbsolutePath());
			mimePart.saveFile(attachFile);
			log.debug("Saved ("+attachFile.length()+" bytes) Attachment: " + attachURI + "->" + attachFile.getAbsolutePath());
		} catch (MessagingException e) {
			log.error("Failed to read attachment: "+attachFile.getName()+" for: "+attachURI);
			log.error(e.getMessage());
			attachFile.delete();
		} catch (IOException e) {
			log.error("Retry save attachment: "+attachFile.getName()+" for: "+attachURI);
			try {
				FileOutputStream fos = new FileOutputStream(attachFile);
				IOUtils.copy(mimePart.getRawInputStream(), fos);
				fos.close();
			} catch (IOException e1) {
				log.error("Failed to save attachment: "+attachFile.getName()+" for: "+attachURI);
				log.error(e1.getMessage());
				attachFile.delete();
			} catch (MessagingException e1) {
				log.error("Failed to read attachment: "+attachFile.getName()+" for: "+attachURI);
				log.error(e1.getMessage());
				attachFile.delete();
			}
		}
	}

	protected String findBody(Part part) throws MessagingException, IOException {
		String body = findBody(part, "text/html");
		if (body!=null) {
			Document doc = Jsoup.parse(body.toString());
			return doc.html();
		}
		return findBody(part, "text/plain");
	}

	// recurse body and multi-parts to find a nominated mime-type
	protected String findBody(Part part, String type) throws MessagingException, IOException {
		if (part.isMimeType("multipart/*")) {
			String content = null;
			//Multipart
			Multipart mimePart = (Multipart)part;
			for (int i = 0; i < mimePart.getCount(); i++) {
				content = content==null?findBody(mimePart.getBodyPart(i), type):content;
			}
			if (content!=null) return content;
		}
		if (part.isMimeType(type)) {
			if (part.getContent() instanceof InputStream) {
				StringWriter writer = new StringWriter();
				IOUtils.copy((InputStream)part.getContent(), writer, "UTF-8");
				return writer.toString();
			} else {
				return part.getContent().toString();
			}
		}
		return null;
	}

	public void start() throws FactException {
		try {
			store.connect(server, port, username, password);
		} catch (MessagingException e) {
			throw new FactException("self:mail:oops:connect#"+e.getMessage(),e);
		}
	}

	public void stop() throws FactException {
		try {
			store.close();
		} catch (MessagingException e) {
			throw new FactException("self:mail:oops:close#"+e.getMessage(),e);
		}
	}

	public String getIdentity() {
		return store.getURLName().toString();
	}

	private String toEmail(InternetAddress internetAddress) {
		return "mailto://"+internetAddress.getAddress().toLowerCase();
	}

	private String toFilename(String filename) {
		URLCodec codec = new URLCodec();
		try {
			return codec.encode(filename);
		} catch (EncoderException e) {
			return null;
		}
	}

	private String toMimeType(String contentType) {
		int ix = contentType.indexOf(";");
		if (ix<0) return COMMONS.MIME_TYPE+contentType.toLowerCase();
		return COMMONS.MIME_TYPE+contentType.substring(0, ix).toLowerCase();
	}

	public File getHome() {
		return home;
	}

	public void setHome(File home) {
		this.home = home;
	}

	public Map getConfig() {
		return config;
	}

	public void setConfig(Map config) {
		this.config = new HashMap();
		Properties properties = new Properties();
		if (config!=null) {
			this.config.putAll(config);
			properties.putAll(config);
		}
		this.session = Session.getDefaultInstance(properties,null);
		try {
			this.store = session.getStore(storeType);
			log.debug("MailStore: "+storeType+" -> "+store.getURLName());
		} catch (javax.mail.NoSuchProviderException e) {
			e.printStackTrace();
		}

	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public Store getStore() {
		return store;
	}

	public void setStore(Store store) {
		this.store = store;
	}

	public boolean isHideMessageBody() {
		return hideMessageBody;
	}

	public boolean isSaveAttachments() {
		return saveAttachments;
	}

	public void setSaveAttachments(boolean saveAttachments) {
		this.saveAttachments = saveAttachments;
	}

	public String getStoreType() {
		return storeType;
	}

	public void setStoreType(String storeType) {
		this.storeType = storeType;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Converter
	public static FactStream toFactStream(Message message) throws IQException, FactException {
		FactStream learn = new N3Stream();
		EmailCurator curator = new EmailCurator();
		curator.curate(learn,message);
		return learn;
	}

}
