/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.common.QualifiedName;
import com.softwareplumbers.common.abstractquery.Query;
import com.softwareplumbers.dms.Constants;
import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.DocumentPart;
import com.softwareplumbers.dms.Exceptions;
import com.softwareplumbers.dms.Exceptions.*;
import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.dms.InputStreamSupplier;
import com.softwareplumbers.dms.NamedRepositoryObject;
import com.softwareplumbers.dms.Options;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.Workspace;
import com.softwareplumbers.dms.common.impl.LocalData;
import com.softwareplumbers.dms.common.impl.DocumentLinkImpl;
import com.softwareplumbers.dms.common.impl.WorkspaceImpl;
import com.softwareplumbers.dms.common.impl.RepositoryObjectFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.StringReader;
import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.stream.JsonParsingException;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/** Implements the Doctane DocumentService interface on top of a Spring REST client.
 *
 * @author SWPNET\jonessex
 */
public class DocumentServiceImpl implements RepositoryService {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(DocumentServiceImpl.class);
 
    private String docsUrl;
    private String workspaceUrl;
    private LoginHandler loginHandler;
    
    /** Set the URL for the Doctane web service to be called.
     * 
     * @param docsUrl 
     */
    public void setDocumentAPIURL(String docsUrl) { 
        this.docsUrl = docsUrl;
    }
    
    /** Set the URL for the Doctane web service to be called.
     * 
     * @param docsUrl 
     */
    public void setDWorkspaceAPIURL(String workspaceUrl) { 
        this.workspaceUrl = workspaceUrl;
    }
 
    /** Set the class that will handle authentication with the Doctane web service.
     * 
     * @param loginHandler 
     */
    public void setLoginHandler(LoginHandler loginHandler) {
        this.loginHandler = loginHandler;
    }
    
    /** Construct a service using URL and login handler.
     * 
     * @param docsUrl
     * @param loginHandler 
     */
    public DocumentServiceImpl(String docsUrl, LoginHandler loginHandler) {
        this.docsUrl = docsUrl;
        this.loginHandler = loginHandler;
    }
    
    /** Construct an uninitialized service.
     * 
     * The DocumentAPIURL and LoginHandler properties must be set before using the service.
     */
    public DocumentServiceImpl() {
        this(null, null);
    }
    
    private final RepositoryObjectFactory factory = new RepositoryObjectFactory();
    
    protected static HttpEntity<InputStreamResource> toEntity(InputStream is, String mimeType) {
	    InputStreamResource resource = new InputStreamResource(is);
		HttpHeaders fileHeader = new HttpHeaders();
		fileHeader.set("Content-Type", mimeType);
		return new HttpEntity<>(resource, fileHeader);
	}
	
        
    protected JsonObject sendMultipart(URI uri, HttpMethod method, String mt, InputStreamSupplier iss, JsonObject jo) throws IOException {
        LOG.entry(uri, method, mt,iss, jo);
        LinkedMultiValueMap<String, Object> multipartMap = new LinkedMultiValueMap<>();
        HttpHeaders metadataHeader = new HttpHeaders();
        metadataHeader.set("Content-Type", org.springframework.http.MediaType.APPLICATION_JSON_VALUE);
        HttpEntity<String> metadataEntity = new HttpEntity<>(jo.toString(), metadataHeader);
        HttpEntity<InputStreamResource> binaryEntity = toEntity(iss.get(), mt == null || mt.trim().isEmpty() ? "application/octet-stream" : mt);
        multipartMap.add("metadata", metadataEntity);
        multipartMap.add("file", binaryEntity);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(org.springframework.http.MediaType.MULTIPART_FORM_DATA);
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));

        loginHandler.applyCredentials(headers);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(multipartMap, headers);
        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.exchange(
                uri, method, requestEntity,
                String.class);

        return LOG.exit(Json.createReader(new StringReader(response.getBody())).readObject());
    }
    
    protected JsonObject sendJson(URI uri, HttpMethod method, JsonObject jo) throws IOException {
        LOG.entry(uri, method, jo);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", org.springframework.http.MediaType.APPLICATION_JSON_VALUE);
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
        loginHandler.applyCredentials(headers);
        HttpEntity<String> requestEntity = new HttpEntity<>(jo.toString(), headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.exchange(
                uri, method, requestEntity,
                String.class);

        return LOG.exit(Json.createReader(new StringReader(response.getBody())).readObject());
    }
    
    protected JsonObject getJson(URI uri) {
        LOG.entry(uri);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
        loginHandler.applyCredentials(headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.exchange(
                uri, HttpMethod.GET, new HttpEntity<>(headers), String.class);
        
        String body = response.getBody();

        try {
            return LOG.exit(Json.createReader(new StringReader(body)).readObject());
        } catch (JsonParsingException e) {
            LOG.warn("failed to parse Json: {}", body);
            throw e;
        }
    }
    
    protected void delete(URI uri) {
        LOG.entry(uri);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
        loginHandler.applyCredentials(headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.exchange(
                uri, HttpMethod.DELETE, new HttpEntity<>(headers), String.class);

        LOG.exit();
    }
    
    protected static boolean writeBytes(ClientHttpResponse response, OutputStream out) throws IOException {
        
        if (response.getStatusCode() != HttpStatus.OK) return false;
        try (InputStream is = response.getBody()) {
            byte[] buf = new byte[1024];
            int len;
            while ((len = is.read(buf)) >= 0) { out.write(buf, 0, len); }
        } 
        out.close();
        return true;
    } 
    
    protected boolean writeData(URI uri, OutputStream out) throws IOException {
        LOG.entry(uri, out);
        RestTemplate restTemplate = new RestTemplate();
        return LOG.exit(
            restTemplate.execute(
                uri, 
                HttpMethod.GET, 
                request -> loginHandler.applyCredentials(request.getHeaders()), 
                response -> writeBytes(response, out)
            )
        );
    }
    
    @Override
    public void writeData(Reference reference, Optional<QualifiedName> partName, OutputStream out) throws InvalidReference, IOException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
        builder.path("{documentId}/");
        if (partName.isPresent()) builder.path("~/").path(partName.get().join("/")).path("/");
        builder.path("file");
        if (reference.version != null) builder = builder.queryParam("version", reference.version);
        if (!writeData(builder.buildAndExpand(reference.id).toUri(), out)) throw new InvalidReference(reference);
    }
    
    @Override
    public InputStream getData(Reference reference, Optional<QualifiedName> partName) throws IOException {        
        PipedOutputStream out = new PipedOutputStream();
        PipedInputStream in = new PipedInputStream(out);
        
        new Thread(()-> { 
            try {
                writeData(reference, partName, out); 
            } catch (InvalidReference | IOException e) {
                // Hopefully this will force an IOException in the reading thread
                try { in.close(); } catch (IOException e2) { }
            } 
        }).start();

        return in;
    }
    
    protected RemoteException getDefaultError(HttpStatusCodeException e) {
        LOG.entry(e);

        String body = e.getResponseBodyAsString();
        JsonObject message = null;
        try {
            message = Json.createReader(new StringReader(body)).readObject();
        } catch (RuntimeException je) {
            // suppress
        }
        if (message != null)
            return new RemoteException(Exceptions.buildException(message));
        else {
            LOG.warn("Unexplained error {} : {}", e.getRawStatusCode(), e.getResponseBodyAsString());
            return new RemoteException(new ServerError(e.getStatusText()));
        }
    }
    
    @Override
    public Reference updateDocument(String id, String mediaType, InputStreamSupplier data, JsonObject metadata) throws InvalidDocumentId {
        LOG.entry(id, mediaType, data, metadata);
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
            builder.path("{documentId}");
            JsonObject result = sendMultipart(builder.buildAndExpand(id).toUri(), HttpMethod.PUT, mediaType, data, metadata);
            return LOG.exit(Reference.fromJson(result));
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));
        } catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
                case NOT_FOUND: throw new InvalidDocumentId(id);
                default:
                    throw LOG.throwing(getDefaultError(e));
            }
        }
    }

    @Override
    public Reference createDocument(String mediaType, InputStreamSupplier data, JsonObject metadata) {
        LOG.entry(mediaType, data, metadata);

        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
            JsonObject result = sendMultipart(builder.build().toUri(), HttpMethod.POST, mediaType, data, metadata);
            Reference ref = Reference.fromJson(result);
            return LOG.exit(ref);
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));
        } catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
                default:
                    throw LOG.throwing(getDefaultError(e));
            }
        }
    }    
    
    @Override
    public Document getDocument(Reference ref) throws InvalidReference {
        LOG.entry(ref);
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
            builder.path("{documentId}/metadata");
            if (ref.version != null) builder.queryParam("version", ref.version);
            JsonObject result = getJson(builder.buildAndExpand(ref.id).toUri());
            return LOG.exit((Document)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
                case NOT_FOUND:
                    throw new InvalidReference(ref);
                default:
                    throw getDefaultError(e);
            }
        }    
    }

    private static void addOptions(UriComponentsBuilder builder, Options.Option... options) {
        for (Options.Option option : options) {
            if (!option.getName().equals(Options.PART.name)) builder.queryParam(option.getName(), option.getValue());
        }
    }
    
    private static void addRootId(UriComponentsBuilder builder, String rootId) {
        if (rootId != Constants.ROOT_ID) builder.path("~" + rootId + "/");
    }
    
    private static void addObjectName(UriComponentsBuilder builder, QualifiedName objectName) {
        if (!objectName.isEmpty()) builder.path(objectName.join("/")).path("/");
    }
    
    @Override
    public DocumentLink getDocumentLink(String rootId, QualifiedName workspaceName, String docId, Options.Get... options) throws InvalidWorkspace, InvalidObjectName, InvalidDocumentId {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, workspaceName);
            builder.path("~{docId}/");
            builder.path("metadata");
            addOptions(builder, options);
            JsonObject result = getJson(builder.buildAndExpand(docId).toUri());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            re.rethrowAsLocal(InvalidDocumentId.class);
            throw re;
            
        } 
    }

    @Override
    public DocumentLink createDocumentLink(String rootId, QualifiedName objectName, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Create... options) throws InvalidWorkspace, InvalidObjectName, InvalidWorkspaceState {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addOptions(builder, options);
            DocumentLink link = new DocumentLinkImpl(objectName, Constants.NO_REFERENCE, mediaType, Constants.NO_LENGTH, Constants.NO_DIGEST, metadata, false, LocalData.NONE);
            JsonObject result = sendMultipart(builder.build().toUri(), HttpMethod.PUT, mediaType, iss, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DocumentLink createDocumentLinkAndName(String rootId, QualifiedName workspaceName, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Create... options) throws InvalidWorkspace, InvalidWorkspaceState {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, workspaceName);
            addOptions(builder, options);
            DocumentLink link = new DocumentLinkImpl(workspaceName, Constants.NO_REFERENCE, mediaType, Constants.NO_LENGTH, Constants.NO_DIGEST, metadata, false, LocalData.NONE);
            JsonObject result = sendMultipart(builder.build().toUri(), HttpMethod.POST, mediaType, iss, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DocumentLink createDocumentLinkAndName(String rootId, QualifiedName workspaceName, Reference reference, Options.Create... options) throws InvalidWorkspace, InvalidWorkspaceState, InvalidReference {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, workspaceName);
            addOptions(builder, options);
            DocumentLink link = new DocumentLinkImpl(workspaceName, reference, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, Constants.NO_METADATA, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().toUri(), HttpMethod.POST, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            re.rethrowAsLocal(InvalidReference.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DocumentLink createDocumentLink(String rootId, QualifiedName objectName, Reference reference, Options.Create... options) throws InvalidWorkspace, InvalidReference, InvalidObjectName, InvalidWorkspaceState {
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addOptions(builder, options);
            DocumentLink link = new DocumentLinkImpl(objectName, reference, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, Constants.NO_METADATA, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().toUri(), HttpMethod.PUT, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            re.rethrowAsLocal(InvalidReference.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DocumentLink updateDocumentLink(String rootId, QualifiedName objectName, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Update... options) throws InvalidWorkspace, InvalidObjectName, InvalidWorkspaceState {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addOptions(builder, options);
            DocumentLink link = new DocumentLinkImpl(objectName, Constants.NO_REFERENCE, mediaType, Constants.NO_LENGTH, Constants.NO_DIGEST, metadata, false, LocalData.NONE);
            JsonObject result = sendMultipart(builder.build().toUri(), HttpMethod.PUT, mediaType, iss, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DocumentLink updateDocumentLink(String rootId, QualifiedName objectName, Reference reference, Options.Update... options) throws InvalidWorkspace, InvalidObjectName, InvalidWorkspaceState, InvalidReference {
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addOptions(builder, options);
            DocumentLink link = new DocumentLinkImpl(objectName, reference, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, Constants.NO_METADATA, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().toUri(), HttpMethod.PUT, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            re.rethrowAsLocal(InvalidReference.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public NamedRepositoryObject copyObject(String rootId, QualifiedName objectName, String targetRootId, QualifiedName targetName, boolean createParent) throws InvalidWorkspaceState, InvalidWorkspace, InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DocumentLink copyDocumentLink(String rootId, QualifiedName objectName, String targetRootId, QualifiedName targetName, boolean createParent) throws InvalidWorkspaceState, InvalidWorkspace, InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Workspace copyWorkspace(String rootId, QualifiedName objectName, String targetRootId, QualifiedName targetName, boolean createParent) throws InvalidWorkspaceState, InvalidWorkspace, InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Workspace createWorkspaceByName(String rootId, QualifiedName objectName, Workspace.State state, JsonObject metadata, Options.Create... options) throws InvalidWorkspaceState, InvalidWorkspace {
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addOptions(builder, options);
            Workspace workspace = new WorkspaceImpl(objectName, state, metadata, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().toUri(), HttpMethod.PUT, workspace.toJson());
            return LOG.exit((Workspace)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Workspace createWorkspaceAndName(String rootId, QualifiedName workspaceName, Workspace.State state, JsonObject metadata, Options.Create... options) throws InvalidWorkspaceState, InvalidWorkspace {
         try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, workspaceName);
            addOptions(builder, options);
            Workspace workspace = new WorkspaceImpl(workspaceName, state, metadata, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().toUri(), HttpMethod.POST, workspace.toJson());
            return LOG.exit((Workspace)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }    
    }

    @Override
    public Workspace updateWorkspaceByName(String rootId, QualifiedName objectName, Workspace.State state, JsonObject metadata, Options.Update... options) throws InvalidWorkspace {
         try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addOptions(builder, options);
            Workspace workspace = new WorkspaceImpl(objectName, state, metadata, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().toUri(), HttpMethod.PUT, workspace.toJson());
            return LOG.exit((Workspace)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteDocument(String rootId, QualifiedName workspaceName, String documentId) throws InvalidWorkspace, InvalidDocumentId, InvalidWorkspaceState {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        addRootId(builder, rootId);
        addObjectName(builder, workspaceName);
        addDocumentId(builder, documentId);
        delete(builder.build().toUri());
    }

    @Override
    public void deleteObjectByName(String rootId, QualifiedName objectName) throws InvalidWorkspace, InvalidObjectName, InvalidWorkspaceState {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        addRootId(builder, rootId);
        addObjectName(builder, objectName);
        delete(builder.build().toUri());
    }

    @Override
    public InputStream getData(String rootId, QualifiedName objectName, Options.Get... gets) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void writeData(String rootId, QualifiedName objectName, OutputStream out, Options.Get... gets) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DocumentPart getPart(Reference rfrnc, QualifiedName qn) throws InvalidReference, InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<DocumentPart> catalogueParts(Reference rfrnc, QualifiedName qn) throws InvalidReference, InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DocumentLink getDocumentLink(String rootId, QualifiedName objectName, Options.Get... gets) throws InvalidWorkspace, InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<Document> catalogue(Query query, boolean bln) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<NamedRepositoryObject> catalogueById(String string, Query query, Options.Search... searchs) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<NamedRepositoryObject> catalogueByName(String rootId, QualifiedName objectName, Query query, Options.Search... searchs) throws InvalidWorkspace {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<Document> catalogueHistory(Reference rfrnc, Query query) throws InvalidReference {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public NamedRepositoryObject getObjectByName(String rootId, QualifiedName objectName, Options.Get... gets) throws InvalidWorkspace, InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Workspace getWorkspaceByName(String string, QualifiedName qn) throws InvalidWorkspace, InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<DocumentLink> listWorkspaces(String rootId, QualifiedName objectName, Query query) throws InvalidDocumentId {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
