/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.common.QualifiedName;
import com.softwareplumbers.common.abstractpattern.parsers.Parsers;
import com.softwareplumbers.common.abstractquery.Query;
import com.softwareplumbers.dms.Constants;
import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.DocumentPart;
import com.softwareplumbers.dms.Exceptions;
import com.softwareplumbers.dms.Exceptions.*;
import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.common.pipedstream.OutputStreamConsumer;
import com.softwareplumbers.dms.NamedRepositoryObject;
import com.softwareplumbers.dms.Options;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.Workspace;
import com.softwareplumbers.dms.common.impl.LocalData;
import com.softwareplumbers.dms.common.impl.DocumentLinkImpl;
import com.softwareplumbers.dms.common.impl.WorkspaceImpl;
import com.softwareplumbers.dms.common.impl.RepositoryObjectFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Predicate;
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
    private String catalogueUrl;
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
     * @param workspaceUrl 
     */
    public void setWorkspaceAPIURL(String workspaceUrl) { 
        this.workspaceUrl = workspaceUrl;
    }
 
    /** Set the URL for the Doctane web service to be called.
     * 
     * @param workspaceUrl 
     */
    public void setCatalogueAPIURL(String catalogueUrl) { 
        this.catalogueUrl = catalogueUrl;
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
        if (jo == null) jo = Constants.EMPTY_METADATA;
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
    
    protected static ServerError rawError(ClientHttpResponse response) {
        try {
            return new ServerError(response.getStatusCode().getReasonPhrase());
        } catch (IOException ioe) {
            // Oh, FFS
            return new ServerError("unknown server error");
        }
    }
   
    
    protected static void writeBytes(ClientHttpResponse response, OutputStream out) throws IOException {
        
        if (response.getStatusCode() != HttpStatus.OK) {
            throw getDefaultError(response.getBody()).orElseGet(()->new RemoteException(rawError(response)));
        }
        OutputStreamConsumer.of(()->response.getBody()).consume(out);
    } 
    
    protected void writeData(URI uri, OutputStream out) throws IOException {
        LOG.entry(uri, out);
        RestTemplate restTemplate = new RestTemplate();
        try {
            restTemplate.execute(
                    uri, 
                    HttpMethod.GET, 
                    request -> loginHandler.applyCredentials(request.getHeaders()), 
                    response -> { writeBytes(response, out); return null; }
            );
        } catch (HttpStatusCodeException e) {
            throw getDefaultError(e);
        }
        LOG.exit();
    }
    
    private static void addUpdateOptions(UriComponentsBuilder builder, Options.Update... options) {        
        if (!Options.CREATE_MISSING_ITEM.isIn(options)) builder.queryParam("updateType", "UPDATE");
        if (Options.CREATE_MISSING_PARENT.isIn(options)) builder.queryParam("createWorkspace", "true");
    }

    private static void addCreateOptions(UriComponentsBuilder builder, Options.Create... options) {        
        if (!Options.RETURN_EXISTING_LINK_TO_SAME_DOCUMENT.isIn(options)) builder.queryParam("returnExisting", "false");
        if (Options.CREATE_MISSING_PARENT.isIn(options)) builder.queryParam("createWorkspace", "true");
    }
    
    private static void addCopyOptions(UriComponentsBuilder builder, Options.Create... options) {        
        if (Options.CREATE_MISSING_PARENT.isIn(options)) builder.queryParam("createWorkspace", "true");
        builder.queryParam("updateType", "COPY");
    }

    private static void addSearchOptions(UriComponentsBuilder builder, Options.Search... options) {        
        if (Options.SEARCH_OLD_VERSIONS.isIn(options)) builder.queryParam("searchHistory", "true");
    }
    
    private static void addGetOptions(UriComponentsBuilder builder, Options.Get... options) {        
    }
    
    private static void addRootId(UriComponentsBuilder builder, String rootId) {
        if (rootId != Constants.ROOT_ID) builder.path("~" + rootId + "/");
    }

    private static void addDocumentId(UriComponentsBuilder builder, String documentId) {
        if (documentId != Constants.NO_ID) builder.path("~" + documentId + "/");
    }
    
    private static void addReference(UriComponentsBuilder builder, Reference reference) {
        if (reference != Constants.NO_REFERENCE) builder.path(reference.id).path("/");
        if (reference.version != null) builder.queryParam("version", reference.version);
    }
    
    private static void addObjectName(UriComponentsBuilder builder, QualifiedName objectName) {
        if (objectName != null && !objectName.isEmpty()) builder.path(objectName.join("/")).path("/");
    }
    
    private static void addPartName(UriComponentsBuilder builder, Options.Option... options) {
        Optional<QualifiedName> partName = Options.PART.getValue(options);
        if (partName.isPresent()) builder.path("~/").path(partName.get().join("/")).path("/");
    }
    
    private static void addPartName(UriComponentsBuilder builder, Optional<QualifiedName> partName) {
        if (partName.isPresent()) builder.path("~/").path(partName.get().join("/")).path("/");
    }
    
    private static void addQuery(UriComponentsBuilder builder, Query query) {
        if (!query.isUnconstrained()) builder.queryParam("filter", query.urlEncode());
    }
    
    @Override
    public void writeData(Reference reference, Optional<QualifiedName> partName, OutputStream out) throws InvalidReference, IOException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
        addReference(builder, reference);
        addPartName(builder, partName);
        builder.path("file");
        try {
            writeData(builder.buildAndExpand(reference.id).toUri(), out);
        } catch (RemoteException re) {
            re.rethrowAsLocal(InvalidReference.class);
            throw re;
        }
    }
        
    @Override
    public InputStream getData(Reference reference, Optional<QualifiedName> partName) throws IOException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
        addReference(builder, reference);
        addPartName(builder, partName);
        builder.path("file");
        if (reference.version != null) builder = builder.queryParam("version", reference.version);  
        final URI uri = builder.buildAndExpand(reference.id).toUri();
        return InputStreamSupplier.of(out->writeData(uri, out)).get();
    }
    
    @Override
    public InputStream getData(String rootId, QualifiedName objectName, Options.Get... options) throws InvalidObjectName, IOException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        addRootId(builder, rootId);
        addObjectName(builder, objectName);
        addPartName(builder, options);
        builder.path("file");
        URI uri = builder.build().toUri();
        try {
            return InputStreamSupplier.of(out->writeData(uri, out)).get();        
        } catch (RemoteException re) {
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re;
        }
    }

    @Override
    public void writeData(String rootId, QualifiedName objectName, OutputStream out, Options.Get... options) throws InvalidObjectName, IOException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        addRootId(builder, rootId);
        addObjectName(builder, objectName);
        addPartName(builder, options);
        builder.path("file");
        try {
            writeData(builder.build().toUri(), out);
        } catch (RemoteException re) {
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re;
        }
    }
    
    protected static Optional<RemoteException> getDefaultError(InputStream body) {
        JsonObject message = null;
        try {
            message = Json.createReader(body).readObject();
        } catch (RuntimeException je) {
            // suppress
        }
        if (message != null)
            return Optional.of(new RemoteException(Exceptions.buildException(message)));
        else
            return Optional.empty();
    }
    
    protected static RemoteException getDefaultError(HttpStatusCodeException e) {
        LOG.entry(e);
        InputStream body = new ByteArrayInputStream(e.getResponseBodyAsByteArray());
        Optional<RemoteException> re = getDefaultError(body);
        return re.orElseGet(() -> {
            LOG.warn("Unexplained error {} : {}", e.getRawStatusCode(), e.getResponseBodyAsString());
            return new RemoteException(new ServerError(e.getStatusText()));
        });
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


    
    @Override
    public DocumentLink getDocumentLink(String rootId, QualifiedName workspaceName, String docId, Options.Get... options) throws InvalidWorkspace, InvalidObjectName, InvalidDocumentId {
        LOG.entry(rootId, workspaceName, docId, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, workspaceName);
            builder.path("~{docId}/");
            builder.path("metadata");
            addGetOptions(builder, options);
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
        LOG.entry(rootId, objectName, mediaType, iss, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addCreateOptions(builder, options);
            JsonObject result = sendMultipart(builder.build().toUri(), HttpMethod.PUT, mediaType, iss, metadata);
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
        LOG.entry(rootId, workspaceName, mediaType, iss, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, workspaceName);
            addCreateOptions(builder, options);
            JsonObject result = sendMultipart(builder.build().toUri(), HttpMethod.POST, mediaType, iss, metadata);
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
        LOG.entry(rootId, workspaceName, reference, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, workspaceName);
            addCreateOptions(builder, options);
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
        LOG.entry(rootId, objectName, reference, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addCreateOptions(builder, options);
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
        LOG.entry(rootId, objectName, mediaType, iss, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addUpdateOptions(builder, options);
            
            JsonObject result = null;
            if (iss == null || mediaType == null) {
                DocumentLink link = new DocumentLinkImpl(objectName, Constants.NO_REFERENCE, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, metadata, false, LocalData.NONE);
                result = sendJson(builder.build().toUri(), HttpMethod.PUT, link.toJson());
            } else {
                result = sendMultipart(builder.build().toUri(), HttpMethod.PUT, mediaType, iss, metadata);
            }
            
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
            addUpdateOptions(builder, options);
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
        LOG.entry(rootId, objectName, targetRootId, targetName, createParent);
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addCopyOptions(builder, Options.Create.EMPTY.addOptionIf(Options.CREATE_MISSING_PARENT, createParent).build());
            DocumentLink link = new DocumentLinkImpl(objectName, Constants.NO_REFERENCE, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, Constants.NO_METADATA, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().toUri(), HttpMethod.PUT, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Workspace copyWorkspace(String rootId, QualifiedName objectName, String targetRootId, QualifiedName targetName, boolean createParent) throws InvalidWorkspaceState, InvalidWorkspace, InvalidObjectName {
        LOG.entry(rootId, objectName, targetRootId, targetName, createParent);
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, targetRootId);
            addObjectName(builder, targetName);
            addCopyOptions(builder, Options.Create.EMPTY.addOptionIf(Options.CREATE_MISSING_PARENT, createParent).build());
            Workspace workspace = new WorkspaceImpl(objectName, Constants.NO_ID, Constants.NO_STATE, Constants.NO_METADATA, false, LocalData.NONE);
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
    public Workspace createWorkspaceByName(String rootId, QualifiedName objectName, Workspace.State state, JsonObject metadata, Options.Create... options) throws InvalidWorkspaceState, InvalidWorkspace {
        LOG.entry(rootId, objectName, state, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addCreateOptions(builder, options);
            Workspace workspace = new WorkspaceImpl(objectName, Constants.NO_ID, state, metadata, false, LocalData.NONE);
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
        LOG.entry(rootId, workspaceName, state, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, workspaceName);
            addCreateOptions(builder, options);
            Workspace workspace = new WorkspaceImpl(workspaceName, Constants.NO_ID, state, metadata, false, LocalData.NONE);
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
        LOG.entry(rootId, objectName, state, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addUpdateOptions(builder, options);
            Workspace workspace = new WorkspaceImpl(objectName, Constants.NO_ID, state, metadata, false, LocalData.NONE);
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
        LOG.entry(rootId, workspaceName, documentId);
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        addRootId(builder, rootId);
        addObjectName(builder, workspaceName);
        addDocumentId(builder, documentId);
        try {
            delete(builder.build().toUri());
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidDocumentId.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            throw re; 
        }
        LOG.exit();
    }

    @Override
    public void deleteObjectByName(String rootId, QualifiedName objectName) throws InvalidWorkspace, InvalidObjectName, InvalidWorkspaceState {
        LOG.entry(rootId, objectName);
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        addRootId(builder, rootId);
        addObjectName(builder, objectName);
        try {
            delete(builder.build().toUri());
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            throw re; 
        }
        LOG.exit();
    }



    @Override
    public DocumentPart getPart(Reference reference, QualifiedName partName) throws InvalidReference, InvalidObjectName {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
            addReference(builder, reference);
            addPartName(builder, Optional.of(partName));
            JsonObject result = getJson(builder.build().toUri());
            return LOG.exit((DocumentPart)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidReference.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        } 
    }

    @Override
    public Stream<DocumentPart> catalogueParts(Reference reference, QualifiedName partName) throws InvalidReference, InvalidObjectName {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
            addReference(builder, reference);
            addPartName(builder, Optional.of(partName));
            URI uri = builder.build().toUri();
            InputStreamSupplier result = InputStreamSupplier.of(out->writeData(uri, out)); 
            return LOG.exit(factory.build(result.get()).map(DocumentPart.class::cast));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidReference.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DocumentLink getDocumentLink(String rootId, QualifiedName objectName, Options.Get... options) throws InvalidWorkspace, InvalidObjectName {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addGetOptions(builder, options);
            JsonObject result = getJson(builder.build().toUri());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        } 
    }

    @Override
    public Stream<Document> catalogue(Query query, boolean searchHistory) {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(catalogueUrl);
            addQuery(builder, query);
            if (searchHistory) addSearchOptions(builder, Options.SEARCH_OLD_VERSIONS);
            URI uri = builder.build().toUri();
            InputStreamSupplier result = InputStreamSupplier.of(out->writeData(uri, out)); 
            return LOG.exit(factory.build(result.get()).map(Document.class::cast));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Stream<NamedRepositoryObject> catalogueById(String rootId, Query query, Options.Search... options) {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addQuery(builder, query);
            addSearchOptions(builder, options);
            URI uri = builder.build().toUri();
            InputStreamSupplier result = InputStreamSupplier.of(out->writeData(uri, out)); 
            return LOG.exit(factory.build(result.get()).map(NamedRepositoryObject.class::cast));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Stream<NamedRepositoryObject> catalogueByName(String rootId, QualifiedName objectName, Query query, Options.Search... options) throws InvalidWorkspace {
        LOG.entry();
        
        //If there are no wildcards already, we need to add a "*" to the end of the name.
        if (!Options.NO_IMPLICIT_WILDCARD.isIn(options)) {
            Predicate<String> hasWildcards = element -> !Parsers.parseUnixWildcard(element).isSimple();
            if (objectName.isEmpty() || objectName.indexFromEnd(hasWildcards) < 0) {
                objectName = objectName.add("*");
            }
            }
        
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addQuery(builder, query);
            addSearchOptions(builder, options);
            URI uri = builder.build().toUri();
            InputStreamSupplier result = InputStreamSupplier.of(out->writeData(uri, out)); 
            return LOG.exit(factory.build(result.get()).map(NamedRepositoryObject.class::cast));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Stream<Document> catalogueHistory(Reference reference, Query query) throws InvalidReference {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(catalogueUrl);
            addReference(builder, reference);
            addQuery(builder, query);
            URI uri = builder.build().toUri();
            InputStreamSupplier result = InputStreamSupplier.of(out->writeData(uri, out)); 
            return LOG.exit(factory.build(result.get()).map(Document.class::cast));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public NamedRepositoryObject getObjectByName(String rootId, QualifiedName objectName, Options.Get... options) throws InvalidWorkspace, InvalidObjectName {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            addPartName(builder, options);
            addGetOptions(builder, options);
            JsonObject result = getJson(builder.build().toUri());
            return LOG.exit((NamedRepositoryObject)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        }
    }

    @Override
    public Workspace getWorkspaceByName(String rootId, QualifiedName objectName) throws InvalidWorkspace, InvalidObjectName {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addRootId(builder, rootId);
            addObjectName(builder, objectName);
            //addPartName(builder, options);
            //addOptions(builder, options);
            JsonObject result = getJson(builder.build().toUri());
            return LOG.exit((Workspace)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        }
    }

    @Override
	public Stream<DocumentLink> listWorkspaces(String documentId, QualifiedName pathFilter, Query query, Options.Search... options) throws InvalidDocumentId {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            if (pathFilter != null && !pathFilter.isEmpty()) {
                addObjectName(builder, pathFilter);
                addDocumentId(builder, documentId);
            } else {
                builder.queryParam("id", documentId);
            }
            addQuery(builder, query);
            addSearchOptions(builder, options);
            URI uri = builder.build().toUri();
            InputStreamSupplier result = InputStreamSupplier.of(out->writeData(uri, out)); 
            return LOG.exit(factory.build(result.get()).map(DocumentLink.class::cast));
        } catch (RemoteException re) {
            re.rethrowAsLocal(InvalidDocumentId.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> Optional<T> getImplementation(Class<T> type) {
        return (type.isAssignableFrom(this.getClass())) ? Optional.of((T)this) : Optional.empty();
    }
}
