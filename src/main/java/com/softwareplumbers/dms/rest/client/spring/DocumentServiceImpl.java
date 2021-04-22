/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.common.abstractpattern.parsers.Parsers;
import com.softwareplumbers.common.abstractquery.Query;
import com.softwareplumbers.dms.Constants;
import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.DocumentPart;
import com.softwareplumbers.dms.Exceptions;
import com.softwareplumbers.dms.Exceptions.*;
import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.dms.RepositoryPath;
import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.common.pipedstream.OutputStreamConsumer;
import com.softwareplumbers.dms.NamedRepositoryObject;
import com.softwareplumbers.dms.Options;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.RepositoryObject;
import com.softwareplumbers.dms.RepositoryPath.ElementType;
import com.softwareplumbers.dms.VersionedRepositoryObject;
import com.softwareplumbers.dms.Workspace;
import com.softwareplumbers.dms.common.impl.LocalData;
import com.softwareplumbers.dms.common.impl.DocumentLinkImpl;
import com.softwareplumbers.dms.common.impl.WorkspaceImpl;
import com.softwareplumbers.dms.common.impl.RepositoryObjectFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import javax.json.stream.JsonParsingException;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/** Implements the Doctane RepositoryService interface on top of a Spring REST client.
 *
 * @author SWPNET\jonessex
 */
public class DocumentServiceImpl implements RepositoryService {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(DocumentServiceImpl.class);
 
    private String docsUrl;
    private String workspaceUrl;
    private String catalogueUrl;
    private char pathEscapeChar = '$';
    private LoginHandler loginHandler;
    
    /** Set the URL for the Doctane web service to be called.
     * 
     * @param docsUrl URL for Doctane document operations
     */
    public void setDocumentAPIURL(String docsUrl) { 
        this.docsUrl = docsUrl;
    }
    
    /** Set the URL for the Doctane web service to be called.
     * 
     * @param workspaceUrl URL for Doctane workspace operations
     */ 
    public void setWorkspaceAPIURL(String workspaceUrl) { 
        this.workspaceUrl = workspaceUrl;
    }
 
    /** Set the URL for the Doctane web service to be called.
     * 
     * @param catalogueUrl URL for Doctane catalogue operations
     */
    public void setCatalogueAPIURL(String catalogueUrl) { 
        this.catalogueUrl = catalogueUrl;
    }
    
    /** Set the class that will handle authentication with the Doctane web service.
     * 
     * @param loginHandler Login handler which handle authentication process
     */
    public void setLoginHandler(LoginHandler loginHandler) {
        this.loginHandler = loginHandler;
    }
    
    public void setPathEscapeChar(char pathEscapeChar) {
        this.pathEscapeChar = pathEscapeChar;
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
    
    /** Convert a stream and mime type into an HttpEntity we can send to the server.
     * 
     * @param is Input stream to send
     * @param mimeType Type of data encoded in the stream
     * @return An HttpEntity that can be sent to the server.
     */
    private static HttpEntity<InputStreamResource> toEntity(InputStream is, String mimeType) {
	    InputStreamResource resource = new InputStreamResource(is);
		HttpHeaders fileHeader = new HttpHeaders();
		fileHeader.set("Content-Type", mimeType);
		return new HttpEntity<>(resource, fileHeader);
	}
	
    private OutputStreamConsumer toOutputStream(JsonObject jo) {
        return out -> {
            try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8.name()); JsonWriter jsonWriter = Json.createWriter(out)) {
                jsonWriter.write(jo);
            }
        };
    }
        
    /** Send multipart data (binary stream + JSON metadata) to the server.
     * 
     * @param uri URI to which we will send the data
     * @param method HTTP method used to send the data (POST or PUT)
     * @param mt Mime type of stream
     * @param iss Supplier of input stream
     * @param jo JSON object to send alongside the binary data.
     * @return Parsed JSON object send by server as response.
     * @throws IOException 
     */
    protected JsonObject sendMultipart(URI uri, HttpMethod method, String mt, InputStreamSupplier iss, JsonObject jo) throws IOException, Exceptions.ServerError {
        LOG.entry(uri, method, mt,iss, jo);
        if (jo == null) jo = Constants.EMPTY_METADATA;
        LinkedMultiValueMap<String, Object> multipartMap = new LinkedMultiValueMap<>();
        HttpHeaders metadataHeader = new HttpHeaders();
        metadataHeader.set("Content-Type", org.springframework.http.MediaType.APPLICATION_JSON_VALUE);
        
        
        HttpEntity<InputStreamResource> metadataEntity = toEntity(InputStreamSupplier.of(toOutputStream(jo)).get(), "application/json");
        HttpEntity<InputStreamResource> binaryEntity = toEntity(iss.get(), mt == null || mt.trim().isEmpty() ? "application/octet-stream" : mt);
        multipartMap.add("metadata", metadataEntity);
        multipartMap.add("file", binaryEntity);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(org.springframework.http.MediaType.MULTIPART_FORM_DATA);
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));

        loginHandler.applyCredentials(headers);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(multipartMap, headers);
        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<byte[]> response = restTemplate.exchange(
                uri, method, requestEntity,
                byte[].class);

        try (
            ByteArrayInputStream is = new ByteArrayInputStream(response.getBody()); 
            Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8.name()); 
            JsonReader jsonReader = Json.createReader(reader)
        ) {
            return LOG.exit(jsonReader.readObject());
        }
    }
    
    /** Send a JSON object to the server.
     * 
     * @param uri URI to which we will send the data
     * @param method HTTP method used to send the data (POST or PUT)
     * @param jo JSON object to send
     * @return Parsed JSON object send by server as response.
     * @throws IOException 
     */
    protected JsonObject sendJson(URI uri, HttpMethod method, JsonObject jo) throws IOException, Exceptions.ServerError {
        LOG.entry(uri, method, jo);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", org.springframework.http.MediaType.APPLICATION_JSON_UTF8_VALUE);
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
        headers.setAcceptCharset(Collections.singletonList(StandardCharsets.UTF_8));
        loginHandler.applyCredentials(headers);
        RestTemplate restTemplate = new RestTemplate();

        JsonObject result = restTemplate.execute(
            uri, 
            method, 
            request -> { 
                request.getHeaders().putAll(headers);
                try (JsonWriter writer = Json.createWriter(request.getBody())) { 
                    writer.write(jo);
                } 
            },
            response -> { 
                LOG.debug("Content type {}", response.getHeaders().getContentType());
                return Json.createReader(new InputStreamReader(response.getBody(), StandardCharsets.UTF_8)).readObject(); 
            }
        );

        return LOG.exit(result);
    }
    
    /** Send a JSON object to the server.
     * 
     * @param uri URI to which we will send the data
     * @param method HTTP method used to send the data (POST or PUT)
     * @param jo JSON object to send
     * @return Parsed JSON object send by server as response.
     * @throws IOException 
     */
    protected void sendJson(URI uri, HttpMethod method, JsonObject jo, OutputStream out) {
        LOG.entry(uri, method, jo);
        RestTemplate restTemplate = new RestTemplate();

        try {
            HttpHeaders credentialHeaders = new HttpHeaders();
            loginHandler.applyCredentials(credentialHeaders);
            restTemplate.execute(
                uri, 
                HttpMethod.PUT, 
                request -> { 
                    request.getHeaders().putAll(credentialHeaders); 
                    try (JsonWriter writer = Json.createWriter(request.getBody())) { 
                        writer.writeObject(jo); 
                    } 
                }, 
                response -> { writeBytes(response, out); return null; }
            );
        } catch (HttpStatusCodeException e) {
            throw getDefaultError(e);
        } catch (ServerError e) {
            throw new BaseRuntimeException(e);
        }
    }    
    
    /** Get JSON from the server.
     * 
     * @param uri URI from which we will request JSON data
     * @return Parsed JSON object send by server as response.
     */
    protected JsonObject getJson(URI uri) throws Exceptions.ServerError {
        LOG.entry(uri);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
        headers.setAcceptCharset(Collections.singletonList(StandardCharsets.UTF_8));
        loginHandler.applyCredentials(headers);

        RestTemplate restTemplate = new RestTemplate();

        JsonObject result = restTemplate.execute(
            uri, 
            HttpMethod.GET, 
            request -> { 
                request.getHeaders().putAll(headers);
            },
            response -> { 
                LOG.debug("Content type {}", response.getHeaders().getContentType());
                return Json.createReader(new InputStreamReader(response.getBody(), StandardCharsets.UTF_8)).readObject(); 
            }
        );
        
        return LOG.exit(result);
    }
    
    /** Send a DELETE operation to the server.
     * 
     * @param uri URI on which we will invoke DELETE.
     */
    protected JsonObject delete(URI uri) throws Exceptions.ServerError {
        LOG.entry(uri);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
        loginHandler.applyCredentials(headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.exchange(
                uri, HttpMethod.DELETE, new HttpEntity<>(headers), String.class);

        String body = response.getBody();

        try {
            return LOG.exit(Json.createReader(new StringReader(body)).readObject());
        } catch (JsonParsingException e) {
            LOG.warn("failed to parse Json: {}", body);
            throw e;
        }
    }
    
    /** Send a DELETE operation to the server.
     * 
     * @param uri URI on which we will invoke DELETE.
     * @param out Output stream to write data to
     */
    protected void delete(URI uri, OutputStream out) {
        LOG.entry(uri);
        RestTemplate restTemplate = new RestTemplate();

        try {
            HttpHeaders credentialHeaders = new HttpHeaders();
            loginHandler.applyCredentials(credentialHeaders);
            restTemplate.execute(
                uri, 
                HttpMethod.DELETE, 
                request -> request.getHeaders().putAll(credentialHeaders), 
                response -> { writeBytes(response, out); return null; }
            );
        } catch (HttpStatusCodeException e) {
            throw getDefaultError(e);
        } catch (ServerError e) {
            throw new BaseRuntimeException(e);
        }
    }
    
    /** Convert a raw ClientHttpResponse into a ServerError exception 
     * 
     * @param response Response to convert
     * @return A ServerError including any reason phrase from the response.
     */
    protected static ServerError rawError(ClientHttpResponse response) {
        try {
            return new ServerError(response.getStatusCode().getReasonPhrase());
        } catch (IOException ioe) {
            // Oh, FFS
            return new ServerError("unknown server error");
        }
    }
   
    private static void writeBytes(ClientHttpResponse response, OutputStream out) throws IOException {
        
        if (response.getStatusCode() != HttpStatus.OK) {
            throw getDefaultError(response.getBody()).orElseGet(()->new RemoteException(rawError(response)));
        }
        OutputStreamConsumer.of(()->response.getBody()).consume(out);
    } 
    
    /** Write data retrieved from the given URI to an output stream.
     * 
     * @param uri URI from which we are retrieving data
     * @param out Output stream to which we write the response
     * @throws IOException 
     */
    protected void writeData(URI uri, OutputStream out) throws IOException {
        LOG.entry(uri, out);
        try {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders credentialHeaders = new HttpHeaders();
            loginHandler.applyCredentials(credentialHeaders);
            restTemplate.execute(
                    uri, 
                    HttpMethod.GET, 
                    request -> request.getHeaders().putAll(credentialHeaders), 
                    response -> { writeBytes(response, out); return null; }
            );
        } catch (HttpStatusCodeException e) {
            throw getDefaultError(e);
        } catch (ServerError e) {
            throw new BaseRuntimeException(e);
        }
        LOG.exit();
    }
    
    protected void writeJson(URI uri, OutputStream out) {
        LOG.entry(uri, out);
        try {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            loginHandler.applyCredentials(headers); 
            headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
            restTemplate.execute(
                    uri, 
                    HttpMethod.GET, 
                    request -> request.getHeaders().putAll(headers), 
                    response -> { writeBytes(response, out); return null; }
            );
        } catch (HttpStatusCodeException e) {
            throw getDefaultError(e);
        } catch (ServerError e) {
            throw new BaseRuntimeException(e);
        }
        LOG.exit();        
    }
    
    private static void addUpdateOptions(UriComponentsBuilder builder, Options.Update... options) {        
        if (!Options.CREATE_MISSING_ITEM.isIn(options)) builder.queryParam("updateType", "UPDATE");
        if (Options.CREATE_MISSING_PARENT.isIn(options)) builder.queryParam("createWorkspace", "true");
        if (Options.EXTERNAL_REFERENCE.isIn(options)) builder.queryParam("externalReference", "true");
    }
    
    private static void addPublishOption(UriComponentsBuilder builder) {
        builder.queryParam("updateType", "PUBLISH");
    }
    
    private static void addUndeleteOption(UriComponentsBuilder builder) {
        builder.queryParam("updateType", "UNDELETE");
    }

    private static void addCreateOptions(UriComponentsBuilder builder, Options.Create... options) {        
        if (!Options.RETURN_EXISTING_LINK_TO_SAME_DOCUMENT.isIn(options)) builder.queryParam("returnExisting", "false");
        if (Options.CREATE_MISSING_PARENT.isIn(options)) builder.queryParam("createWorkspace", "true");
        if (Options.EXTERNAL_REFERENCE.isIn(options)) builder.queryParam("externalReference", "true");
        builder.queryParam("updateType", "CREATE");
    }
    
    private static void addCopyOptions(UriComponentsBuilder builder, Options.Create... options) {        
        if (Options.CREATE_MISSING_PARENT.isIn(options)) builder.queryParam("createWorkspace", "true");
        builder.queryParam("updateType", "COPY");
    }

    private static void addRenameOptions(UriComponentsBuilder builder, Options.Create... options) {        
        if (Options.CREATE_MISSING_PARENT.isIn(options)) builder.queryParam("createWorkspace", "true");
        builder.queryParam("updateType", "RENAME");
    }
    
    private static void addSearchOptions(UriComponentsBuilder builder, Options.Search... options) {        
        if (Options.SEARCH_OLD_VERSIONS.isIn(options)) builder.queryParam("searchHistory", "true");
        if (Options.RETURN_ALL_VERSIONS.isIn(options)) builder.queryParam("RETURN_ALL_VERSIONS", "true");
        if (Options.INCLUDE_DELETED.isIn(options)) builder.queryParam("INCLUDE_DELETED", "true");
    }
    
    private static void addGetOptions(UriComponentsBuilder builder, Options.Get... options) {        
    }
    
    private void addObjectName(UriComponentsBuilder builder, RepositoryPath objectName) {
        builder.queryParam("escapeWith", pathEscapeChar);
        if (objectName != null && !objectName.isEmpty()) builder.path(objectName.toString(pathEscapeChar)).path("/");
    }

    private static void addQuery(UriComponentsBuilder builder, Query query) {
        if (!query.isUnconstrained()) builder.queryParam("filter", query.urlEncode());
    }
    
    private static void addReference(UriComponentsBuilder builder, Reference reference) {
        if (reference != Constants.NO_REFERENCE) builder.path(reference.id).path("/");
        if (reference.version != null) builder.queryParam("version", reference.version);
    }
    
    @Override
    public void writeData(Reference reference, RepositoryPath partName, OutputStream out) throws InvalidReference, IOException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
        addReference(builder, reference);
        addObjectName(builder, partName);
        builder.path("file");
        try {
            writeData(builder.buildAndExpand(reference.id).encode().toUri(), out);
        } catch (RemoteException re) {
            re.rethrowAsLocal(InvalidReference.class);
            throw re;
        }
    }
        
    @Override
    public InputStream getData(Reference reference, RepositoryPath partName) throws IOException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
        addReference(builder, reference);
        addObjectName(builder, partName);
        builder.path("file");
        if (reference.version != null) builder = builder.queryParam("version", reference.version);  
        final URI uri = builder.buildAndExpand(reference.id).encode().toUri();
        return InputStreamSupplier.of(out->writeData(uri, out)).get();
    }
    
    @Override
    public InputStream getData(RepositoryPath objectName, Options.Get... options) throws InvalidObjectName, IOException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        addObjectName(builder, objectName);
        URI uri = builder.build().encode().toUri();
        try {
            return InputStreamSupplier.of(out->writeData(uri, out)).get();        
        } catch (RemoteException re) {
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re;
        }
    }

    @Override
    public void writeData(RepositoryPath objectName, OutputStream out, Options.Get... options) throws InvalidObjectName, IOException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        addObjectName(builder, objectName);
        try {
            writeData(builder.build().encode().toUri(), out);
        } catch (RemoteException re) {
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re;
        }
    }
    
    /** Parse a remote exception from a text stream.
     * 
     * @param body Text stream with JSON encoded remote error
     * @return An error, if successfully parsed.
     */
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
    
    /** Default error handler.
     * 
     * Where we have a server error message that can be parsed into a locally understood
     * exception type, do this, and then wrap it in a RemoteException.
     * 
     * @see Exceptions#buildException(javax.json.JsonObject) 
     * 
     * @param e an HttpStatusCodeException originating from a REST call to a Doctane server.
     * @return An appropriate RemoteException.
     */
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
            JsonObject result = sendMultipart(builder.buildAndExpand(id).encode().toUri(), HttpMethod.PUT, mediaType, data, metadata);
            return LOG.exit(Reference.fromJson(result));
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }catch (HttpStatusCodeException e) {
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
            JsonObject result = sendMultipart(builder.build().encode().toUri(), HttpMethod.POST, mediaType, data, metadata);
            Reference ref = Reference.fromJson(result);
            return LOG.exit(ref);
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
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
            JsonObject result = getJson(builder.buildAndExpand(ref.id).encode().toUri());
            return LOG.exit((Document)factory.build(result, Optional.empty()));
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
                case NOT_FOUND:
                    throw new InvalidReference(ref);
                default:
                    throw getDefaultError(e);
            }
        }    
    }

    @Override
    public DocumentLink createDocumentLink(RepositoryPath objectName, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Create... options) throws InvalidWorkspace, InvalidObjectName, InvalidWorkspaceState {
        LOG.entry(objectName, mediaType, iss, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addObjectName(builder, objectName);
            addCreateOptions(builder, options);
            JsonObject result = sendMultipart(builder.build().encode().toUri(), HttpMethod.PUT, mediaType, iss, metadata);
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
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
    public DocumentLink createDocumentLinkAndName(RepositoryPath workspaceName, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Create... options) throws InvalidWorkspace, InvalidWorkspaceState {
        LOG.entry(workspaceName, mediaType, iss, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addObjectName(builder, workspaceName);
            addCreateOptions(builder, options);
            JsonObject result = sendMultipart(builder.build().encode().toUri(), HttpMethod.POST, mediaType, iss, metadata);
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
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
    public DocumentLink createDocumentLinkAndName(RepositoryPath workspaceName, Reference reference, Options.Create... options) throws InvalidWorkspace, InvalidWorkspaceState, InvalidReference {
        LOG.entry(workspaceName, reference, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addObjectName(builder, workspaceName);
            addCreateOptions(builder, options);
            DocumentLink link = new DocumentLinkImpl(Constants.NO_ID, Constants.NO_VERSION, workspaceName, false, reference, Constants.NO_UPDATE_TIME, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, Constants.NO_METADATA, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.POST, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
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
    public DocumentLink createDocumentLink(RepositoryPath objectName, Reference reference, Options.Create... options) throws InvalidWorkspace, InvalidReference, InvalidObjectName, InvalidWorkspaceState {
        LOG.entry(objectName, reference, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addObjectName(builder, objectName);
            addCreateOptions(builder, options);
            DocumentLink link = new DocumentLinkImpl(Constants.NO_ID, Constants.NO_VERSION, objectName, false, reference, Constants.NO_UPDATE_TIME, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, Constants.NO_METADATA, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
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
    public DocumentLink updateDocumentLink(RepositoryPath objectName, String mediaType, InputStreamSupplier iss, JsonObject metadata, Options.Update... options) throws InvalidWorkspace, InvalidObjectName, InvalidWorkspaceState {
        LOG.entry(objectName, mediaType, iss, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            
            addObjectName(builder, objectName);
            addUpdateOptions(builder, options);
            
            JsonObject result;
            if (iss == null || mediaType == null) {
                DocumentLink link = new DocumentLinkImpl(Constants.NO_ID, Constants.NO_VERSION, objectName, false, Constants.NO_REFERENCE, Constants.NO_UPDATE_TIME, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, metadata, false, LocalData.NONE);
                result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, link.toJson());
            } else {
                result = sendMultipart(builder.build().encode().toUri(), HttpMethod.PUT, mediaType, iss, metadata);
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
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public DocumentLink updateDocumentLink(RepositoryPath objectName, Reference reference, Options.Update... options) throws InvalidWorkspace, InvalidObjectName, InvalidWorkspaceState, InvalidReference {
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            
            addObjectName(builder, objectName);
            addUpdateOptions(builder, options);
            DocumentLink link = new DocumentLinkImpl(Constants.NO_ID, Constants.NO_VERSION, objectName, false, reference, Constants.NO_UPDATE_TIME, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, Constants.NO_METADATA, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, link.toJson());
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
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public NamedRepositoryObject copyObject(RepositoryPath objectName, RepositoryPath targetName, boolean createParent) throws InvalidWorkspaceState, InvalidWorkspace, InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DocumentLink copyDocumentLink(RepositoryPath objectName, RepositoryPath targetName, boolean createParent) throws InvalidWorkspaceState, InvalidWorkspace, InvalidObjectName {
        LOG.entry(objectName, targetName, createParent);
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);        
            addObjectName(builder, targetName);
            addCopyOptions(builder, Options.Create.EMPTY.addOptionIf(Options.CREATE_MISSING_PARENT, createParent).build());
            DocumentLink link = new DocumentLinkImpl(Constants.NO_ID, Constants.NO_VERSION, objectName, false, Constants.NO_REFERENCE, Constants.NO_UPDATE_TIME, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, Constants.NO_METADATA, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public Workspace copyWorkspace(RepositoryPath objectName, RepositoryPath targetName, boolean createParent) throws InvalidWorkspaceState, InvalidWorkspace, InvalidObjectName {
        LOG.entry(objectName, targetName, createParent);
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            addObjectName(builder, targetName);
            addCopyOptions(builder, Options.Create.EMPTY.addOptionIf(Options.CREATE_MISSING_PARENT, createParent).build());
            Workspace workspace = new WorkspaceImpl(Constants.NO_ID, Constants.NO_VERSION, objectName, false, Constants.NO_STATE, Constants.NO_METADATA, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, workspace.toJson());
            return LOG.exit((Workspace)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public Workspace createWorkspaceByName(RepositoryPath objectName, Workspace.State state, JsonObject metadata, Options.Create... options) throws InvalidWorkspaceState, InvalidWorkspace, InvalidObjectName {
        LOG.entry(objectName, state, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            
            addObjectName(builder, objectName);
            addCreateOptions(builder, options);
            Workspace workspace = new WorkspaceImpl(Constants.NO_ID, Constants.NO_VERSION, objectName, false, state, metadata, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, workspace.toJson());
            return LOG.exit((Workspace)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public Workspace createWorkspaceAndName(RepositoryPath workspaceName, Workspace.State state, JsonObject metadata, Options.Create... options) throws InvalidWorkspaceState, InvalidWorkspace {
        LOG.entry(workspaceName, state, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);            
            addObjectName(builder, workspaceName);
            addCreateOptions(builder, options);
            Workspace workspace = new WorkspaceImpl(Constants.NO_ID, Constants.NO_VERSION, workspaceName, false, state, metadata, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.POST, workspace.toJson());
            return LOG.exit((Workspace)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }  
    }

    @Override
    public Workspace updateWorkspaceByName(RepositoryPath objectName, Workspace.State state, JsonObject metadata, Options.Update... options) throws InvalidWorkspace {
        LOG.entry(objectName, state, metadata, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            
            addObjectName(builder, objectName);
            addUpdateOptions(builder, options);
            Workspace workspace = new WorkspaceImpl(Constants.NO_ID, Constants.NO_VERSION, objectName, false, state, metadata, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, workspace.toJson());
            return LOG.exit((Workspace)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public Stream<DocumentLink> deleteDocument(RepositoryPath workspaceName, String documentId) throws InvalidWorkspace, InvalidDocumentId, InvalidWorkspaceState {
        LOG.entry(workspaceName, documentId);
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        
        addObjectName(builder, workspaceName.addId(documentId));

        try {
            InputStreamSupplier result = InputStreamSupplier.of(out->delete(builder.build().encode().toUri(), out));
            return LOG.exit(factory.build(result.get()).map(DocumentLink.class::cast));            
        } catch (RemoteException re) {
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidDocumentId.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            if (re.getCause() instanceof InvalidObjectName)
                throw new InvalidDocumentId(documentId);
            throw re; 
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }
    
    @Override
    public Stream<DocumentLink> undeleteDocument(RepositoryPath workspaceName, String documentId) throws InvalidWorkspace, InvalidDocumentId, InvalidWorkspaceState {
        LOG.entry(workspaceName, documentId);
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        
        addObjectName(builder, workspaceName.addId(documentId));
        addUndeleteOption(builder);

        try {
            InputStreamSupplier result = InputStreamSupplier.of(out->sendJson(builder.build().encode().toUri(), HttpMethod.PUT, Json.createObjectBuilder().build(), out));
            return LOG.exit(factory.build(result.get()).map(DocumentLink.class::cast));            
        } catch (RemoteException re) {
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidDocumentId.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            if (re.getCause() instanceof InvalidObjectName)
                throw new InvalidDocumentId(documentId);
            throw re; 
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }    

    @Override
    public NamedRepositoryObject deleteObjectByName(RepositoryPath objectName) throws InvalidWorkspace, InvalidObjectName, InvalidWorkspaceState {
        LOG.entry(objectName);
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        
        addObjectName(builder, objectName);
        try {
            JsonObject result = delete(builder.build().encode().toUri());
            return LOG.exit((NamedRepositoryObject)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            throw re; 
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public NamedRepositoryObject undeleteObjectByName(RepositoryPath objectName) throws InvalidWorkspace, InvalidObjectName, InvalidWorkspaceState {
        LOG.entry(objectName);
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
        
        addObjectName(builder, objectName);
        addUndeleteOption(builder);

        try {
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, Json.createObjectBuilder().build());
            return LOG.exit((NamedRepositoryObject)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            throw re; 
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }    


    @Override
    public RepositoryObject getPart(Reference reference, RepositoryPath partName) throws InvalidReference, InvalidObjectName {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
            addReference(builder, reference);
            addObjectName(builder, partName);
            JsonObject result = getJson(builder.build().encode().toUri());
            return LOG.exit((DocumentPart)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidReference.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public Stream<DocumentPart> catalogueParts(Reference reference, RepositoryPath partName) throws InvalidReference, InvalidObjectName {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
            addReference(builder, reference);
            addObjectName(builder, partName);
            URI uri = builder.build().encode().toUri();
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
    public DocumentLink getDocumentLink(RepositoryPath objectName, Options.Get... options) throws InvalidWorkspace, InvalidObjectName {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            
            addObjectName(builder, objectName);
            addGetOptions(builder, options);
            JsonObject result = getJson(builder.build().encode().toUri());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        }  catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public Stream<Document> catalogue(Query query, boolean searchHistory) {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(catalogueUrl);
            addQuery(builder, query);
            if (searchHistory) addSearchOptions(builder, Options.SEARCH_OLD_VERSIONS);
            URI uri = builder.build().encode().toUri();
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
    public Stream<NamedRepositoryObject> catalogueByName(RepositoryPath objectName, Query query, Options.Search... options) throws InvalidWorkspace {
        LOG.entry(objectName, query, Options.loggable(options));
        
        objectName = objectName.normalizeSearchPath(options);
        
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            
            addObjectName(builder, objectName);
            addQuery(builder, query);
            addSearchOptions(builder, options);
            URI uri = builder.build().encode().toUri();
            InputStreamSupplier result = InputStreamSupplier.of(out->writeJson(uri, out)); 
            return LOG.exit(factory.build(result.get()).map(NamedRepositoryObject.class::cast));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            throw LOG.throwing(re); 
        } catch (IOException e) {
            throw LOG.throwing(new RuntimeException(e));
        }
    }

    @Override
    public Stream<Document> catalogueHistory(Reference reference, Query query) throws InvalidReference {
        LOG.entry();
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(catalogueUrl);
            addReference(builder, reference);
            addQuery(builder, query);
            URI uri = builder.build().encode().toUri();
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
    public NamedRepositoryObject getObjectByName(RepositoryPath objectName, Options.Get... options) throws InvalidWorkspace, InvalidObjectName {
        LOG.entry(objectName, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            
            addObjectName(builder, objectName);
            addGetOptions(builder, options);
            JsonObject result = getJson(builder.build().encode().toUri());
            return LOG.exit((NamedRepositoryObject)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public Workspace getWorkspaceByName(RepositoryPath objectName) throws InvalidWorkspace, InvalidObjectName {
        LOG.entry(objectName);
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);
            
            addObjectName(builder, objectName);
            //addPartName(builder, options);
            //addOptions(builder, options);
            JsonObject result = getJson(builder.build().encode().toUri());
            return LOG.exit((Workspace)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public <T> Optional<T> getImplementation(Class<T> type) {
        return (type.isAssignableFrom(this.getClass())) ? Optional.of((T)this) : Optional.empty();
    }
    
    @Override
    public Workspace publishWorkspace(RepositoryPath objectName, String version, JsonObject metadata) throws InvalidWorkspace, InvalidVersionName {
        LOG.entry(objectName, version);    
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);            
            addObjectName(builder, objectName.setVersion(version));
            addPublishOption(builder);
            Workspace workspace = new WorkspaceImpl(Constants.NO_ID, version, objectName.setVersion(version), false, Constants.NO_STATE, metadata, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, workspace.toJson());
            return LOG.exit((Workspace)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidVersionName.class);
            re.rethrowAsLocal(InvalidWorkspace.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }
    
    @Override
    public DocumentLink publishDocumentLink(RepositoryPath objectName, String version, JsonObject metadata) throws InvalidObjectName, InvalidVersionName {
        LOG.entry(objectName, version);    
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);            
            addObjectName(builder, objectName.setVersion(version));
            addPublishOption(builder);
            DocumentLink link = new DocumentLinkImpl(Constants.NO_ID, version, objectName.setVersion(version), false, Constants.NO_REFERENCE, Constants.NO_UPDATE_TIME, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, metadata, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidVersionName.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }    

    @Override
    public DocumentLink renameDocumentLink(RepositoryPath path, RepositoryPath target, Options.Create... options) throws InvalidWorkspace, InvalidWorkspaceState, InvalidObjectName {
        LOG.entry(path, target, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);            
            addObjectName(builder, path);
            addRenameOptions(builder, options);
            DocumentLink link = new DocumentLinkImpl(Constants.NO_ID, Constants.NO_VERSION, target, false, Constants.NO_REFERENCE, Constants.NO_UPDATE_TIME, Constants.NO_TYPE, Constants.NO_LENGTH, Constants.NO_DIGEST, Constants.NO_METADATA, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, link.toJson());
            return LOG.exit((DocumentLink)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            re.rethrowAsLocal(InvalidWorkspaceState.class);
            re.rethrowAsLocal(InvalidObjectName.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public Workspace renameWorkspace(RepositoryPath path, RepositoryPath target, Options.Create... options) throws InvalidWorkspace, InvalidWorkspaceState, InvalidObjectName {
        LOG.entry(path, target, Options.loggable(options));
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(workspaceUrl);            
            addObjectName(builder, path);
            addRenameOptions(builder, options);
            Workspace workspace = new WorkspaceImpl(Constants.NO_ID, Constants.NO_VERSION, target, false, Constants.NO_STATE, Constants.NO_METADATA, false, LocalData.NONE);
            JsonObject result = sendJson(builder.build().encode().toUri(), HttpMethod.PUT, workspace.toJson());
            return LOG.exit((Workspace)factory.build(result, Optional.empty()));
        } catch (HttpStatusCodeException e) {
            RemoteException re = getDefaultError(e);
            re.rethrowAsLocal(InvalidWorkspace.class);
            throw re; 
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ServerError se) {
            throw new BaseRuntimeException(se);
        }
    }

    @Override
    public NamedRepositoryObject renameObject(RepositoryPath path, RepositoryPath target, Options.Create... options) throws InvalidWorkspace, InvalidWorkspaceState, InvalidObjectName {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
