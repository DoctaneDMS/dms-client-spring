/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.DocumentService;
import com.softwareplumbers.dms.Exceptions.*;
import com.softwareplumbers.dms.InputStreamSupplier;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.common.impl.RepositoryObjectFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.StringReader;
import java.net.URI;
import java.util.Collections;
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
public class DocumentServiceImpl implements DocumentService {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(DocumentServiceImpl.class);
 
    private String docsUrl;
    private LoginHandler loginHandler;
    
    /** Set the URL for the Doctane web service to be called.
     * 
     * @param docsUrl 
     */
    public void setDocumentAPIURL(String docsUrl) { 
        this.docsUrl = docsUrl;
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
    
    protected void writeData(Reference reference, OutputStream out) throws InvalidReference, IOException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl).path("{documentId}/file");
        if (reference.version != null) builder = builder.queryParam("version", reference.version);
        if (!writeData(builder.buildAndExpand(reference.id).toUri(), out)) throw new InvalidReference(reference);
    }
    
    protected InputStream getData(Reference reference) throws IOException {        
        PipedOutputStream out = new PipedOutputStream();
        PipedInputStream in = new PipedInputStream(out);
        
        new Thread(()-> { 
            try {
                writeData(reference, out); 
            } catch (InvalidReference | IOException e) {
                // Hopefully this will force an IOException in the reading thread
                try { in.close(); } catch (IOException e2) { }
            } 
        }).start();

        return in;
    }
    
    protected BaseRuntimeException getDefaultError(HttpStatusCodeException e) {
        LOG.entry(e);

        String body = e.getResponseBodyAsString();
        JsonObject message = null;
        try {
            message = Json.createReader(new StringReader(body)).readObject();
        } catch (RuntimeException je) {
            // suppress
        }
        if (message != null)
            return new RemoteException(message);
        else {
            LOG.warn("Unexplained error {} : {}", e.getRawStatusCode(), e.getResponseBodyAsString());
            return new ServerError(e.getStatusText());
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
            return LOG.exit((Document)factory.build(result, this::getData));
        } catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
                case NOT_FOUND:
                    throw new InvalidReference(ref);
                default:
                    throw getDefaultError(e);
            }
        }    
    }
}
