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
import java.io.StringReader;
import java.net.URI;
import java.util.Collections;
import javax.json.Json;
import javax.json.JsonObject;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 *
 * @author SWPNET\jonessex
 */
public class DocumentServiceImpl implements DocumentService {
    
    private String docsUrl;
    private LoginHandler loginHandler;
    
    public void setDocumentAPIURL(String docsUrl) { 
        this.docsUrl = docsUrl;
    }
    
    public void setLoginHandler(LoginHandler loginHandler) {
        this.loginHandler = loginHandler;
    }
    
    public DocumentServiceImpl(String docsUrl, LoginHandler loginHandler) {
        this.docsUrl = docsUrl;
        this.loginHandler = loginHandler;
    }
    
    public DocumentServiceImpl() {
        this(null, null);
    }
    
    private final RepositoryObjectFactory factory = new RepositoryObjectFactory();
        
    protected JsonObject sendMultipart(URI uri, HttpMethod method, javax.ws.rs.core.MediaType mt, InputStreamSupplier iss, JsonObject jo) throws IOException {
        LinkedMultiValueMap<String, Object> multipartMap = new LinkedMultiValueMap<>();
        HttpHeaders metadataHeader = new HttpHeaders();
        metadataHeader.set("Content-Type", org.springframework.http.MediaType.APPLICATION_JSON_VALUE);
        HttpHeaders binaryHeader = new HttpHeaders();
        if (mt != null) metadataHeader.set("Content-Type", mt.toString());
        HttpEntity<String> metadataEntity = new HttpEntity<String>(jo.toString(), metadataHeader);
        HttpEntity<InputStream> binaryEntity = new HttpEntity<InputStream>(iss.get(), binaryHeader);
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

        return Json.createReader(new StringReader(response.getBody())).readObject();
    }
    
    protected JsonObject sendJson(URI uri, HttpMethod method, JsonObject jo) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", org.springframework.http.MediaType.APPLICATION_JSON_VALUE);
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
        loginHandler.applyCredentials(headers);
        HttpEntity<String> requestEntity = new HttpEntity<String>(jo.toString(), headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.exchange(
                uri, method, requestEntity,
                String.class);

        return Json.createReader(new StringReader(response.getBody())).readObject();
    }
    
    protected JsonObject getJson(URI uri) {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
        loginHandler.applyCredentials(headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.exchange(
                uri, HttpMethod.GET, null, String.class);

        return Json.createReader(new StringReader(response.getBody())).readObject();        
    }
    
    protected InputStream getData(URI uri) {
        HttpHeaders headers = new HttpHeaders();
        loginHandler.applyCredentials(headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<InputStream> response = restTemplate.exchange(
                uri, HttpMethod.GET, null, InputStream.class);

        return response.getBody();        
    }
    
    protected InputStream getData(Reference reference) throws InvalidReference {        
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl).path("{documentId}/file");
            if (reference.version != null) builder = builder.queryParam("version", reference.version);
            return getData(builder.buildAndExpand(reference.id).toUri());
        } catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
                case NOT_FOUND: throw new InvalidReference(reference);
                default:
                    throw getDefaultError(e);
            }
        }        
    }
    
    public BaseRuntimeException getDefaultError(HttpStatusCodeException e) {
        String body = e.getResponseBodyAsString();
        JsonObject message = null;
        try {
            message = Json.createReader(new StringReader(body)).readObject();
        } catch (RuntimeException je) {
            // suppress
        }
        if (message != null)
            return new RemoteException(message);
        else
            return new ServerError(e.getStatusText());
    }
    
    @Override
    public Reference updateDocument(String id, javax.ws.rs.core.MediaType mediaType, InputStreamSupplier data, JsonObject metadata) throws InvalidDocumentId {
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
            builder.path("{documentId}");
            JsonObject result = sendMultipart(builder.buildAndExpand(id).toUri(), HttpMethod.PUT, mediaType, data, metadata);
            return Reference.fromJson(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
                case NOT_FOUND: throw new InvalidDocumentId(id);
                default:
                    throw getDefaultError(e);
            }
        }
    }

    @Override
    public Reference createDocument(javax.ws.rs.core.MediaType mediaType, InputStreamSupplier data, JsonObject metadata) {
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
            JsonObject result = sendMultipart(builder.build().toUri(), HttpMethod.PUT, mediaType, data, metadata);
            return Reference.fromJson(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
                default:
                    throw getDefaultError(e);
            }
        }
    }    
    
    @Override
    public Document getDocument(Reference ref) throws InvalidReference {
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(docsUrl);
            builder.path("{documentId}");
            if (ref.version != null) builder.queryParam("version", ref.version);
            JsonObject result = getJson(builder.buildAndExpand(ref.id).toUri());
            return (Document)factory.build(result, this::getData);
        } catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
                default:
                    throw getDefaultError(e);
            }
        }    
    }
}
