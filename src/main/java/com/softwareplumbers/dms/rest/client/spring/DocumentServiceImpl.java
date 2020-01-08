/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.DocumentService;
import com.softwareplumbers.dms.Exceptions;
import com.softwareplumbers.dms.InputStreamSupplier;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.RepositoryObject;
import java.net.URI;
import java.util.Collections;
import javax.json.JsonObject;
import javax.ws.rs.core.MediaType;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author SWPNET\jonessex
 */
public class DocumentServiceImpl implements DocumentService {
    
    private String docsURL = null;
    
    public String getDocumentAPIURL() { return this.docsURL; }
    public void setDocumentAPIURL(String docsURL) { this.docsURL = docsURL; }
    
    private void applyCredentials(HttpHeaders headers) {
        
    }
    
    private class RepositoryObjectFactory {
    
    }
    
    private RepositoryObject sendMultipart(HttpMethod method, URI uri, HttpEntity<?> file, JsonObject metadata) {
		try {

			LinkedMultiValueMap<String, Object> multipartMap = new LinkedMultiValueMap<>();
			HttpHeaders metadataHeader = new HttpHeaders();
			metadataHeader.set("Content-Type", org.springframework.http.MediaType.APPLICATION_JSON_VALUE);
			HttpEntity<String> metadataEntity = new HttpEntity<String>(metadata.toString(), metadataHeader);
					
			multipartMap.add("metadata", metadataEntity);
			multipartMap.add("file", file);

			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(org.springframework.http.MediaType.MULTIPART_FORM_DATA);
			headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
			
			applyCredentials(headers);

			HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(multipartMap, headers);
			RestTemplate restTemplate = new RestTemplate();

			ResponseEntity<String> response = restTemplate.exchange(
					uri, method, requestEntity,
					String.class);

			JSONObject result = new JSONObject(response.getBody());
			return logReturn("sendMultipart", result);
		} catch (JSONException e) {
			throw logRethrow("sendMultipart",e);
		} catch (HttpStatusCodeException e) {
			throw logThrow("sendMultipart",handleError(e));
		}	
	}

    @Override
    public Document getDocument(Reference rfrnc) throws Exceptions.InvalidReference {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Reference createDocument(MediaType mt, InputStreamSupplier iss, JsonObject jo) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Reference updateDocument(String string, MediaType mt, InputStreamSupplier iss, JsonObject jo) throws Exceptions.InvalidDocumentId {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
