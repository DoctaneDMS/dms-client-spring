/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.keymanager.KeyManager;
import java.security.KeyStoreException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

/**
 *
 * @author jonathan
 */
@Profile("tmp")
@Configuration
public class TmpConfig {
    
    @Autowired
	Environment env;
    
    @Bean
    public KeyManager keyManager() throws KeyStoreException { 
        KeyManager<SecretKeys, KeyPairs> keyManager = new KeyManager<>();
        keyManager.setLocation("/var/tmp/doctane-proxy.keystore");
        keyManager.setPassword(env.getProperty("doctane.keystore.password"));
        keyManager.setRequiredSecretKeys(SecretKeys.class);
        keyManager.setRequiredKeyPairs(KeyPairs.class);
        return keyManager;
    }
    
    @Bean LoginHandler loginHandler() throws KeyStoreException {
        SignedRequestLoginHandler handler = new SignedRequestLoginHandler();
        handler.setKeyManager(keyManager());
        handler.setAuthURI("http://localhost:8080/auth/tmp/service?request={request}&signature={signature}");
        handler.setRepository("tmp");
        return handler;
    }
    
    @Bean
    public RepositoryService testService() throws KeyStoreException {
        DocumentServiceImpl service = new DocumentServiceImpl();
        service.setDocumentAPIURL("http://localhost:8080/docs/tmp/");
        service.setLoginHandler(loginHandler());
        return service;
    }
}
