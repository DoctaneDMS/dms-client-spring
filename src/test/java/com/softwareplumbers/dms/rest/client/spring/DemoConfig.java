/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.dms.common.test.TestModel;
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
@Profile("demo")
@Configuration
public class DemoConfig {
    
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
        handler.setAuthURI("https://api.doctane.com/rest-server-filenet/auth/test/service?request={request}&signature={signature}");
        handler.setRepository("test");
        return handler;
    }
    
    @Bean
    public RepositoryService testService() throws KeyStoreException {
        DocumentServiceImpl service = new DocumentServiceImpl();
        service.setDocumentAPIURL("https://api.doctane.com/rest-server-filenet/docs/test/");
        service.setWorkspaceAPIURL("https://api.doctane.com/rest-server-filenet/ws/test/");
        service.setCatalogueAPIURL("https://api.doctane.com/rest-server-filenet/cat/test/");
        service.setLoginHandler(loginHandler());
        return service;
    }
    
    @Bean TestModel documentMetadataModel() {
        TestModel.Field uniqueField = new TestModel.IdField("DocFaceRef");
        TestModel model = new TestModel(
                new TestModel.StringField("TradeDescription", "BR001", "BR002", "BR003", "BR004"),
                new TestModel.BooleanField("BankDocument"),
                new TestModel.SessionIdField("BatchID"),
                uniqueField
        );
        model.setUniqueField(uniqueField);
        return model;
    }

    @Bean TestModel workspaceMetadataModel() {
        return new TestModel(
                new TestModel.StringField("EventDescription", "Event01", "Event02", "Event03", "Event04"),
                new TestModel.StringField("Branch", "BR001", "BR002", "BR003", "BR004"),
                new TestModel.SessionIdField("TheirReference")
        );
    }
}
