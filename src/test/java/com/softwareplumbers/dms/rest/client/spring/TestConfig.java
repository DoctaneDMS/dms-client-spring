/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.dms.common.test.TestModel;
import com.softwareplumbers.keymanager.KeyManager;
import java.io.IOException;
import java.security.KeyStoreException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

/** Sample configuration for a Spring client.
 *
 * @author Jonathan Essex
 */
@Profile("test")
@Configuration
public class TestConfig {
    
    @Autowired
	Environment env;
    
    @Bean
    public KeyManager keyManager() throws KeyStoreException, IOException { 
        KeyManager<SecretKeys, KeyPairs> keyManager = new KeyManager<>();
        keyManager.setLocation(env.getProperty("local.keystore"));
        keyManager.setPublishLocation(env.getProperty("local.certs.dir"));
        keyManager.setPassword(env.getProperty("doctane.keystore.password"));
        keyManager.setRequiredSecretKeys(SecretKeys.class);
        keyManager.setRequiredKeyPairs(KeyPairs.class);
        return keyManager;
    }
    
    @Bean LoginHandler loginHandler() throws KeyStoreException, IOException {
        SignedRequestLoginHandler handler = new SignedRequestLoginHandler();
        handler.setKeyManager(keyManager());
        handler.setAuthURI("http://localhost:8080/auth/test/service?request={request}&signature={signature}");
        handler.setRepository("test");
        return handler;
    }
    
    @Bean
    public RepositoryService testService() throws KeyStoreException, IOException {
        DocumentServiceImpl service = new DocumentServiceImpl();
        service.setDocumentAPIURL("http://localhost:8080/docs/test/");
        service.setWorkspaceAPIURL("http://localhost:8080/ws/test/");
        service.setCatalogueAPIURL("http://localhost:8080/cat/test/");
        service.setLoginHandler(loginHandler());
        return service;
    }
    
    @Bean public TestModel documentMetadataModel() {
        TestModel.Field uniqueField = new TestModel.IdField("DocFaceRef");
        TestModel model = new TestModel(
                new TestModel.StringField("DocumentTitle", "BillOfLading", "Invoice", "Purchase Order", "Insurance", "Manifest"),
                new TestModel.StringField("TradeDescription", "BR001", "BR002", "BR003", "BR004"),
                new TestModel.BooleanField("BankDocument"),
                new TestModel.SessionIdField("BatchID"),
                uniqueField
        );
        model.setUniqueField(uniqueField);
        return model;
    }

    @Bean public TestModel workspaceMetadataModel() {
        return new TestModel(
                new TestModel.StringField("EventDescription", "Event01", "Event02", "Event03", "Event04"),
                new TestModel.StringField("Branch", "BR001", "BR002", "BR003", "BR004"),
                new TestModel.SessionIdField("TheirReference")
        );
    }     
}
