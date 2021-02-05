/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.dms.Document;
import com.softwareplumbers.dms.DocumentLink;
import com.softwareplumbers.dms.Exceptions;
import com.softwareplumbers.dms.Options;
import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.RepositoryPath;
import com.softwareplumbers.dms.Workspace;
import com.softwareplumbers.dms.common.test.DocumentServiceTest;
import com.softwareplumbers.dms.common.test.TestUtils;
import com.softwareplumbers.dms.common.test.TestModel;
import java.util.UUID;
import javax.json.Json;
import javax.json.JsonObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static com.softwareplumbers.dms.common.test.TestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.springframework.beans.factory.annotation.Qualifier;
/**
 *
 * @author jonathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestConfig.class, QAConfig.class, TmpConfig.class, DemoConfig.class })
public class TestClient extends DocumentServiceTest {

    @Autowired
    RepositoryService service;
    
    @Autowired @Qualifier("workspaceMetadataModel")
    TestModel workspaceMetadataModel;
    
    @Autowired @Qualifier("documentMetadataModel")
    TestModel documentMetadataModel;
    
    public TestClient() {
        super(false);
    }
    
    @Override
    public RepositoryService service() {
        return service;
    }

    @Override
    public Reference randomDocumentReference() {
        return new Reference(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }
    
    @Override
    public String randomWorkspaceId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public TestModel documentMetadataModel() {
        return documentMetadataModel;
    }
    
    @Override
    public TestModel workspaceMetadataModel() {
        return workspaceMetadataModel;
    }

    @Test
    public void testSendWithEmptyMimeType() {
        Reference ref = service.createDocument("", ()->toStream(TestUtils.randomText()), Json.createObjectBuilder().build());
        assertNotNull(ref);
    }
    
    @Test
    public void testThatPathsAreNotIrredemablyMessedUpByMatrixParameters() throws Exceptions.BaseException {
        // Yep, a semicolon in a path name can really mess things up, because there is literally no way
        // to stop Jersey from interpreting that as a matrix parameter. 
        RepositoryPath name1 = randomQualifiedName();
        service().createWorkspaceByName(name1, Workspace.State.Open, workspaceMetadataModel().generateValue(), Options.CREATE_MISSING_PARENT);
        String originalText = randomText();
        Reference ref1 = service().createDocument("text/plain", ()->toStream(originalText), documentMetadataModel().generateValue());
        RepositoryPath docName = name1.add("aName;withaSemicolon");
        service().createDocumentLink(docName, ref1, Options.CREATE_MISSING_PARENT);
	    DocumentLink doc1 = (DocumentLink)service().getObjectByName(docName);
	    assertEquals(docName, doc1.getName());
	}  
}
