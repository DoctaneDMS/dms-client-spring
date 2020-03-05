/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.dms.RepositoryService;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.common.test.DocumentPartTest;
import com.softwareplumbers.dms.common.test.TestUtils;
import com.softwareplumbers.dms.common.test.TestModel;
import java.util.UUID;
import javax.json.Json;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static com.softwareplumbers.dms.common.test.TestUtils.*;
import static org.junit.Assert.assertNotNull;
import org.springframework.beans.factory.annotation.Qualifier;
/**
 *
 * @author jonathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestConfig.class, QAConfig.class, TmpConfig.class, DemoConfig.class })
public class TestDocumentPartClient extends DocumentPartTest {

    @Autowired
    RepositoryService service;
    
    @Autowired @Qualifier("workspaceMetadataModel")
    TestModel workspaceMetadataModel;
    
    @Autowired @Qualifier("documentMetadataModel")
    TestModel documentMetadataModel;
    
    @Override
    public RepositoryService service() {
        return service;
    }

    @Override
    public TestModel documentMetadataModel() {
        return documentMetadataModel;
    }
    
    @Override
    public TestModel workspaceMetadataModel() {
        return workspaceMetadataModel;
    }
}
