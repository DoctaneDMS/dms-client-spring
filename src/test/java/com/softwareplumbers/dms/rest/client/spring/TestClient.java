/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.rest.client.spring;

import com.softwareplumbers.dms.DocumentService;
import com.softwareplumbers.dms.Reference;
import com.softwareplumbers.dms.common.test.DocumentServiceTest;
import com.softwareplumbers.dms.common.test.TestUtils;
import java.util.UUID;
import javax.json.Json;
import javax.json.JsonObject;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * @author jonathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestConfig.class })
public class TestClient extends DocumentServiceTest {

    @Autowired
    DocumentService service;
    
    @Override
    public DocumentService service() {
        return service;
    }

    @Override
    public Reference randomDocumentReference() {
        return new Reference(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    @Override
    public JsonObject randomDocumentMetadata() {
        return Json.createObjectBuilder()
            .add("TradeDescription", TestUtils.randomText())
            .build();
    }
}
