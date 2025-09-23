package com.igot.cb.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ApiRespParamTest {

    @Test
    void testDefaultConstructor() {
        ApiRespParam param = new ApiRespParam();
        
        assertNull(param.getResMsgId());
        assertNull(param.getMsgId());
        assertNull(param.getErr());
        assertNull(param.getStatus());
        assertNull(param.getErrMsg());
    }

    @Test
    void testConstructorWithId() {
        String id = "test-id";
        ApiRespParam param = new ApiRespParam(id);
        
        assertEquals(id, param.getResMsgId());
        assertEquals(id, param.getMsgId());
    }

    @Test
    void testSettersAndGetters() {
        ApiRespParam param = new ApiRespParam();
        
        param.setResMsgId("resMsgId");
        param.setMsgId("msgId");
        param.setErr("error");
        param.setStatus("success");
        param.setErrMsg("error message");
        
        assertEquals("resMsgId", param.getResMsgId());
        assertEquals("msgId", param.getMsgId());
        assertEquals("error", param.getErr());
        assertEquals("success", param.getStatus());
        assertEquals("error message", param.getErrMsg());
    }


}