package com.igot.cb.util;

import org.junit.jupiter.api.Test;

import com.igot.cb.model.ApiRespParam;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ApiRespParamTest {

    @Test
    void testDefaultConstructorAndSettersGetters() {
        ApiRespParam param = new ApiRespParam();

        param.setResMsgId("res1");
        param.setMsgId("msg1");
        param.setErr("error");
        param.setStatus("SUCCESS");
        param.setErrMsg("No error");

        assertEquals("res1", param.getResMsgId());
        assertEquals("msg1", param.getMsgId());
        assertEquals("error", param.getErr());
        assertEquals("SUCCESS", param.getStatus());
        assertEquals("No error", param.getErrMsg());
    }

    @Test
    void testConstructorWithId() {
        ApiRespParam param = new ApiRespParam("id123");

        assertEquals("id123", param.getResMsgId());
        assertEquals("id123", param.getMsgId());
        assertNull(param.getErr());
        assertNull(param.getStatus());
        assertNull(param.getErrMsg());
    }
}
