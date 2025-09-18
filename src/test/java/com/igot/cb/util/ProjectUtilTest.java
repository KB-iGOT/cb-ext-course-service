package com.igot.cb.util;

import com.igot.cb.model.ApiResponse;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class ProjectUtilTest {

    @Test
    void testCreateDefaultResponse() {
        String api = "test.api";
        ApiResponse response = ProjectUtil.createDefaultResponse(api);
        
        assertNotNull(response);
        assertEquals(api, response.getId());
        assertEquals(Constants.API_VERSION_1, response.getVer());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertNotNull(response.getParams());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertNotNull(response.getParams().getResMsgId());
        assertNotNull(response.getTs());
    }

    @Test
    void testGetTimeStamp() {
        Date timestamp1 = ProjectUtil.getTimeStamp();
        Date timestamp2 = ProjectUtil.getTimeStamp();
        
        assertNotNull(timestamp1);
        assertNotNull(timestamp2);
        assertTrue(timestamp2.getTime() >= timestamp1.getTime());
    }
}