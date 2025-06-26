package com.igot.cb.transactional.util;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import org.joda.time.DateTime;
import org.springframework.http.HttpStatus;

public class ProjectUtil {

    public static ApiResponse createDefaultResponse(String api) {
        ApiResponse response = new ApiResponse();
        response.setId(api);
        response.setVer(Constants.API_VERSION_1);
        response.setParams(new ApiRespParam(UUID.randomUUID().toString()));
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
        response.setTs(DateTime.now().toString());
        return response;
    }

    public static Date getTimeStamp() {
        return new Timestamp(System.currentTimeMillis());
    }
}
