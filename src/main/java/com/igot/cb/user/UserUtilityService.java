package com.igot.cb.user;

import java.util.List;
import java.util.Map;

public interface UserUtilityService {

    public void getUserDetailsFromDB(List<String> userIds, List<String> fields,
                                     Map<String, Map<String, String>> userInfoMap);
}
