package com.igot.cb.user.service;

import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.user.UserUtilityService;
import com.igot.cb.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;



@Service
@Slf4j
public class UserUtilityServiceimpl implements UserUtilityService {


    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    private final CassandraOperation cassandraOperation;

    @Autowired
    DecryptServiceImpl decryptService;

    public UserUtilityServiceimpl(CassandraOperation cassandraOperation) {
        this.cassandraOperation = cassandraOperation;
    }


    @Override
    public void getUserDetailsFromDB(List<String> userIds, List<String> fields,
                                     Map<String, Map<String, String>> userInfoMap) {
        Map<String, Object> propertyMap = new HashMap<>();

        try {
            for (int i = 0; i < userIds.size(); i += 10) {
                List<String> userList = userIds.subList(i, Math.min(userIds.size(), i + 10));
                propertyMap.put(Constants.ID, userList);

                List<Map<String, Object>> userInfoList = cassandraOperation
                        .getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER, propertyMap, fields, null);
                for (Map<String, Object> user : userInfoList) {
                    Map<String, String> userMap = new HashMap<String, String>();
                    String userId = (String) user.get(Constants.USER_ID);

                    if (userInfoMap.containsKey(userId)) {
                        continue;
                    }

                    for (String field : fields) {
                        if (user.containsKey(field)) {
                            if (Constants.DECRYPTED_FIELDS.contains(field)) {
                                if (StringUtils.isNotBlank((String) user.get(field))) {
                                    String value = decryptService.decryptString((String) user.get(field));
                                    if (StringUtils.isBlank(value)) {
                                        logger.error(
                                                String.format("Invalid valid for field %s for user %s", field, userId));
                                    }
                                    userMap.put(field, value);
                                }
                            } else {
                                userMap.put(field, (String) user.get(field));
                            }
                        }
                    }
                    userInfoMap.put(userId, userMap);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get user details from DB. Exception: ", e);
        }
    }
}
