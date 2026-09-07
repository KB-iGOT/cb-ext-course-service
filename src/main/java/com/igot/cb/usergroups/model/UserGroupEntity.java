package com.igot.cb.usergroups.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * User Group entity for Cassandra operations.
 * Uses frozen<list<map<text, frozen<list<text>>>>> for criteria.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserGroupEntity {
    private String orgid;
    private String usergroupid;
    private String usergroupname;
    private String createdby;
    private String createddate;
    private String updatedby;
    private String updateddate;
    private List<Map<String, List<String>>> criteria;
    private String status;
}
