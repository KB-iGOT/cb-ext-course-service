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
    private String orgId;
    private String userGroupId;
    private String userGroupName;
    private String createdBy;
    private String createdDate;
    private String updatedBy;
    private String updatedDate;
    private List<Map<String, List<String>>> criteria;
    private String status;
}
