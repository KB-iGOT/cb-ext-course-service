package com.igot.cb.cbplan.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;

/**
 * Context object holding organization scope validation criteria.
 * Used during CB Plan validation to determine allowed organization scopes.
 */
@Getter
@AllArgsConstructor
@Setter
public class OrgScopeContext {
    private final boolean isCCA;
    private final boolean userIsL0;
    private final String userOrgId;
    private final boolean isAdmin;
    private final Set<String> criteriaOrgIds;
    private final boolean rootOrgMissingInSomeGroup;
    private final boolean ministryOrStateIdUsed;

}
