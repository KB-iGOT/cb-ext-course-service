package com.igot.cb.cbplan.model;

import lombok.Getter;
import lombok.Setter;

/**
 * Mutable flag holder tracking which criteria types are used in user groups.
 * Used during CB Plan validation to detect conflicts between rootOrgId and ministryOrStateId.
 */
@Getter
@Setter
public class CriteriaFlags {
    private boolean rootOrgIdUsed;
    private boolean ministryOrStateIdUsed;

    public CriteriaFlags() {
        this.rootOrgIdUsed = false;
        this.ministryOrStateIdUsed = false;
    }
}
