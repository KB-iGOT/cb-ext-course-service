package com.igot.cb.cbplan.model;

/**
 * The logged in org's two independent targeting capabilities, bundled so methods that need both
 * stay within Sonar's parameter-count limit (S107) instead of carrying isCCA and
 * allowsServiceBasedTargeting as two separate booleans.
 *
 * @param isCCA whether the org is CCA
 * @param allowsServiceBasedTargeting whether the org may target by service criteria across every
 *                                    organisation without CCA status
 */
public record OrgCapabilities(boolean isCCA, boolean allowsServiceBasedTargeting) {
}
