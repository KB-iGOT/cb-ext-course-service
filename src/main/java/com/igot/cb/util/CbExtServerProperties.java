package com.igot.cb.util;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
@Getter
@Setter
public class CbExtServerProperties {

    @Value("${cb-plan.update.publish.authorized.roles}")
    private String cbPlanUpdatePublishAuthorizedRoles;

    @Value("${cb.plan.v2.index}")
    private String cpPlanIndex;

    @Value("${cbplan.allowed.fields.update}")
    private String cbPlanUpdateAllowedFields;

    @Value("${elastic.required.field.cb.plan.json.path}")
    private String elasticCbPlanJsonPath;

    @Value("${msg.on.user.group.restriction.for.all.org}")
    private String msgOnUserGroupRestrictionForAllOrg;

    @Value("${non.text.fields}")
    private String nonTextFields;

    public List<String> getCbPlanUpdatePublishAuthorizedRoles() {
        return Arrays.asList(cbPlanUpdatePublishAuthorizedRoles.split(",", -1));
    }

    public void setCbPlanUpdatePublishAuthorizedRoles(String cbPlanUpdatePublishAuthorizedRoles) {
        this.cbPlanUpdatePublishAuthorizedRoles = cbPlanUpdatePublishAuthorizedRoles;
    }

    public List<String> getCbPlanUpdateAllowedFields() {
        return Arrays.asList(cbPlanUpdateAllowedFields.split(",", -1));
    }

    @Value("${notification.support.mail}")
    private String notificationSupportMail;

    @Value("${sb.service.url}")
    private String sbUrl;

    @Value("${sunbird.user.search.endpoint}")
    private String userSearchEndPoint;

    @Value("${notification.service.host}")
    private String notificationServiceHost;

    @Value("${notification.async.path}")
    private String notificationAsyncPath;

    @Value("${cb.wrapper.notification.host}")
    private String cbWrapperNotificationHost;

    @Value("${cb.wrapper.notification.path}")
    private String cbWrapperNotificationPath;

    @Value("${promotional.content.cache.ttl.minutes}")
    private int promotionalContentCacheTtlMinutes;

    @Value("${promotional.content.cache.max.size}")
    private int promotionalContentCacheMaxSize;

    @Value("${promotional.content.cache.warming.enabled}")
    private boolean promotionalContentCacheWarmingEnabled;

    @Value("${promotional.content.cache.batch.size}")
    private int promotionalContentCacheBatchSize;

    @Value("${promotional.content.cache.max.query.size}")
    private int promotionalContentCacheMaxQuerySize;
}
