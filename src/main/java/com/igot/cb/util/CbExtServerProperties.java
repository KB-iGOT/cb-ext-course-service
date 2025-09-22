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

    @Value("${non.text.field}")
    private String nonTextField;

    public List<String> getCbPlanUpdatePublishAuthorizedRoles() {
        return Arrays.asList(cbPlanUpdatePublishAuthorizedRoles.split(",", -1));
    }

    public void setCbPlanUpdatePublishAuthorizedRoles(String cbPlanUpdatePublishAuthorizedRoles) {
        this.cbPlanUpdatePublishAuthorizedRoles = cbPlanUpdatePublishAuthorizedRoles;
    }
}
