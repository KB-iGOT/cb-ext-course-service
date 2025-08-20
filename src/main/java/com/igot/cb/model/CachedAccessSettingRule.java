package com.igot.cb.model;

import java.util.Map;

import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CachedAccessSettingRule {
    private long cachedTimeMillis;
    private String contextId;
    private String contextIdType;
    private Map<String, Object> contextData;
    private boolean isArchived;

    public CachedAccessSettingRule(String accessSettingRule) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> ruleData = mapper.readValue(accessSettingRule,
                    new TypeReference<Map<String, Object>>() {
                    });
            this.contextId = (String) ruleData.get("contextId");
            this.contextIdType = (String) ruleData.get("contextIdType");
            this.contextData = (Map<String, Object>) ruleData.get("contextData");
            this.isArchived = (Boolean) ruleData.getOrDefault("isArchived", false);
            this.cachedTimeMillis = System.currentTimeMillis();
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse access setting rule: " + e.getMessage(), e);
        }
    }

    public CachedAccessSettingRule(String contextId, String contextIdType, String contextDataStr,
            boolean isArchived) {
        this.contextId = contextId;
        this.contextIdType = contextIdType;
        if (StringUtils.hasText(contextDataStr)) {
            try {
                this.contextData = new ObjectMapper().readValue(contextDataStr,
                        new TypeReference<Map<String, Object>>() {
                        });
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse context data: " + e.getMessage(), e);
            }
        } else {
            throw new RuntimeException("Invalid context data: " + contextDataStr + ", for contextId: " + contextId);
        }
        this.isArchived = isArchived;
        this.cachedTimeMillis = System.currentTimeMillis();
    }

    public boolean isExpired(long ttlMillis) {
        return (System.currentTimeMillis() - cachedTimeMillis) > ttlMillis;
    }

    public String getCacheKey() {
        return contextId + "|" + contextIdType;
    }
}
