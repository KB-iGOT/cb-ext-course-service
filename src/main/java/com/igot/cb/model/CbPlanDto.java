package com.igot.cb.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;

import java.util.Date;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
public class CbPlanDto {

    private String id;

    @Required
    @NotBlank
    private String name;

    @Required
    @NotBlank
    private String contentType;

    @Required
    @NotNull
    private List<String> contentList;

    @Required
    @NotBlank
    private String orgScope;

    @Required
    @NotNull
    private Map<String, Object> contextData;

    @Required
    @NotNull
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private Date endDate;

    private Boolean isApar ;

    private List<String> orgIdList ;
}
