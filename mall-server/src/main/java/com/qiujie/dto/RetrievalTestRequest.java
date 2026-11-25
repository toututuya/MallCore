package com.qiujie.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.Map;

@Data
public class RetrievalTestRequest {
    @NotBlank
    @Size(max = 500)
    private String query;
    private Map<String, Object> params;
}
