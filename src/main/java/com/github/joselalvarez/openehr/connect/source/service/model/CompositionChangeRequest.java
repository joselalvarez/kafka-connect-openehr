package com.github.joselalvarez.openehr.connect.source.service.model;

import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
public class CompositionChangeRequest extends ChangeRequest {
    private String templateId;
    private String rootConcept;
}
