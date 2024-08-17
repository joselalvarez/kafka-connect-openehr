package com.github.joselalvarez.openehr.connect.source.task.model;

import com.github.joselalvarez.openehr.connect.source.record.RecordOffset;
import lombok.Data;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
public class EventFilter {

    private ZonedDateTime fromDate;
    private ZonedDateTime toDate;
    private String templateId;
    private String rootConcept;
    private String namespace;

    private List<RecordOffset> offsetList = new ArrayList<>();
}
