package com.github.joselalvarez.openehr.connect.source.service.model;

import com.github.joselalvarez.openehr.connect.source.task.offset.RecordOffset;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Getter
@SuperBuilder
public class ChangeRequest {

    private ZonedDateTime fromDate;
    private ZonedDateTime toDate;
    private Long maxPoll;

    private List<RecordOffset> offsetList = new ArrayList<>();

}
