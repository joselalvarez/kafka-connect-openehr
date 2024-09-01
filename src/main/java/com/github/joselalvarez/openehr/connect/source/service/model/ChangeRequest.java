package com.github.joselalvarez.openehr.connect.source.service.model;

import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffset;
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
    private long maxPoll;

    private List<PartitionOffset> partitionOffsets = new ArrayList<>();

}
