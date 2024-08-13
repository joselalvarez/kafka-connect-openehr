package com.github.joselalvarez.openehr.connect.source.service.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.time.ZonedDateTime;
import java.util.UUID;

@Getter
@AllArgsConstructor
@ToString
public class EventLogOffset {

    private int partition;
    private ZonedDateTime date;
    private UUID uid;
    private Integer version;

    public EventLogOffset(int partition) {
        this.partition = partition;
    }

    public boolean isEmpty() {
        return date == null || uid == null || version == null;
    }

}
