package com.github.joselalvarez.openehr.connect.source.service.model;

import lombok.Data;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
public class EventLogFilter {

    private ZonedDateTime fromDate;
    private ZonedDateTime toDate;
    private String templateId;
    private String rootConcept;
    private long batchSize;

    private int tablePartitions;
    private int tasks;
    private int taskId;

    private List<EventLogOffset> offsetList = new ArrayList<>();

    public EventLogFilter(int tablePartitions, int tasks, int taskId, long batchSize) {
        this.tablePartitions = tablePartitions;
        this.tasks = tasks;
        this.taskId = taskId;
        this.batchSize = batchSize;
    }

    public ZonedDateTime getBestFromDate() {
        ZonedDateTime minOffsetDate = null;
        if (offsetList != null && !offsetList.isEmpty()) {
            for (EventLogOffset offset : offsetList) {
                if (!offset.isEmpty() && (minOffsetDate == null || minOffsetDate.isAfter(offset.getDate()))) {
                    minOffsetDate = offset.getDate();
                }
            }
        }
        if (fromDate != null && minOffsetDate != null) {
            return fromDate.isAfter(minOffsetDate) ? fromDate : minOffsetDate;
        }

        return fromDate != null ? fromDate : minOffsetDate;

    }

}