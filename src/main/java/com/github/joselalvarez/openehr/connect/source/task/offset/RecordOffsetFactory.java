package com.github.joselalvarez.openehr.connect.source.task.offset;

import java.util.List;
import java.util.Map;

public interface RecordOffsetFactory {
    List<Map<String, ?>> getCompositionChangePartitionsByTask(int taskId);
    List<Map<String, ?>> getEhrStatusChangePartitionsByTask(int taskId);
    RecordOffset buildRecordOffset(Map<String, ?> partition, Map<String, ?> offset);
}
