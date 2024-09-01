package com.github.joselalvarez.openehr.connect.source.task.offset;

import java.util.List;
import java.util.Map;

public interface PartitionOffsetFactory {
    List<Map<String, ?>> getCompositionChangePartitionsByTask(int taskId);
    List<Map<String, ?>> getEhrStatusChangePartitionsByTask(int taskId);
    PartitionOffset buildPartitionOffset(Map<String, ?> partition, Map<String, ?> offset);
}
