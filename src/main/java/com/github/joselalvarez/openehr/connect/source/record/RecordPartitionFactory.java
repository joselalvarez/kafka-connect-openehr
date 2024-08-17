package com.github.joselalvarez.openehr.connect.source.record;

import java.util.List;
import java.util.Map;

public interface RecordPartitionFactory{
    List<Map<String, ?>> getTaskPartitions(int taskId);
    RecordOffset build(Map<String, ?> partition, Map<String, ?> offset);
}
