package com.github.joselalvarez.openehr.connect.source.task.offset;

import java.util.Map;

public interface PartitionOffset {
    Map<String, ?> getPartitionMap();
    Map<String, ?> getOffsetMap();
}
