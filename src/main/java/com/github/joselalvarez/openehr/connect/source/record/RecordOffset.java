package com.github.joselalvarez.openehr.connect.source.record;

import java.util.Map;

public interface RecordOffset {
    boolean supports(Class<?> clazz);
    Map<String, ?> getPartitionMap();
    Map<String, ?> getOffsetMap();
}
