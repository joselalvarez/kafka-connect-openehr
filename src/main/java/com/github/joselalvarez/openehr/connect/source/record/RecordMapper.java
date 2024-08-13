package com.github.joselalvarez.openehr.connect.source.record;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

public interface RecordMapper<T, O> {

    Struct mapStruct(T source);
    SourceRecord mapRecord(T source, String topic);
    List<SourceRecord> mapRecordList(List<T> sourceList, String topic);
    Map<String, ?> mapRecordPartition(O offset);
    Map<String, ?> mapRecordOffset(O offset);
    O mapOffset(int partition, Map<String, ?> recordOffset);
    O mapOffset(int partition);
}
