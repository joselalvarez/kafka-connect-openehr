package com.github.joselalvarez.openehr.connect.source.record;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.task.model.CompositionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class CompositionEventRecordMapper {

    private final ObjectMapper canonicalObjectMapper;

    public Struct mapStruct(CompositionEvent source) {
        CompositionEventRecord target = new CompositionEventRecord();

        target.setChangeType(source.getChangeType().getValue());
        target.setTimeCommitted(source.getTimeCommitted().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

        target.setEhrId(source.getEhrId().toString());
        target.setUid(source.getUid().toString());
        target.setVersion(source.getVersion());

        target.setArchetypeId(source.getArchetypeId() != null ? source.getArchetypeId().getFullId() : null);
        target.setTemplateId(source.getTemplateId() != null ? source.getTemplateId().getValue() : null);
        target.setCompositionId(source.getCompositionId() != null ? source.getCompositionId().getValue() : null);
        target.setReplacedId(source.getReplacedId() != null ? source.getReplacedId().getValue() : null);

        try {
            if (source.getComposition() != null)
                target.setComposition(canonicalObjectMapper.writeValueAsBytes(source.getComposition()));
        } catch (JsonProcessingException e) {
            log.error("Composition serialization error: {}", e);
        }
        return target.getDelegate();
    }

    public SourceRecord mapRecord(CompositionEvent source, String topic) {
        return new SourceRecord(
                source.getOffset().getPartitionMap(),
                source.getOffset().getOffsetMap(),
                topic,
                null, // Topic partition (default by kafka)
                Schema.STRING_SCHEMA, // Message key schema
                source.getEhrId().toString(), // Message key
                CompositionEventRecord.SCHEMA, // Value schema
                mapStruct(source) // Value
        );
    }

    public List<SourceRecord> mapRecordList(List<CompositionEvent> sourceList, String topic) {
        List<SourceRecord> resultList = new ArrayList<>();
        for (CompositionEvent ce : sourceList) {
            resultList.add(mapRecord(ce, topic));
        }
        return resultList;
    }
}
