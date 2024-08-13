package com.github.joselalvarez.openehr.connect.source.record;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionEvent;
import com.github.joselalvarez.openehr.connect.source.service.model.EventLogOffset;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@RequiredArgsConstructor
@Slf4j
public class CompositionEventRecordMapper implements RecordMapper<CompositionEvent, EventLogOffset> {

    private final ObjectMapper canonicalObjectMapper;

    @Override
    public Struct mapStruct(CompositionEvent source) {
        CompositionEventRecord target = new CompositionEventRecord();

        target.setChangeType(source.getChangeType().getValue());
        target.setTimeCommitted(source.getTimeCommitted().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

        target.setEhrId(source.getEhrId().toString());
        target.setUid(source.getUid().toString());
        target.setVersion(source.getVersion());

        target.setArchetypeId(source.getArchetypeID() != null ? source.getArchetypeID().getFullId() : null);
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

    @Override
    public SourceRecord mapRecord(CompositionEvent source, String topic) {
        return new SourceRecord(
                mapRecordPartition(source.getOffset()),
                mapRecordOffset(source.getOffset()),
                topic,
                null, // Topic partition (default by kafka)
                Schema.STRING_SCHEMA, // Message key schema
                source.getEhrId().toString(), // Message key
                CompositionEventRecord.SCHEMA, // Value schema
                mapStruct(source) // Value
        );
    }

    @Override
    public List<SourceRecord> mapRecordList(List<CompositionEvent> sourceList, String topic) {
        List<SourceRecord> resultList = new ArrayList<>();
        for (CompositionEvent ce : sourceList) {
            resultList.add(mapRecord(ce, topic));
        }
        return resultList;
    }

    @Override
    public Map<String, ?> mapRecordPartition(EventLogOffset offset) {
        return new LinkedHashMap<>() {{
            put("cp", String.valueOf(offset.getPartition()));
        }};
    }

    @Override
    public Map<String, ?> mapRecordOffset(EventLogOffset offset) {
        return !offset.isEmpty() ? new LinkedHashMap<>() {{
            put("d", offset.getDate().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            put("u", offset.getUid().toString());
            put("v", offset.getVersion().toString());
        }} : null;
    }

    @Override
    public EventLogOffset mapOffset(int partition, Map<String, ?> recordOffset) {

        return new EventLogOffset(
                partition,
                recordOffset.get("d") != null ? ZonedDateTime.parse(recordOffset.get("d").toString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME) : null,
                recordOffset.get("u") != null ? UUID.fromString((String) recordOffset.get("u")) : null,
                recordOffset.get("v") != null ? Integer.valueOf((String) recordOffset.get("v")) : null
        );
    }

    @Override
    public EventLogOffset mapOffset(int partition) {
        return mapOffset(partition, new HashMap<>());
    }


}
