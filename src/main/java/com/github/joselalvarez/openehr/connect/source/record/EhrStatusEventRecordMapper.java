package com.github.joselalvarez.openehr.connect.source.record;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusEvent;
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
public class EhrStatusEventRecordMapper implements LocalOffsetStorage.OffsetMapper<EventLogOffset> {

    private final ObjectMapper canonicalObjectMapper;

    public Struct mapStruct(EhrStatusEvent source) {
        EhrStatusEventRecord target = new EhrStatusEventRecord();

        target.setChangeType(source.getChangeType().getValue());
        target.setTimeCommitted(source.getTimeCommitted().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

        target.setEhrId(source.getEhrId().toString());
        target.setUid(source.getUid().toString());
        target.setVersion(source.getVersion());

        target.setArchetypeId(source.getArchetypeId());
        target.setEhrStatusId(source.getEhrStatusId() != null ? source.getEhrStatusId().getValue() : null);
        target.setReplacedId(source.getReplacedId() != null ? source.getReplacedId().getValue() : null);

        target.setSubjectType(source.getSubjectType());
        target.setSubjectNamespace(source.getSubjectNamespace());
        target.setSubjectId(source.getSubjectId());
        target.setSubjectIdScheme(source.getSubjectIdScheme());

        try {
            if (source.getEhrStatus() != null)
                target.setEhrStatus(canonicalObjectMapper.writeValueAsBytes(source.getEhrStatus()));
        } catch (JsonProcessingException e) {
            log.error("Composition serialization error: {}", e);
        }
        return target.getDelegate();
    }

    public SourceRecord mapRecord(EhrStatusEvent source, String topic) {
        return new SourceRecord(
                mapRecordPartition(source.getOffset()),
                mapRecordOffset(source.getOffset()),
                topic,
                null, // Topic partition (default by kafka)
                Schema.STRING_SCHEMA, // Message key schema
                source.getEhrId().toString(), // Message key
                EhrStatusEventRecord.SCHEMA, // Value schema
                mapStruct(source) // Value
        );
    }

    public List<SourceRecord> mapRecordList(List<EhrStatusEvent> sourceList, String topic) {
        List<SourceRecord> resultList = new ArrayList<>();
        for (EhrStatusEvent ce : sourceList) {
            resultList.add(mapRecord(ce, topic));
        }
        return resultList;
    }

    public Map<String, ?> mapRecordPartition(EventLogOffset offset) {
        return new LinkedHashMap<>() {{
            put("t", "ehr_status");
            put("p", String.valueOf(offset.getPartition()));
        }};
    }

    public Map<String, ?> mapRecordOffset(EventLogOffset offset) {
        return !offset.isEmpty() ? new LinkedHashMap<>() {{
            put("d", offset.getDate().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            put("u", offset.getUid().toString());
            put("v", offset.getVersion().toString());
        }} : null;
    }


    @Override
    public List<Map<String, ?>> getTablePartitions(OpenEHRSourceConnectorConfig taskConfig) {
        List<Map<String, ?>> partitions = new ArrayList<>();
        for (int i = 0; i < taskConfig.getTablePatitionSize(); i++) {
            if (i % taskConfig.getTaskMax() == taskConfig.getTaskId()) {
                partitions.add(mapRecordPartition(new EventLogOffset(i)));
            }
        }
        return partitions;
    }

    @Override
    public String mapHashPartition(Map<String, ?> partition) {
        return String.format("%s-%s", partition.get("t"), partition.get("p"));
    }

    @Override
    public EventLogOffset mapOffset(Map<String, ?> partition, Map<String, ?> offset) {
        return new EventLogOffset(
                Integer.valueOf(partition.get("p").toString()),
                offset.get("d") != null ? ZonedDateTime.parse(offset.get("d").toString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME) : null,
                offset.get("u") != null ? UUID.fromString((String) offset.get("u")) : null,
                offset.get("v") != null ? Integer.valueOf((String) offset.get("v")) : null
        );

    }
}
