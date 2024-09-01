package com.github.joselalvarez.openehr.connect.source.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.exception.OpenEHRSourceConnectException;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusChange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.format.DateTimeFormatter;

@RequiredArgsConstructor
@Slf4j
public class EhrStatusChangeMapper {

    private final ObjectMapper canonicalObjectMapper;
    private final OpenEHRSourceConnectorConfig connectorConfig;

    public Struct mapStruct(EhrStatusChange source) {
        EhrStatusChangeStruct target = new EhrStatusChangeStruct();

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
            log.error("EhrStatus serialization error: {}", e);
            throw OpenEHRSourceConnectException.serdeError(e);
        }
        return target.getDelegate();
    }

    public SourceRecord mapRecord(EhrStatusChange source) {
        return new SourceRecord(
                source.getPartitionOffset().getPartitionMap(),
                source.getPartitionOffset().getOffsetMap(),
                connectorConfig.getEhrTopic(),
                null, // Topic partition (default by kafka)
                Schema.STRING_SCHEMA, // Message key schema
                source.getEhrId().toString(), // Message key
                EhrStatusChangeStruct.SCHEMA, // Value schema
                mapStruct(source) // Value
        );
    }

}
