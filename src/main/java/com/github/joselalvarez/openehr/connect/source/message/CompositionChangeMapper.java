package com.github.joselalvarez.openehr.connect.source.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.exception.OpenEHRSourceConnectException;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionChange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.format.DateTimeFormatter;

@RequiredArgsConstructor
@Slf4j
public class CompositionChangeMapper {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ObjectMapper canonicalObjectMapper;
    private final OpenEHRSourceConnectorConfig connectorConfig;

    public Struct mapStruct(CompositionChange source) {
        CompositionChangeStruct target = new CompositionChangeStruct();

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
            throw OpenEHRSourceConnectException.serdeError(e);
        }
        return target.getDelegate();
    }

    public SourceRecord mapRecord(CompositionChange source) {
        return new SourceRecord(
                source.getPartitionOffset().getPartitionMap(),
                source.getPartitionOffset().getOffsetMap(),
                connectorConfig.getCompositionTopic(),
                null, // Topic partition (default by kafka)
                Schema.STRING_SCHEMA, // Message key schema
                source.getEhrId().toString(), // Message key
                CompositionChangeStruct.SCHEMA, // Value schema
                mapStruct(source) // Value
        );
    }

}
