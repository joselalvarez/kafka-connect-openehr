package com.github.joselalvarez.openehr.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.record.CompositionEventRecord;
import com.github.joselalvarez.openehr.connect.source.record.EhrStatusEventRecord;
import com.nedap.archie.rm.ehr.Ehr;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.Converter;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class JsonConverter implements Converter {

    public final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {}

    @Override
    public byte[] fromConnectData(String s, Schema schema, Object o) {

        try {
            if (CompositionEventRecord.isSchema(schema)) {

                    CompositionEventRecord message = new CompositionEventRecord((Struct) o);

                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put(CompositionEventRecord.TYPE, CompositionEventRecord.TYPE_VALUE);
                    map.put(CompositionEventRecord.CHANGE_TYPE_FIELD, message.getChangeType());
                    map.put(CompositionEventRecord.TIME_COMMITTED_FIELD, message.getTimeCommitted());
                    map.put(CompositionEventRecord.EHR_ID_FIELD, message.getEhrId());
                    map.put(CompositionEventRecord.UID_FIELD, message.getUid());
                    map.put(CompositionEventRecord.VERSION_FIELD, message.getVersion());
                    map.put(CompositionEventRecord.ARCHETYPE_ID_FIELD, message.getArchetypeId());
                    map.put(CompositionEventRecord.TEMPLATE_ID_FIELD, message.getTemplateId());
                    map.put(CompositionEventRecord.COMPOSITION_ID_FIELD, message.getCompositionId());
                    map.put(CompositionEventRecord.REPLACED_ID_FIELD, message.getReplacedId());

                    if (message.getComposition() != null && message.getComposition().length > 0) {
                        JsonNode composition = objectMapper.readValue(message.getComposition(), JsonNode.class);
                        map.put(CompositionEventRecord.COMPOSITION_FIELD, composition);
                    } else {
                        map.put(CompositionEventRecord.COMPOSITION_FIELD, null);
                    }

                    return objectMapper.writeValueAsBytes(map);

            } else if (EhrStatusEventRecord.isSchema(schema)) {
                EhrStatusEventRecord message = new EhrStatusEventRecord((Struct) o);

                Map<String, Object> map = new LinkedHashMap<>();
                map.put(EhrStatusEventRecord.TYPE, EhrStatusEventRecord.TYPE_VALUE);
                map.put(EhrStatusEventRecord.CHANGE_TYPE_FIELD, message.getChangeType());
                map.put(EhrStatusEventRecord.TIME_COMMITTED_FIELD, message.getTimeCommitted());
                map.put(EhrStatusEventRecord.EHR_ID_FIELD, message.getEhrId());
                map.put(EhrStatusEventRecord.UID_FIELD, message.getUid());
                map.put(EhrStatusEventRecord.VERSION_FIELD, message.getVersion());
                map.put(EhrStatusEventRecord.ARCHETYPE_ID_FIELD, message.getArchetypeId());
                map.put(EhrStatusEventRecord.EHR_STATUS_ID_FIELD, message.getEhrStatusId());
                map.put(EhrStatusEventRecord.REPLACED_ID_FIELD, message.getReplacedId());

                map.put(EhrStatusEventRecord.SUBJECT_TYPE_FIELD, message.getSubjectType());
                map.put(EhrStatusEventRecord.SUBJECT_NAMESPACE_FIELD, message.getSubjectNamespace());
                map.put(EhrStatusEventRecord.SUBJECT_ID_FIELD, message.getSubjectId());
                map.put(EhrStatusEventRecord.SUBJECT_ID_SCHEME_FIELD, message.getSubjectIdScheme());

                if (message.getEhrStatus() != null && message.getEhrStatus().length > 0) {
                    JsonNode ehrStatus = objectMapper.readValue(message.getEhrStatus(), JsonNode.class);
                    map.put(EhrStatusEventRecord.EHR_STATUS_FIELD, ehrStatus);
                } else {
                    map.put(EhrStatusEventRecord.EHR_STATUS_FIELD, null);
                }

                return objectMapper.writeValueAsBytes(map);
            }
        } catch (Exception e) {
            log.error("JsonConverter serialization error: {}", e);
            throw new ConnectException("JsonConverter serialization error", e);
        }

        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaAndValue toConnectData(String s, byte[] bytes) {
        throw new UnsupportedOperationException();
    }
}
