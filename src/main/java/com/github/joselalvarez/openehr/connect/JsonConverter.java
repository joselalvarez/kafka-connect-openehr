package com.github.joselalvarez.openehr.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.message.CompositionChangeRecord;
import com.github.joselalvarez.openehr.connect.source.message.EhrStatusChangeRecord;
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
            if (CompositionChangeRecord.isSchema(schema)) {

                    CompositionChangeRecord message = new CompositionChangeRecord((Struct) o);

                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put(CompositionChangeRecord.TYPE, CompositionChangeRecord.TYPE_NAME);
                    map.put(CompositionChangeRecord.CHANGE_TYPE_FIELD, message.getChangeType());
                    map.put(CompositionChangeRecord.TIME_COMMITTED_FIELD, message.getTimeCommitted());
                    map.put(CompositionChangeRecord.EHR_ID_FIELD, message.getEhrId());
                    map.put(CompositionChangeRecord.UID_FIELD, message.getUid());
                    map.put(CompositionChangeRecord.VERSION_FIELD, message.getVersion());
                    map.put(CompositionChangeRecord.ARCHETYPE_ID_FIELD, message.getArchetypeId());
                    map.put(CompositionChangeRecord.TEMPLATE_ID_FIELD, message.getTemplateId());
                    map.put(CompositionChangeRecord.COMPOSITION_ID_FIELD, message.getCompositionId());
                    map.put(CompositionChangeRecord.REPLACED_ID_FIELD, message.getReplacedId());

                    if (message.getComposition() != null && message.getComposition().length > 0) {
                        JsonNode composition = objectMapper.readValue(message.getComposition(), JsonNode.class);
                        map.put(CompositionChangeRecord.COMPOSITION_FIELD, composition);
                    } else {
                        map.put(CompositionChangeRecord.COMPOSITION_FIELD, null);
                    }

                    return objectMapper.writeValueAsBytes(map);

            } else if (EhrStatusChangeRecord.isSchema(schema)) {
                EhrStatusChangeRecord message = new EhrStatusChangeRecord((Struct) o);

                Map<String, Object> map = new LinkedHashMap<>();
                map.put(EhrStatusChangeRecord.TYPE, EhrStatusChangeRecord.TYPE_NAME);
                map.put(EhrStatusChangeRecord.CHANGE_TYPE_FIELD, message.getChangeType());
                map.put(EhrStatusChangeRecord.TIME_COMMITTED_FIELD, message.getTimeCommitted());
                map.put(EhrStatusChangeRecord.EHR_ID_FIELD, message.getEhrId());
                map.put(EhrStatusChangeRecord.UID_FIELD, message.getUid());
                map.put(EhrStatusChangeRecord.VERSION_FIELD, message.getVersion());
                map.put(EhrStatusChangeRecord.ARCHETYPE_ID_FIELD, message.getArchetypeId());
                map.put(EhrStatusChangeRecord.EHR_STATUS_ID_FIELD, message.getEhrStatusId());
                map.put(EhrStatusChangeRecord.REPLACED_ID_FIELD, message.getReplacedId());

                map.put(EhrStatusChangeRecord.SUBJECT_TYPE_FIELD, message.getSubjectType());
                map.put(EhrStatusChangeRecord.SUBJECT_NAMESPACE_FIELD, message.getSubjectNamespace());
                map.put(EhrStatusChangeRecord.SUBJECT_ID_FIELD, message.getSubjectId());
                map.put(EhrStatusChangeRecord.SUBJECT_ID_SCHEME_FIELD, message.getSubjectIdScheme());

                if (message.getEhrStatus() != null && message.getEhrStatus().length > 0) {
                    JsonNode ehrStatus = objectMapper.readValue(message.getEhrStatus(), JsonNode.class);
                    map.put(EhrStatusChangeRecord.EHR_STATUS_FIELD, ehrStatus);
                } else {
                    map.put(EhrStatusChangeRecord.EHR_STATUS_FIELD, null);
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
