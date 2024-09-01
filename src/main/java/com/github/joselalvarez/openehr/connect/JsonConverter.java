package com.github.joselalvarez.openehr.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.exception.OpenEHRSourceConnectException;
import com.github.joselalvarez.openehr.connect.source.message.CompositionChangeStruct;
import com.github.joselalvarez.openehr.connect.source.message.EhrStatusChangeStruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class JsonConverter implements Converter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private byte [] convert(CompositionChangeStruct source) throws IOException {
        Map<String, Object> map = new LinkedHashMap<>();

        map.put(CompositionChangeStruct.TYPE, CompositionChangeStruct.TYPE_NAME);
        map.put(CompositionChangeStruct.CHANGE_TYPE_FIELD, source.getChangeType());
        map.put(CompositionChangeStruct.TIME_COMMITTED_FIELD, source.getTimeCommitted());
        map.put(CompositionChangeStruct.EHR_ID_FIELD, source.getEhrId());
        map.put(CompositionChangeStruct.UID_FIELD, source.getUid());
        map.put(CompositionChangeStruct.VERSION_FIELD, source.getVersion());
        map.put(CompositionChangeStruct.ARCHETYPE_ID_FIELD, source.getArchetypeId());
        map.put(CompositionChangeStruct.TEMPLATE_ID_FIELD, source.getTemplateId());
        map.put(CompositionChangeStruct.COMPOSITION_ID_FIELD, source.getCompositionId());
        map.put(CompositionChangeStruct.REPLACED_ID_FIELD, source.getReplacedId());

        if (source.getComposition() != null && source.getComposition().length > 0) {
            JsonNode composition = objectMapper.readValue(source.getComposition(), JsonNode.class);
            map.put(CompositionChangeStruct.COMPOSITION_FIELD, composition);
        } else {
            map.put(CompositionChangeStruct.COMPOSITION_FIELD, null);
        }

        return objectMapper.writeValueAsBytes(map);
    }

    private byte [] convert(EhrStatusChangeStruct source) throws IOException {
        Map<String, Object> map = new LinkedHashMap<>();

        map.put(EhrStatusChangeStruct.TYPE, EhrStatusChangeStruct.TYPE_NAME);
        map.put(EhrStatusChangeStruct.CHANGE_TYPE_FIELD, source.getChangeType());
        map.put(EhrStatusChangeStruct.TIME_COMMITTED_FIELD, source.getTimeCommitted());
        map.put(EhrStatusChangeStruct.EHR_ID_FIELD, source.getEhrId());
        map.put(EhrStatusChangeStruct.UID_FIELD, source.getUid());
        map.put(EhrStatusChangeStruct.VERSION_FIELD, source.getVersion());
        map.put(EhrStatusChangeStruct.ARCHETYPE_ID_FIELD, source.getArchetypeId());
        map.put(EhrStatusChangeStruct.EHR_STATUS_ID_FIELD, source.getEhrStatusId());
        map.put(EhrStatusChangeStruct.REPLACED_ID_FIELD, source.getReplacedId());

        map.put(EhrStatusChangeStruct.SUBJECT_TYPE_FIELD, source.getSubjectType());
        map.put(EhrStatusChangeStruct.SUBJECT_NAMESPACE_FIELD, source.getSubjectNamespace());
        map.put(EhrStatusChangeStruct.SUBJECT_ID_FIELD, source.getSubjectId());
        map.put(EhrStatusChangeStruct.SUBJECT_ID_SCHEME_FIELD, source.getSubjectIdScheme());

        if (source.getEhrStatus() != null && source.getEhrStatus().length > 0) {
            JsonNode ehrStatus = objectMapper.readValue(source.getEhrStatus(), JsonNode.class);
            map.put(EhrStatusChangeStruct.EHR_STATUS_FIELD, ehrStatus);
        } else {
            map.put(EhrStatusChangeStruct.EHR_STATUS_FIELD, null);
        }

        return objectMapper.writeValueAsBytes(map);
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {}

    @Override
    public byte[] fromConnectData(String s, Schema schema, Object o) {

        try {
            if (CompositionChangeStruct.supports(schema)) {
                return convert(new CompositionChangeStruct((Struct) o));
            } else if (EhrStatusChangeStruct.supports(schema)) {
                return convert(new EhrStatusChangeStruct((Struct) o));
            }
        } catch (Exception e) {
            log.error("Serialization error: {}", e);
            throw OpenEHRSourceConnectException.serdeError(e);
        }

        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaAndValue toConnectData(String s, byte[] bytes) {
        throw new UnsupportedOperationException();
    }
}
