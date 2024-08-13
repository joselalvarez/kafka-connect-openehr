package com.github.joselalvarez.openehr.connect.source.record;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class CompositionEventRecord {

    public static final String CHANGE_TYPE_FIELD = "change_type";
    public static final String TIME_COMMITTED_FIELD = "time_committed";

    public static final String EHR_ID_FIELD = "ehr_id";
    public static final String UID_FIELD = "uid";
    public static final String VERSION_FIELD = "version";

    public static final String ARCHETYPE_ID_FIELD = "archetype_id";
    public static final String TEMPLATE_ID_FIELD = "template_id";
    public static final String COMPOSITION_ID_FIELD = "composition_id";
    public static final String REPLACED_ID_FIELD = "replaced_id";
    public static final String COMPOSITION_FIELD = "composition";

    public static final Schema SCHEMA;

    static {
        SCHEMA = SchemaBuilder.struct()
                        .name(CompositionEventRecord.class.getCanonicalName())
                        .field(CHANGE_TYPE_FIELD, Schema.STRING_SCHEMA)
                        .field(TIME_COMMITTED_FIELD, Schema.STRING_SCHEMA)
                        .field(EHR_ID_FIELD, Schema.STRING_SCHEMA)
                        .field(UID_FIELD, Schema.STRING_SCHEMA)
                        .field(VERSION_FIELD, Schema.INT32_SCHEMA)
                        .field(ARCHETYPE_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(TEMPLATE_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(COMPOSITION_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(REPLACED_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(COMPOSITION_FIELD, Schema.OPTIONAL_BYTES_SCHEMA)
                        .build();
    }

    private Struct delegate;

    public CompositionEventRecord() {
        this.delegate = new Struct(SCHEMA);
    }

    public CompositionEventRecord(Struct delegate) {
        this.delegate = delegate;
    }

    public Struct getDelegate() {
        return delegate;
    }

    public static boolean isSchema(Schema schema) {
        return schema != null && CompositionEventRecord.class.getCanonicalName().equals(schema.name());
    }

    public void setChangeType(String changeType) {
        delegate.put(CHANGE_TYPE_FIELD, changeType);
    }

    public String getChangeType() {
        return delegate.getString(CHANGE_TYPE_FIELD);
    }

    public void setTimeCommitted(String timeCommited) {
        delegate.put(TIME_COMMITTED_FIELD, timeCommited);
    }

    public String getTimeCommitted() {
        return delegate.getString(TIME_COMMITTED_FIELD);
    }

    public void setEhrId(String ehrId) {
        delegate.put(EHR_ID_FIELD, ehrId);
    }

    public String getEhrId() {
        return delegate.getString(EHR_ID_FIELD);
    }

    public void setUid(String ehrId) {
        delegate.put(UID_FIELD, ehrId);
    }

    public String getUid() {
        return delegate.getString(UID_FIELD);
    }

    public void setVersion(Integer version) {
        delegate.put(VERSION_FIELD, version);
    }

    public Integer getVersion() {
        return delegate.getInt32(VERSION_FIELD);
    }

    public void setArchetypeId(String archetypeId) {
        delegate.put(ARCHETYPE_ID_FIELD, archetypeId);
    }

    public String getArchetypeId() {
        return delegate.getString(ARCHETYPE_ID_FIELD);
    }

    public void setTemplateId(String templateId) {
        delegate.put(TEMPLATE_ID_FIELD, templateId);
    }

    public String getTemplateId() {
        return delegate.getString(TEMPLATE_ID_FIELD);
    }

    public void setCompositionId(String compositionId) {
        delegate.put(COMPOSITION_ID_FIELD, compositionId);
    }

    public String getCompositionId() {
        return delegate.getString(COMPOSITION_ID_FIELD);
    }

    public void setReplacedId(String replacedId) {
        delegate.put(REPLACED_ID_FIELD, replacedId);
    }

    public String getReplacedId() {
        return delegate.getString(REPLACED_ID_FIELD);
    }

    public void setComposition(byte[] composition) {
        delegate.put(COMPOSITION_FIELD, composition);
    }

    public byte [] getComposition() {
        return delegate.getBytes(COMPOSITION_FIELD);
    }

}
