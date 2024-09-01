package com.github.joselalvarez.openehr.connect.source.message;

import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusChange;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class EhrStatusChangeStruct {

    public static final String TYPE = "_type";
    public static final String TYPE_NAME = "EHR_STATUS_CHANGE";

    public static final String CHANGE_TYPE_FIELD = "change_type";
    public static final String TIME_COMMITTED_FIELD = "time_committed";

    public static final String EHR_ID_FIELD = "ehr_id";
    public static final String UID_FIELD = "uid";
    public static final String VERSION_FIELD = "version";

    public static final String ARCHETYPE_ID_FIELD = "archetype_id";
    public static final String EHR_STATUS_ID_FIELD = "ehr_status_id";
    public static final String REPLACED_ID_FIELD = "replaced_id";

    public static final String SUBJECT_TYPE_FIELD = "subject_type";
    public static final String SUBJECT_NAMESPACE_FIELD = "subject_namespace";
    public static final String SUBJECT_ID_FIELD = "subject_id";
    public static final String SUBJECT_ID_SCHEME_FIELD = "subject_id_scheme";

    public static final String EHR_STATUS_FIELD = "ehr_status";

    public static final Schema SCHEMA;

    static {
        SCHEMA = SchemaBuilder.struct()
                .name(EhrStatusChange.class.getCanonicalName())
                .field(CHANGE_TYPE_FIELD, Schema.STRING_SCHEMA)
                .field(TIME_COMMITTED_FIELD, Schema.STRING_SCHEMA)
                .field(EHR_ID_FIELD, Schema.STRING_SCHEMA)
                .field(UID_FIELD, Schema.STRING_SCHEMA)
                .field(VERSION_FIELD, Schema.INT32_SCHEMA)
                .field(ARCHETYPE_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(EHR_STATUS_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(REPLACED_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SUBJECT_TYPE_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SUBJECT_NAMESPACE_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SUBJECT_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SUBJECT_ID_SCHEME_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(EHR_STATUS_FIELD, Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
    }

    private Struct delegate;

    public EhrStatusChangeStruct() {
        this.delegate = new Struct(SCHEMA);
    }

    public EhrStatusChangeStruct(Struct delegate) {
        this.delegate = delegate;
    }

    public Struct getDelegate() {
        return delegate;
    }

    public static boolean supports(Schema schema) {
        return schema != null && SCHEMA.name().equals(schema.name());
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

    public void setEhrStatusId(String ehrStatusId) {
        delegate.put(EHR_STATUS_ID_FIELD, ehrStatusId);
    }

    public String getEhrStatusId() {
        return delegate.getString(EHR_STATUS_ID_FIELD);
    }

    public void setReplacedId(String replacedId) {
        delegate.put(REPLACED_ID_FIELD, replacedId);
    }

    public String getReplacedId() {
        return delegate.getString(REPLACED_ID_FIELD);
    }

    public void setSubjectType(String subjectType) {
        delegate.put(SUBJECT_TYPE_FIELD, subjectType);
    }

    public String getSubjectType() {
        return delegate.getString(SUBJECT_TYPE_FIELD);
    }

    public void setSubjectNamespace(String namespace) {
        delegate.put(SUBJECT_NAMESPACE_FIELD, namespace);
    }

    public String getSubjectNamespace() {
        return delegate.getString(SUBJECT_NAMESPACE_FIELD);
    }

    public void setSubjectId(String subjectId) {
        delegate.put(SUBJECT_ID_FIELD, subjectId);
    }

    public String getSubjectId() {
        return delegate.getString(SUBJECT_ID_FIELD);
    }

    public void setSubjectIdScheme(String subjectIdScheme) {
        delegate.put(SUBJECT_ID_SCHEME_FIELD, subjectIdScheme);
    }

    public String getSubjectIdScheme() {
        return delegate.getString(SUBJECT_ID_SCHEME_FIELD);
    }

    public void setEhrStatus(byte[] composition) {
        delegate.put(EHR_STATUS_FIELD, composition);
    }

    public byte [] getEhrStatus() {
        return delegate.getBytes(EHR_STATUS_FIELD);
    }

}
