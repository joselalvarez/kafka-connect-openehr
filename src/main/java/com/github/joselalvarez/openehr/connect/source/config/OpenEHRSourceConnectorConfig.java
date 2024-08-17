package com.github.joselalvarez.openehr.connect.source.config;

import com.github.joselalvarez.openehr.connect.common.ConnectorConfig;
import com.github.joselalvarez.openehr.connect.common.ConnectorConfigValidators;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Properties;

@Getter
public class OpenEHRSourceConnectorConfig extends ConnectorConfig {

    //General
    public static final int TABLE_PATITION_SIZE = 16;
    public static final String KAFKA_COMPOSITION_TOPIC = "kafka.composition.topic";
    public static final String KAFKA_EHR_TOPIC = "kafka.ehr.status.topic";
    public static final String POLL_INTERVAL_MS = "poll.interval.ms";
    public static final long POLL_INTERVAL_MS_DEF = 10000;
    public static final String POLL_BATCH_SIZE = "poll.batch.size";
    public static final long POLL_BATCH_SIZE_DEF = 1000;

    // Composition filters
    public static final String COMPOSITION_FILTER_GROUP = "Composition filters";
    public static final String FILTER_COMPOSITION_FROM_DATE = "filter.composition.from.date";
    public static final String FILTER_COMPOSITION_TO_DATE = "filter.composition.to.date";
    public static final String FILTER_COMPOSITION_BY_ARCHETYPE = "filter.composition.by.archetype";
    public static final String FILTER_COMPOSITION_BY_TEMPLATE = "filter.composition.by.template";

    // Ehr status filters
    public static final String ERH_STATUS_FILTER_GROUP = "Ehr status filters";
    public static final String FILTER_ERH_STATUS_FROM_DATE = "filter.ehr.status.from.date";
    public static final String FILTER_ERH_STATUS_TO_DATE = "filter.ehr.status.to.date";
    public static final String FILTER_ERH_STATUS_BY_NAMESPACE = "filter.ehr.status.by.namespace";

    // EHRBase database connection
    public static final String DATABASE_GROUP = "EHRBase database connection";
    public static final String HIKARICP_DOC = "https://github.com/brettwooldridge/HikariCP?tab=readme-ov-file#gear-configuration-knobs-baby";
    public static final String DATABASE_CONFIG_PREFIX = "database";
    public static final String HIKARI_URL = "jdbcUrl";
    public static final String JDBC_URL = "jdbc.url";
    public static final String DATABASE_URL = String.format("%s.%s", DATABASE_CONFIG_PREFIX, JDBC_URL);
    public static final String DATABASE_USERNAME = String.format("%s.%s", DATABASE_CONFIG_PREFIX, "username");
    public static final String DATABASE_PASSWORD = String.format("%s.%s", DATABASE_CONFIG_PREFIX, "password");
    public static final String DATABASE_SCHEMA = String.format("%s.%s", DATABASE_CONFIG_PREFIX, "schema");
    public static final String READ_ONLY = "readOnly";

    private int tablePartitionSize = TABLE_PATITION_SIZE;
    private String compositionTopic;
    private String ehrTopic;
    private long pollIntervalMs;
    private long pollBatchSize;

    private ZonedDateTime filterCompositionFromDate;
    private ZonedDateTime filterCompositionToDate;
    private String filterCompositionByTemplate;
    private String filterCompositionByArchetype;

    private ZonedDateTime filterEhrStatusFromDate;
    private ZonedDateTime filterEhrStatusToDate;
    private String filterEhrStatusByNamespace;

    private Properties jdbcProperties;

    @Override
    public ConfigDef definitions() {
        ConfigDef def = super.definitions();
        ConnectorConfigValidators.IsoOffsetDateTimeValidator dateTimeValidator = new ConnectorConfigValidators.IsoOffsetDateTimeValidator();
        ConnectorConfigValidators.ArchetypeValidator archetypeValidator = new ConnectorConfigValidators.ArchetypeValidator();

        def.define(KAFKA_COMPOSITION_TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "TODO");
        def.define(KAFKA_EHR_TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "TODO");
        def.define(POLL_INTERVAL_MS, ConfigDef.Type.LONG, POLL_INTERVAL_MS_DEF, ConfigDef.Importance.MEDIUM, "TODO");
        def.define(POLL_BATCH_SIZE, ConfigDef.Type.LONG, POLL_BATCH_SIZE_DEF, ConfigDef.Importance.MEDIUM, "TODO");

        def.define(FILTER_COMPOSITION_FROM_DATE, ConfigDef.Type.STRING, null, dateTimeValidator, ConfigDef.Importance.LOW, "TODO", COMPOSITION_FILTER_GROUP, -1, ConfigDef.Width.NONE, FILTER_COMPOSITION_FROM_DATE);
        def.define(FILTER_COMPOSITION_TO_DATE, ConfigDef.Type.STRING, null, dateTimeValidator, ConfigDef.Importance.LOW, "TODO", COMPOSITION_FILTER_GROUP, -1, ConfigDef.Width.NONE, FILTER_COMPOSITION_TO_DATE);
        def.define(FILTER_COMPOSITION_BY_TEMPLATE, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, "TODO", COMPOSITION_FILTER_GROUP, -1, ConfigDef.Width.NONE, FILTER_COMPOSITION_BY_TEMPLATE);
        def.define(FILTER_COMPOSITION_BY_ARCHETYPE, ConfigDef.Type.STRING, null, archetypeValidator, ConfigDef.Importance.LOW, "TODO", COMPOSITION_FILTER_GROUP, -1, ConfigDef.Width.NONE, FILTER_COMPOSITION_BY_ARCHETYPE);

        def.define(FILTER_ERH_STATUS_FROM_DATE, ConfigDef.Type.STRING, null, dateTimeValidator, ConfigDef.Importance.LOW, "TODO", ERH_STATUS_FILTER_GROUP, -1, ConfigDef.Width.NONE, FILTER_ERH_STATUS_FROM_DATE);
        def.define(FILTER_ERH_STATUS_TO_DATE, ConfigDef.Type.STRING, null, dateTimeValidator, ConfigDef.Importance.LOW, "TODO", ERH_STATUS_FILTER_GROUP, -1, ConfigDef.Width.NONE, FILTER_ERH_STATUS_TO_DATE);
        def.define(FILTER_ERH_STATUS_BY_NAMESPACE, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, "TODO", ERH_STATUS_FILTER_GROUP, -1, ConfigDef.Width.NONE, FILTER_ERH_STATUS_BY_NAMESPACE);

        def.define(DATABASE_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, (ConfigDef.Validator) null, ConfigDef.Importance.HIGH, HIKARICP_DOC, DATABASE_GROUP, -1, ConfigDef.Width.NONE, DATABASE_URL);
        def.define(DATABASE_USERNAME, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, (ConfigDef.Validator) null, ConfigDef.Importance.HIGH, HIKARICP_DOC, DATABASE_GROUP, -1, ConfigDef.Width.NONE, DATABASE_USERNAME);
        def.define(DATABASE_PASSWORD, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, (ConfigDef.Validator) null, ConfigDef.Importance.HIGH, HIKARICP_DOC, DATABASE_GROUP, -1, ConfigDef.Width.NONE, DATABASE_PASSWORD);
        def.define(DATABASE_SCHEMA, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.MEDIUM, HIKARICP_DOC, DATABASE_GROUP, -1, ConfigDef.Width.NONE, DATABASE_SCHEMA);

        return def;
    }

    public String rootConcept(String value) {
        if (StringUtils.isNotBlank(value)) {
            ArchetypeID id = new ArchetypeID(value);
            String concept = id.getDomainConcept();
            String version = id.getVersionId();
            if (version != null) {
                return String.format(".%s.v%s", concept, version);
            }
            return String.format(".%s", concept);
        }
        return null;
    }


    @Override
    public void load(Map<String, String> map) {

        super.load(map);

        compositionTopic = clean(getConfig().getString(KAFKA_COMPOSITION_TOPIC));
        ehrTopic = clean(getConfig().getString(KAFKA_EHR_TOPIC));
        pollIntervalMs = getConfig().getLong(POLL_INTERVAL_MS);
        pollBatchSize = getConfig().getLong(POLL_BATCH_SIZE);

        filterCompositionFromDate = date(getConfig().getString(FILTER_COMPOSITION_FROM_DATE));
        filterCompositionToDate = date(getConfig().getString(FILTER_COMPOSITION_TO_DATE));
        filterCompositionByTemplate = clean(getConfig().getString(FILTER_COMPOSITION_BY_TEMPLATE));
        filterCompositionByArchetype = rootConcept(getConfig().getString(FILTER_COMPOSITION_BY_ARCHETYPE));

        filterEhrStatusFromDate = date(getConfig().getString(FILTER_ERH_STATUS_FROM_DATE));
        filterEhrStatusToDate = date(getConfig().getString(FILTER_ERH_STATUS_TO_DATE));
        filterEhrStatusByNamespace = clean(getConfig().getString(FILTER_ERH_STATUS_BY_NAMESPACE));

        String url = clean(getConfig().getString(DATABASE_URL));
        Map<String, String> databaseMap = getOriginalMapWithPrefix(String.format("%s.", DATABASE_CONFIG_PREFIX));
        databaseMap.remove(JDBC_URL);
        databaseMap.put(HIKARI_URL, url);
        databaseMap.put(READ_ONLY, "true");
        jdbcProperties = new Properties();
        jdbcProperties.putAll(databaseMap);
    }

    public void validate() throws ConfigException {
        if (compositionTopic == null && ehrTopic == null) {
            throw new ConfigException(String.format("Connector[name=%s]: At least one of the topics [%s, %s] must be defined", getConnectorName(), KAFKA_COMPOSITION_TOPIC, KAFKA_EHR_TOPIC));
        }
    }
}
