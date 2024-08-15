package com.github.joselalvarez.openehr.connect.source.config;

import com.github.joselalvarez.openehr.connect.common.BaseConnectorConfig;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OpenEHRSourceConnectorConfig extends BaseConnectorConfig {

    public static final int TABLE_PATITION_SIZE = 16;

    public static final String KAFKA_COMPOSITION_TOPIC = "kafka.composition.topic";
    public static final String KAFKA_EHR_TOPIC = "kafka.ehr.topic";

    public static final String POLL_INTERVAL_MS = "poll.interval.ms";
    public static final long POLL_INTERVAL_MS_DEF = 10000;

    public static final String POLL_BATCH_SIZE = "poll.batch.size";
    public static final long POLL_BATCH_SIZE_DEF = 1000;

    public static final String COMPOSITION_GROUP_FILTERS = "Composition filters";
    public static final String FILTER_COMPOSITION_FROM_DATE = "filter.composition.from.date";
    public static final String FILTER_COMPOSITION_TO_DATE = "filter.composition.to.date";
    public static final String FILTER_COMPOSITION_BY_ARCHETYPE = "filter.composition.by.archetype";
    public static final String FILTER_COMPOSITION_BY_TEMPLATE = "filter.composition.by.template";

    public static final String HIKARICP_DOC = "https://github.com/brettwooldridge/HikariCP?tab=readme-ov-file#gear-configuration-knobs-baby";
    public static final String GROUP_JDBC = "ERHBase database connection";
    public static final String GROUP_JDBC_EXTRA = "ERHBase database extra configurations";
    public static final String JDBC_PREFIX = "database";
    public static final String JDBC_URL = String.format("%s.%s", JDBC_PREFIX, "jdbcUrl");
    public static final String JDBC_USERNAME = String.format("%s.%s", JDBC_PREFIX, "username");
    public static final String JDBC_PASSWORD = String.format("%s.%s", JDBC_PREFIX, "password");
    public static final String JDBC_SCHEMA = String.format("%s.%s", JDBC_PREFIX, "schema");
    public static final String JDBC_POOL_NAME = String.format("%s.%s", JDBC_PREFIX, "poolName");
    public static final String JDBC_AUTO_COMMIT = String.format("%s.%s", JDBC_PREFIX, "autoCommit");
    public static final String JDBC_CONNECTION_TIMEOUT = String.format("%s.%s", JDBC_PREFIX, "connectionTimeout");
    public static final String JDBC_IDLE_TIMEOUT = String.format("%s.%s", JDBC_PREFIX, "idleTimeout");
    public static final String JDBC_KEEP_ALIVE_TIME= String.format("%s.%s", JDBC_PREFIX, "keepaliveTime");
    public static final String JDBC_MAX_LIFE_TIME = String.format("%s.%s", JDBC_PREFIX, "maxLifetime");
    public static final String JDBC_CONNECTION_TEST_QUERY = String.format("%s.%s", JDBC_PREFIX, "connectionTestQuery");
    public static final String JDBC_MINIMUM_IDLE = String.format("%s.%s", JDBC_PREFIX, "minimumIdle");
    public static final String JDBC_MAXIMUM_POOL_SIZE = String.format("%s.%s", JDBC_PREFIX, "maximumPoolSize");
    public static final String JDBC_METRIC_REGISTRY = String.format("%s.%s", JDBC_PREFIX, "metricRegistry");
    public static final String JDBC_HEALTH_CHECK_REGISTRY = String.format("%s.%s", JDBC_PREFIX, "healthCheckRegistry");
    public static final String JDBC_INITIALIZATION_FAIL_TIMEOUT = String.format("%s.%s", JDBC_PREFIX, "initializationFailTimeout");
    public static final String JDBC_ISOLATE_INTERNAL_QUERIES = String.format("%s.%s", JDBC_PREFIX, "isolateInternalQueries");
    public static final String JDBC_ALLOW_POOL_SUSPENSION = String.format("%s.%s", JDBC_PREFIX, "allowPoolSuspension");
    public static final String JDBC_READ_ONLY = String.format("%s.%s", JDBC_PREFIX, "readOnly");
    public static final String JDBC_REGISTER_MBEANS = String.format("%s.%s", JDBC_PREFIX, "registerMbeans");
    public static final String JDBC_CATALOG = String.format("%s.%s", JDBC_PREFIX, "catalog");
    public static final String JDBC_CONNECTION_INIT_SQL = String.format("%s.%s", JDBC_PREFIX, "connectionInitSql");
    public static final String JDBC_TRANSACTION_ISOLATION = String.format("%s.%s", JDBC_PREFIX, "transactionIsolation");
    public static final String JDBC_VALIDATION_TIMEOUT = String.format("%s.%s", JDBC_PREFIX, "validationTimeout");
    public static final String JDBC_LEAK_DETECTION_THRESHOLD = String.format("%s.%s", JDBC_PREFIX, "leakDetectionThreshold");

    public static class ArchetypeValidator implements ConfigDef.Validator {

        @Override
        public void ensureValid(String name, Object value) {
            if (value != null && StringUtils.isNotBlank(value.toString())) {
                try {
                    new ArchetypeID(value.toString());
                } catch (Exception e) {
                    throw new ConfigException(name, value.toString(), "entry must be a valid archetype human readable id");
                }
            }
        }
    }

    public static ConfigDef getDefinitions() {
        ConfigDef def = BaseConnectorConfig.getDefinitions();
        DateTimeValidator dateTimeValidator = new DateTimeValidator();
        ArchetypeValidator archetypeValidator = new ArchetypeValidator();

        def.define(KAFKA_COMPOSITION_TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "TODO");
        def.define(KAFKA_EHR_TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "TODO");
        def.define(POLL_INTERVAL_MS, ConfigDef.Type.LONG, POLL_INTERVAL_MS_DEF, ConfigDef.Importance.MEDIUM, "TODO");
        def.define(POLL_BATCH_SIZE, ConfigDef.Type.LONG, POLL_BATCH_SIZE_DEF, ConfigDef.Importance.MEDIUM, "TODO");

        def.define(FILTER_COMPOSITION_FROM_DATE, ConfigDef.Type.STRING, null, dateTimeValidator, ConfigDef.Importance.LOW, "TODO", COMPOSITION_GROUP_FILTERS, -1, ConfigDef.Width.NONE, FILTER_COMPOSITION_FROM_DATE);
        def.define(FILTER_COMPOSITION_TO_DATE, ConfigDef.Type.STRING, null, dateTimeValidator, ConfigDef.Importance.LOW, "TODO", COMPOSITION_GROUP_FILTERS, -1, ConfigDef.Width.NONE, FILTER_COMPOSITION_TO_DATE);
        def.define(FILTER_COMPOSITION_BY_TEMPLATE, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, "TODO", COMPOSITION_GROUP_FILTERS, -1, ConfigDef.Width.NONE, FILTER_COMPOSITION_BY_TEMPLATE);
        def.define(FILTER_COMPOSITION_BY_ARCHETYPE, ConfigDef.Type.STRING, null, archetypeValidator, ConfigDef.Importance.LOW, "TODO", COMPOSITION_GROUP_FILTERS, -1, ConfigDef.Width.NONE, FILTER_COMPOSITION_BY_ARCHETYPE);

        def.define(JDBC_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, (ConfigDef.Validator) null, ConfigDef.Importance.HIGH, HIKARICP_DOC, GROUP_JDBC, -1, ConfigDef.Width.NONE, JDBC_URL);
        def.define(JDBC_USERNAME, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, (ConfigDef.Validator) null, ConfigDef.Importance.HIGH, HIKARICP_DOC, GROUP_JDBC, -1, ConfigDef.Width.NONE, JDBC_USERNAME);
        def.define(JDBC_PASSWORD, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, (ConfigDef.Validator) null, ConfigDef.Importance.HIGH, HIKARICP_DOC, GROUP_JDBC, -1, ConfigDef.Width.NONE, JDBC_PASSWORD);
        def.define(JDBC_SCHEMA, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.MEDIUM, HIKARICP_DOC, GROUP_JDBC, -1, ConfigDef.Width.NONE, JDBC_SCHEMA);
        def.define(JDBC_POOL_NAME, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_POOL_NAME);
        def.define(JDBC_AUTO_COMMIT, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_AUTO_COMMIT);
        def.define(JDBC_CONNECTION_TIMEOUT, ConfigDef.Type.LONG, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_CONNECTION_TIMEOUT);
        def.define(JDBC_IDLE_TIMEOUT, ConfigDef.Type.LONG, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_IDLE_TIMEOUT);
        def.define(JDBC_KEEP_ALIVE_TIME, ConfigDef.Type.LONG, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_KEEP_ALIVE_TIME);
        def.define(JDBC_MAX_LIFE_TIME, ConfigDef.Type.LONG, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_MAX_LIFE_TIME);
        def.define(JDBC_CONNECTION_TEST_QUERY, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_CONNECTION_TEST_QUERY);
        def.define(JDBC_MINIMUM_IDLE, ConfigDef.Type.LONG, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_MINIMUM_IDLE);
        def.define(JDBC_MAXIMUM_POOL_SIZE, ConfigDef.Type.LONG, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_MAXIMUM_POOL_SIZE);
        def.define(JDBC_METRIC_REGISTRY, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_METRIC_REGISTRY);
        def.define(JDBC_HEALTH_CHECK_REGISTRY, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_HEALTH_CHECK_REGISTRY);
        def.define(JDBC_INITIALIZATION_FAIL_TIMEOUT, ConfigDef.Type.LONG, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_INITIALIZATION_FAIL_TIMEOUT);
        def.define(JDBC_ISOLATE_INTERNAL_QUERIES, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_ISOLATE_INTERNAL_QUERIES);
        def.define(JDBC_ALLOW_POOL_SUSPENSION, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_ALLOW_POOL_SUSPENSION);
        def.define(JDBC_READ_ONLY, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_READ_ONLY);
        def.define(JDBC_REGISTER_MBEANS, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_REGISTER_MBEANS);
        def.define(JDBC_CATALOG, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_CATALOG);
        def.define(JDBC_CONNECTION_INIT_SQL, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_CONNECTION_INIT_SQL);
        def.define(JDBC_TRANSACTION_ISOLATION, ConfigDef.Type.STRING, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_TRANSACTION_ISOLATION);
        def.define(JDBC_VALIDATION_TIMEOUT, ConfigDef.Type.LONG, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_VALIDATION_TIMEOUT);
        def.define(JDBC_LEAK_DETECTION_THRESHOLD, ConfigDef.Type.LONG, null, (ConfigDef.Validator) null, ConfigDef.Importance.LOW, HIKARICP_DOC, GROUP_JDBC_EXTRA, -1, ConfigDef.Width.NONE, JDBC_LEAK_DETECTION_THRESHOLD);

        return def;
    }

    public OpenEHRSourceConnectorConfig(Map<String, String> configMap) {
        super(getDefinitions(), configMap);
    }

    public int getTablePatitionSize() {
        return TABLE_PATITION_SIZE;
    }

    public String getCompositionTopic() {
        String value = getString(KAFKA_COMPOSITION_TOPIC);
        return StringUtils.isNotBlank(value) ? value: null;
    }

    public String getEhrTopic() {
        String value = getString(KAFKA_EHR_TOPIC);
        return StringUtils.isNotBlank(value) ? value: null;
    }

    public long getPollBatchSize() {
        return getLong(POLL_BATCH_SIZE);
    }

    public long getPollIntervalMs() {
        return getLong(POLL_INTERVAL_MS);
    }

    public ZonedDateTime getFilterCompositionFromDate() {
        String value = getString(FILTER_COMPOSITION_FROM_DATE);
        return StringUtils.isNotBlank(value) ? ZonedDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME) : null;
    }

    public ZonedDateTime getFilterCompositionToDate() {
        String value = getString(FILTER_COMPOSITION_TO_DATE);
        return StringUtils.isNotBlank(value) ? ZonedDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME) : null;
    }

    public String getFilterCompositionByRootConcept() {
        String archetype = getString(FILTER_COMPOSITION_BY_ARCHETYPE);
        if (StringUtils.isNotBlank(archetype)) {
            ArchetypeID id = new ArchetypeID(archetype);
            String concept = id.getDomainConcept();
            String version = id.getVersionId();
            if (version != null) {
                return String.format(".%s.v%s", concept, version);
            }
            return String.format(".%s", concept);
        }
        return null;
    }

    public String getFilterCompositionByTemplateId() {
        String templateId = getString(FILTER_COMPOSITION_BY_TEMPLATE);
        return StringUtils.isNotBlank(templateId) ? templateId : null;
    }

    public Properties getJdbcProperties() {
        Properties jdbcProperties = new Properties();
        jdbcProperties.putAll(originalsWithPrefix(String.format("%s.", JDBC_PREFIX), true));
        return jdbcProperties;
    }

    @Override
    public int getTaskMax() {
        int max = super.getTaskMax();
        return max <= TABLE_PATITION_SIZE ? max : TABLE_PATITION_SIZE;
    }

    public Map<String, String> getTaskConfig(String sharedContextId, int taskId) {
        Map<String, String> taskMap = new HashMap<>();
        taskMap.put(BaseConnectorConfig.SHARED_CONTEXT_ID, sharedContextId);
        taskMap.put(BaseConnectorConfig.TASK_ID, String.valueOf(taskId));
        taskMap.putAll(originalsStrings());
        return taskMap;
    }

    public void validate() throws ConfigException {
        if (StringUtils.isBlank(getCompositionTopic()) && StringUtils.isBlank(getEhrTopic())) {
            throw new ConfigException(String.format("Connector[name=%s]: At least one of the topics [%s, %s] must be defined", getConnectorName(), KAFKA_COMPOSITION_TOPIC, KAFKA_EHR_TOPIC));
        }
    }

}
