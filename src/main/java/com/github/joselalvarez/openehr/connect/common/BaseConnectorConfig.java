package com.github.joselalvarez.openehr.connect.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public abstract class BaseConnectorConfig extends AbstractConfig {

    public static class DateTimeValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value != null && StringUtils.isNotBlank(value.toString())) {
                try {
                    ZonedDateTime.parse(value.toString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                } catch (Exception e) {
                    throw new ConfigException(name, value.toString(), "entry must be a valid 'ISO_OFFSET_DATE_TIME'");
                }
            }
        }
    }

    public static final String CONNECTOR_NAME = "name";
    public static final String TASK_MAX = "tasks.max";
    public static final String SHARED_CONTEXT_ID = "shared.context.id";
    public static final String TASK_ID = "task.id";

    private String connectorName;
    private int taskMax;
    private String sharedContextId;
    private int taskId;

    public static ConfigDef getDefinitions() {
        return new ConfigDef();
    }

    public BaseConnectorConfig(ConfigDef definitions, Map<String, String> map) {
        super(definitions, map);
        this.connectorName = map.getOrDefault(CONNECTOR_NAME, "unknown");
        this.taskMax = Integer.valueOf(map.getOrDefault(TASK_MAX, "1"));
        this.sharedContextId = map.getOrDefault(SHARED_CONTEXT_ID, getConnectorName());
        this.taskId = Integer.valueOf(map.getOrDefault(TASK_ID, "-1"));
    }

    public String getConnectorName() {
        return connectorName;
    }

    public int getTaskMax() {
        return taskMax;
    }

    public String getSharedContextId() {
        return sharedContextId;
    }

    public int getTaskId() {
        return taskId;
    }

    public String getTaskName() {
        return String.format("%s-%s", getConnectorName(), getTaskId());
    }

}
