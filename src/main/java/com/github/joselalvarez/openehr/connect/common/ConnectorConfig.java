package com.github.joselalvarez.openehr.connect.common;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Getter
public class ConnectorConfig {

    public static final String CONNECTOR_NAME = "name";
    public static final String TASKS_MAX = "tasks.max";
    public static final String TASKS_CONFIGURED = "tasks.configured";

    private AbstractConfig config;
    private Map<String, String> originalMap = new HashMap<>();

    private String connectorName;
    private int tasksMax;
    private int tasksConfigured;

    protected String clean(String value) {
        return StringUtils.isNotBlank(value) ? value.trim() : null;
    }

    protected ZonedDateTime date(String value) {
        return StringUtils.isNotBlank(value) ? ZonedDateTime.parse(value.trim(), DateTimeFormatter.ISO_OFFSET_DATE_TIME) : null;
    }

    public ConfigDef definitions() {
        return new ConfigDef();
    }

    public void load(Map<String, String> map) {
        originalMap = Collections.unmodifiableMap(new LinkedHashMap<>(map));
        config = new AbstractConfig(definitions(), map);
        connectorName = map.getOrDefault(CONNECTOR_NAME, "unknown");
        tasksMax = Integer.valueOf(map.getOrDefault(TASKS_MAX, "1"));
        tasksConfigured = Integer.valueOf(map.getOrDefault(TASKS_CONFIGURED, "0"));
    }

    public Map<String, String> getOriginalMapWithPrefix(String prefix) {
        Map<String, String> map = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : originalMap.entrySet()) {
            if (entry.getKey() != null && entry.getKey().startsWith(prefix)) {
                map.put(entry.getKey().replaceFirst(prefix, ""), entry.getValue());
            }
        }
        return map;
    }

    public Map<String, String> createTaskConfig(String sharedContextId, int taskId, int total) {
        Map<String, String> map = new LinkedHashMap<>(originalMap);
        map.put(TaskConfig.SHARED_CONTEXT_ID, sharedContextId);
        map.put(TaskConfig.TASK_ID, String.valueOf(taskId));
        map.put(TaskConfig.TASK_NAME, String.format("%s-%s", connectorName, taskId));
        map.put(TASKS_CONFIGURED, String.valueOf(total));
        return map;
    }
}
