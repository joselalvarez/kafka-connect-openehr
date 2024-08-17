package com.github.joselalvarez.openehr.connect.common;

import lombok.Getter;

import java.util.Map;
import java.util.function.Supplier;

@Getter
public class TaskConfig<C extends ConnectorConfig> {

    public static final String SHARED_CONTEXT_ID = "task.shared.context.id";
    public static final String TASK_ID = "task.id";
    public static final String TASK_NAME = "task.name";

    private String sharedContextId;
    private int taskId;
    private String taskName;

    private C connectorConfig;

    public void load(Map<String, String> map, Supplier<C> connectorConfigFactory) {
        connectorConfig = connectorConfigFactory.get();
        connectorConfig.load(map);
        sharedContextId = map.getOrDefault(SHARED_CONTEXT_ID, "default");
        taskId = Integer.valueOf(map.getOrDefault(TASK_ID, "0"));
        taskName = map.getOrDefault(TASK_NAME, "unknown");
    }

}
