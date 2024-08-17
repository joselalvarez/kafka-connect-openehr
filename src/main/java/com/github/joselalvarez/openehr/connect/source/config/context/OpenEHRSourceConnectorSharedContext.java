package com.github.joselalvarez.openehr.connect.source.config.context;

import com.github.joselalvarez.openehr.connect.common.TaskConfig;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceTaskConfig;

import java.util.HashMap;
import java.util.Map;

public class OpenEHRSourceConnectorSharedContext {

    private static Map<String, EHRBaseSourceConnectorContext> contextMap = new HashMap<>();

    public synchronized static OpenEHRSourceConnectorContext start(OpenEHRSourceTaskConfig taskConfig) {
        String sharedContextId = taskConfig.getSharedContextId();
        if (!contextMap.containsKey(sharedContextId)) {
            contextMap.put(sharedContextId, new EHRBaseSourceConnectorContext(taskConfig.getConnectorConfig()));
        }
        return (EHRBaseSourceConnectorContext) contextMap.get(sharedContextId).retain();
    }

    public synchronized static void stop(TaskConfig<OpenEHRSourceConnectorConfig> taskConfig) {
        contextMap.remove(taskConfig.getSharedContextId());
    }

}
