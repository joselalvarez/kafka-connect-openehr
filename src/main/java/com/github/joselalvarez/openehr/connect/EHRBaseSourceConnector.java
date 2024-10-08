package com.github.joselalvarez.openehr.connect;

import com.github.joselalvarez.openehr.connect.common.ConnectorInfo;
import com.github.joselalvarez.openehr.connect.source.task.OpenEHRSourceTask;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class EHRBaseSourceConnector extends SourceConnector {

    private String connectorName;
    private OpenEHRSourceConnectorConfig connectorConfig = new OpenEHRSourceConnectorConfig();

    @Override
    public String version() {
        return ConnectorInfo.CONNECTOR_VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        log.info("EHRBase source connector starting ...");
        connectorConfig.load(map);
        connectorConfig.validate();
        connectorName = connectorConfig.getConnectorName();
        log.info("Connector[name={}]: configuration loaded", connectorName);
        log.info("Connector[name={}]: online", connectorName);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return OpenEHRSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int max) {

        log.info("Connector[name={}]: config task [max={}]", connectorName, max);
        int total = max;
        if (max > connectorConfig.getTablePartitionSize()) {
            log.warn("Connector[name={}]: The number of tasks '{}' exceeds the number of partitions '{}'", connectorName, max, connectorConfig.getTablePartitionSize());
            total = connectorConfig.getTablePartitionSize();
        }

        List<Map<String, String>> taskMapList = new ArrayList<>();
        String sharedContextId = UUID.randomUUID().toString();
        for (int i = 0; i < max; i++) {
            taskMapList.add(connectorConfig.createTaskConfig(sharedContextId, i, total));
        }

        log.info("Connector[name={}]: {} tasks configured", connectorName, total);
        return taskMapList;

    }

    @Override
    public void stop() {
        log.info("Connector[name={}]: stop", connectorName);
    }

    @Override
    public ConfigDef config() {
        return connectorConfig.definitions();
    }

}
