package com.github.joselalvarez.openehr.connect;

import com.github.joselalvarez.openehr.connect.common.ConnectorInfo;
import com.github.joselalvarez.openehr.connect.source.OpenEHRSourceTask;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

@Slf4j
public class EHRBaseSourceConnector extends SourceConnector {

    private String connectorName;
    private OpenEHRSourceConnectorConfig connectorConfig;

    @Override
    public String version() {
        return ConnectorInfo.CONNECTOR_VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        log.info("EHRBase source connector starting ...");
        connectorConfig = new OpenEHRSourceConnectorConfig(map);
        connectorName = connectorConfig.getConnectorName();
        connectorConfig.validate();
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
        return connectorConfig.getTaskMapListConfig(max);
    }

    @Override
    public void stop() {
        log.info("Connector[name={}]: stop", connectorName);
    }

    @Override
    public ConfigDef config() {
        return OpenEHRSourceConnectorConfig.getDefinitions();
    }

}
