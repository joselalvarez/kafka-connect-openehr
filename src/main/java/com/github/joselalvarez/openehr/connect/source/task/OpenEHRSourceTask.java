package com.github.joselalvarez.openehr.connect.source.task;

import com.github.joselalvarez.openehr.connect.common.ConnectorInfo;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceTaskConfig;
import com.github.joselalvarez.openehr.connect.source.config.context.OpenEHRSourceConnectorContext;
import com.github.joselalvarez.openehr.connect.source.config.context.OpenEHRSourceConnectorSharedContext;
import com.github.joselalvarez.openehr.connect.source.message.CompositionChangeMapper;
import com.github.joselalvarez.openehr.connect.source.message.EhrStatusChangeMapper;
import com.github.joselalvarez.openehr.connect.source.service.OpenEHRChangeLogService;
import com.github.joselalvarez.openehr.connect.source.service.model.ChangeRequest;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionChangeRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class OpenEHRSourceTask extends PeriodicSourceTask {

    private OpenEHRSourceTaskConfig taskConfig = new OpenEHRSourceTaskConfig();
    private OpenEHRSourceConnectorConfig connectorConfig;
    private OpenEHRSourceConnectorContext connectorContext;
    private OpenEHRChangeLogService changeLogService;
    private CompositionChangeMapper compositionChangeRecordMapper;
    private EhrStatusChangeMapper ehrStatusChangeRecordMapper;
    private TaskPartitionOffsetStorage partitionOffsetStorage;


    @Override
    public String version() {
        return ConnectorInfo.CONNECTOR_VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        taskConfig.load(map, OpenEHRSourceConnectorConfig::new);
        connectorConfig = taskConfig.getConnectorConfig();
        connectorContext = OpenEHRSourceConnectorSharedContext.start(taskConfig);
        changeLogService = connectorContext.getOpenEHRChangeLogService();
        compositionChangeRecordMapper = connectorContext.getCompositionChangeMapper();
        ehrStatusChangeRecordMapper = connectorContext.getEhrStatusChangeMapper();
        partitionOffsetStorage = new TaskPartitionOffsetStorage(
                context.offsetStorageReader(),
                taskConfig,
                connectorContext.getPartitionOffsetFactory()
        );
        super.start(connectorConfig.getPollIntervalMs(), connectorContext::isClosed);
        log.info("Task[name={}]: online", taskConfig.getTaskName());
    }

    @Override
    public List<SourceRecord> periodicCompositionPoll() {

        CompositionChangeRequest request = CompositionChangeRequest.builder()
                .fromDate(connectorConfig.getFilterCompositionFromDate())
                .toDate(connectorConfig.getFilterCompositionToDate())
                .rootConcept(connectorConfig.getFilterCompositionByArchetype())
                .templateId(connectorConfig.getFilterCompositionByTemplate())
                .partitionOffsets(partitionOffsetStorage.getCompositionChangePartitionOffsets())
            .build();

        return changeLogService.getCompositionChanges(request)
                .map(compositionChangeRecordMapper::mapRecord)
                .map(partitionOffsetStorage::registerCompositionChangePartitionOffset)
            .collect(Collectors.toList());

    }

    @Override
    public List<SourceRecord> periodicEhrStatusPoll() {

        ChangeRequest request = ChangeRequest.builder()
                .fromDate(connectorConfig.getFilterEhrStatusFromDate())
                .toDate(connectorConfig.getFilterEhrStatusToDate())
                .maxPoll(connectorConfig.getPollBatchSize())
                .partitionOffsets(partitionOffsetStorage.getEhrStatusChangePartitionOffsets())
            .build();

        return changeLogService.getEhrStatusChanges(request)
                .map(ehrStatusChangeRecordMapper::mapRecord)
                .map(partitionOffsetStorage::registerEhrStatusChangePartitionOffset)
            .collect(Collectors.toList());
    }

    @Override
    public void stop() {
        connectorContext.close();
        OpenEHRSourceConnectorSharedContext.stop(taskConfig);
        log.info("Task[name={}]: stop", taskConfig.getTaskName());
    }

}
