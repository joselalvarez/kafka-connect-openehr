package com.github.joselalvarez.openehr.connect.source.task;

import com.github.joselalvarez.openehr.connect.common.ConnectorInfo;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceTaskConfig;
import com.github.joselalvarez.openehr.connect.source.config.context.OpenEHRSourceConnectorContext;
import com.github.joselalvarez.openehr.connect.source.config.context.OpenEHRSourceConnectorSharedContext;
import com.github.joselalvarez.openehr.connect.source.message.CompositionChangeRecordMapper;
import com.github.joselalvarez.openehr.connect.source.message.EhrStatusChangeRecordMapper;
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
    private CompositionChangeRecordMapper compositionChangeRecordMapper;
    private EhrStatusChangeRecordMapper ehrStatusChangeRecordMapper;
    private TaskOffsetStorage taskOffsetStorage;


    @Override
    public String version() {
        return ConnectorInfo.CONNECTOR_VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        taskConfig.load(map, OpenEHRSourceConnectorConfig::new);
        connectorConfig = taskConfig.getConnectorConfig();
        connectorContext = OpenEHRSourceConnectorSharedContext.start(taskConfig);
        changeLogService = connectorContext.getOpenEHREventLogService();
        compositionChangeRecordMapper = connectorContext.getCompositionChangeRecordMapper();
        ehrStatusChangeRecordMapper = connectorContext.getEhrStatusChangeRecordMapper();
        taskOffsetStorage = new TaskOffsetStorage(
                context.offsetStorageReader(),
                taskConfig,
                connectorContext.getRecordOffsetFactory()
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
                .offsetList(taskOffsetStorage.getCompositionChangeOffsetList())
            .build();

        return changeLogService.getCompositionChanges(request)
                .map(compositionChangeRecordMapper::mapRecord)
                .map(taskOffsetStorage::registerCompositionChangeOffset)
            .collect(Collectors.toList());

    }

    @Override
    public List<SourceRecord> periodicEhrStatusPoll() {

        ChangeRequest request = ChangeRequest.builder()
                .fromDate(connectorConfig.getFilterEhrStatusFromDate())
                .toDate(connectorConfig.getFilterEhrStatusToDate())
                .maxPoll(connectorConfig.getPollBatchSize())
                .offsetList(taskOffsetStorage.getEhrStatusChangeOffsetList())
            .build();

        return changeLogService.getEhrStatusChanges(request)
                .map(ehrStatusChangeRecordMapper::mapRecord)
                .map(taskOffsetStorage::registerEhrStatusChangeOffset)
            .collect(Collectors.toList());
    }

    @Override
    public void stop() {
        connectorContext.close();
        OpenEHRSourceConnectorSharedContext.stop(taskConfig);
        log.info("Task[name={}]: stop", taskConfig.getTaskName());
    }

}
