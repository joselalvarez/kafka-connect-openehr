package com.github.joselalvarez.openehr.connect.source;

import com.github.joselalvarez.openehr.connect.common.ConnectorInfo;
import com.github.joselalvarez.openehr.connect.common.TaskConfig;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceTaskConfig;
import com.github.joselalvarez.openehr.connect.source.config.context.OpenEHRSourceConnectorContext;
import com.github.joselalvarez.openehr.connect.source.config.context.OpenEHRSourceConnectorSharedContext;
import com.github.joselalvarez.openehr.connect.source.record.CompositionEventRecordMapper;
import com.github.joselalvarez.openehr.connect.source.record.EhrStatusEventRecordMapper;
import com.github.joselalvarez.openehr.connect.source.record.TaskOffsetStorage;
import com.github.joselalvarez.openehr.connect.source.task.OpenEHREventLogService;
import com.github.joselalvarez.openehr.connect.source.task.model.CompositionEvent;
import com.github.joselalvarez.openehr.connect.source.task.model.EhrStatusEvent;
import com.github.joselalvarez.openehr.connect.source.task.model.EventFilter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

@Slf4j
public class OpenEHRSourceTask extends PeriodicSourceTask {

    private OpenEHRSourceTaskConfig taskConfig = new OpenEHRSourceTaskConfig();
    private OpenEHRSourceConnectorConfig connectorConfig;
    private OpenEHRSourceConnectorContext connectorContext;
    private OpenEHREventLogService eventLogService;
    private CompositionEventRecordMapper compositionEventRecordMapper;
    private EhrStatusEventRecordMapper ehrStatusEventRecordMapper;
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
        eventLogService = connectorContext.getOpenEHREventLogService();
        compositionEventRecordMapper = connectorContext.getCompositionEventRecordMapper();
        ehrStatusEventRecordMapper = connectorContext.getEhrStatusEventRecordMapper();
        taskOffsetStorage = new TaskOffsetStorage(
                context.offsetStorageReader(),
                taskConfig,
                connectorContext.getRecordPartitionFactory()
        );
        super.start(connectorConfig.getPollIntervalMs(), connectorContext::isClosed);
        log.info("Task[name={}]: online", taskConfig.getTaskName());
    }

    @Override
    public List<SourceRecord> periodicCompositionPoll() {
        EventFilter filter = new EventFilter();
        filter.setFromDate(connectorConfig.getFilterCompositionFromDate());
        filter.setToDate(connectorConfig.getFilterCompositionToDate());
        filter.setRootConcept(connectorConfig.getFilterCompositionByArchetype());
        filter.setTemplateId(connectorConfig.getFilterCompositionByTemplate());
        filter.setOffsetList(taskOffsetStorage.getOffsetListSnapshot(CompositionEvent.class));

        List<CompositionEvent> pollList = eventLogService.getCompositionEventList(filter);

        return taskOffsetStorage.updateWith(compositionEventRecordMapper
                            .mapRecordList(pollList, connectorConfig.getCompositionTopic())
        );
    }

    @Override
    public List<SourceRecord> periodicEhrStatusPoll() {
        EventFilter filter = new EventFilter();

        filter.setOffsetList(taskOffsetStorage.getOffsetListSnapshot(EhrStatusEvent.class));

        List<EhrStatusEvent> pollList = eventLogService.getEhrStatusEventList(filter);

        return taskOffsetStorage.updateWith(ehrStatusEventRecordMapper
                .mapRecordList(pollList, connectorConfig.getEhrTopic())
        );
    }

    @Override
    public void stop() {
        connectorContext.close();
        OpenEHRSourceConnectorSharedContext.stop(taskConfig);
        log.info("Task[name={}]: stop", taskConfig.getTaskName());
    }

}
