package com.github.joselalvarez.openehr.connect.source;

import com.github.joselalvarez.openehr.connect.common.ConnectorInfo;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.config.context.OpenEHRSourceConnectorContext;
import com.github.joselalvarez.openehr.connect.source.config.context.OpenEHRSourceConnectorSharedContext;
import com.github.joselalvarez.openehr.connect.source.record.CompositionEventRecordMapper;
import com.github.joselalvarez.openehr.connect.source.record.EhrStatusEventRecordMapper;
import com.github.joselalvarez.openehr.connect.source.record.LocalOffsetStorage;
import com.github.joselalvarez.openehr.connect.source.service.OpenEHREventLogService;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionEvent;
import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusEvent;
import com.github.joselalvarez.openehr.connect.source.service.model.EventLogFilter;
import com.github.joselalvarez.openehr.connect.source.service.model.EventLogOffset;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

@Slf4j
public class OpenEHRSourceTask extends PeriodicSourceTask {

    private OpenEHRSourceConnectorConfig taskConfig;
    private OpenEHRSourceConnectorContext connectorContext;
    private OpenEHREventLogService eventLogService;
    private CompositionEventRecordMapper compositionEventRecordMapper;
    private EhrStatusEventRecordMapper ehrStatusEventRecordMapper;
    private LocalOffsetStorage<EventLogOffset> compositionEventOffsetStore;
    private LocalOffsetStorage<EventLogOffset> ehrStatusEventOffsetStore;


    @Override
    public String version() {
        return ConnectorInfo.CONNECTOR_VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        taskConfig = new OpenEHRSourceConnectorConfig(map);
        connectorContext = OpenEHRSourceConnectorSharedContext.start(taskConfig);
        eventLogService = connectorContext.getOpenEHREventLogService();
        compositionEventRecordMapper = connectorContext.getCompositionEventRecordMapper();
        ehrStatusEventRecordMapper = connectorContext.getEhrStatusEventRecordMapper();

        compositionEventOffsetStore = new LocalOffsetStorage(
                context.offsetStorageReader(),
                taskConfig,
                compositionEventRecordMapper);

        ehrStatusEventOffsetStore = new LocalOffsetStorage(
                context.offsetStorageReader(),
                taskConfig,
                ehrStatusEventRecordMapper);
        super.start(taskConfig.getPollIntervalMs(), connectorContext::isClosed);
        log.info("Task[name={}]: online", taskConfig.getTaskName());
    }

    @Override
    public List<SourceRecord> periodicCompositionPoll() {

        EventLogFilter filter = new EventLogFilter(
                taskConfig.getTablePatitionSize(),
                taskConfig.getTaskMax(),
                taskConfig.getTaskId(),
                taskConfig.getPollBatchSize());

        filter.setFromDate(taskConfig.getFilterCompositionFromDate());
        filter.setToDate(taskConfig.getFilterCompositionToDate());
        filter.setRootConcept(taskConfig.getFilterCompositionByRootConcept());
        filter.setTemplateId(taskConfig.getFilterCompositionByTemplateId());
        filter.setOffsetList(compositionEventOffsetStore.getCurrentOffsetList());

        List<CompositionEvent> pollList = eventLogService.getCompositionEventList(filter);

        return compositionEventOffsetStore.registerOffsets(compositionEventRecordMapper
                            .mapRecordList(pollList, taskConfig.getCompositionTopic())
        );
    }

    @Override
    public List<SourceRecord> periodicEhrStatusPoll() {
        EventLogFilter filter = new EventLogFilter(
                taskConfig.getTablePatitionSize(),
                taskConfig.getTaskMax(),
                taskConfig.getTaskId(),
                taskConfig.getPollBatchSize());

        //filter.setFromDate(taskConfig.getFilterCompositionFromDate());
        //filter.setToDate(taskConfig.getFilterCompositionToDate());
        filter.setOffsetList(ehrStatusEventOffsetStore.getCurrentOffsetList());

        List<EhrStatusEvent> pollList = eventLogService.getEhrStatusEventList(filter);

        return ehrStatusEventOffsetStore.registerOffsets(ehrStatusEventRecordMapper
                .mapRecordList(pollList, taskConfig.getEhrTopic())
        );
    }

    @Override
    public void stop() {
        connectorContext.close();
        OpenEHRSourceConnectorSharedContext.stop(taskConfig);
        log.info("Task[name={}]: stop", taskConfig.getTaskName());
    }

}
