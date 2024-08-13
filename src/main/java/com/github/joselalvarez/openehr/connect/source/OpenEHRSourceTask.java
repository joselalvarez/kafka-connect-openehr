package com.github.joselalvarez.openehr.connect.source;

import com.github.joselalvarez.openehr.connect.common.ConnectorInfo;
import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.config.context.OpenEHRSourceConnectorContext;
import com.github.joselalvarez.openehr.connect.source.config.context.OpenEHRSourceConnectorSharedContext;
import com.github.joselalvarez.openehr.connect.source.record.CompositionEventRecordMapper;
import com.github.joselalvarez.openehr.connect.source.record.LocalOffsetStorage;
import com.github.joselalvarez.openehr.connect.source.service.OpenEHREventLogService;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionEvent;
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
    private CompositionEventRecordMapper messageMapper;
    private LocalOffsetStorage<CompositionEvent, EventLogOffset> compositionEventOffsetStore;

    @Override
    public String version() {
        return ConnectorInfo.CONNECTOR_VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        taskConfig = new OpenEHRSourceConnectorConfig(map);
        connectorContext = OpenEHRSourceConnectorSharedContext.start(taskConfig);
        eventLogService = connectorContext.getOpenEHREventLogService();
        messageMapper = connectorContext.geCompositionEventRecordMapper();
        compositionEventOffsetStore = new LocalOffsetStorage(taskConfig, context.offsetStorageReader(), messageMapper);
        super.start(taskConfig.getPollIntervalMs(), connectorContext::isClosed);
        log.info("Task[name={}]: online", taskConfig.getTaskName());
    }

    @Override
    public List<SourceRecord> periodicPoll() {

        EventLogFilter filter = new EventLogFilter(
                taskConfig.getTablePatitionSize(),
                taskConfig.getTaskMax(),
                taskConfig.getTaskId(),
                taskConfig.getPollBatchSize());

        filter.setFromDate(taskConfig.getFilterFromDate());
        filter.setToDate(taskConfig.getFilterToDate());
        filter.setRootConcept(taskConfig.getFilterByRootConcept());
        filter.setTemplateId(taskConfig.getFilterByTemplateId());

        filter.setOffsetList(compositionEventOffsetStore.getCurrentOffsetList());

        List<CompositionEvent> pollList = eventLogService.getCompositionEventList(filter);

        compositionEventOffsetStore.updateOffsetsFrom(pollList,
                CompositionEvent::getOffset,
                EventLogOffset::getPartition
        );

        return messageMapper.mapRecordList(pollList, taskConfig.getCompositionTopic());
    }

    @Override
    public void stop() {
        connectorContext.close();
        OpenEHRSourceConnectorSharedContext.stop(taskConfig);
        log.info("Task[name={}]: stop", taskConfig.getTaskName());
    }

}
