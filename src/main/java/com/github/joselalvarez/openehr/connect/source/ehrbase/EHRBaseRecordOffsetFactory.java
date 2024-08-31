package com.github.joselalvarez.openehr.connect.source.ehrbase;

import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.ehrbase.entity.EHRBaseChangeOffset;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionChange;
import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusChange;
import com.github.joselalvarez.openehr.connect.source.task.offset.RecordOffset;
import com.github.joselalvarez.openehr.connect.source.task.offset.RecordOffsetFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EHRBaseRecordOffsetFactory implements RecordOffsetFactory {

    private OpenEHRSourceConnectorConfig connectorConfig;

    public EHRBaseRecordOffsetFactory(OpenEHRSourceConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    private List<Map<String, ?>> getChangePartitionsByTask(int taskId, Class<?> type) {
        List<Map<String, ?>> partitions = new ArrayList<>();
        for (int i = 0; i < connectorConfig.getTablePartitionSize(); i++) {
            if (i % connectorConfig.getTasksConfigured() == taskId) {
                partitions.add(new EHRBaseChangeOffset(type, i).getPartitionMap());
            }
        }
        return partitions;
    }

    @Override
    public List<Map<String, ?>> getCompositionChangePartitionsByTask(int taskId) {
        return getChangePartitionsByTask(taskId, CompositionChange.class);
    }

    @Override
    public List<Map<String, ?>> getEhrStatusChangePartitionsByTask(int taskId) {
        return getChangePartitionsByTask(taskId, EhrStatusChange.class);
    }

    @Override
    public RecordOffset buildRecordOffset(Map<String, ?> partition, Map<String, ?> offset) {
        return EHRBaseChangeOffset.from(partition, offset);
    }
}
