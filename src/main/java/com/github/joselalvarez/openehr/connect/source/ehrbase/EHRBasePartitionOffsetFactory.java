package com.github.joselalvarez.openehr.connect.source.ehrbase;

import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.ehrbase.entity.EHRBaseChangePartitionOffset;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionChange;
import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusChange;
import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffset;
import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffsetFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EHRBasePartitionOffsetFactory implements PartitionOffsetFactory {

    private OpenEHRSourceConnectorConfig connectorConfig;

    public EHRBasePartitionOffsetFactory(OpenEHRSourceConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    private List<Map<String, ?>> getChangePartitionsByTask(int taskId, Class<?> type) {
        List<Map<String, ?>> partitions = new ArrayList<>();
        for (int i = 0; i < connectorConfig.getTablePartitionSize(); i++) {
            if (i % connectorConfig.getTasksConfigured() == taskId) {
                partitions.add(new EHRBaseChangePartitionOffset(type, i).getPartitionMap());
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
    public PartitionOffset buildPartitionOffset(Map<String, ?> partition, Map<String, ?> offset) {
        return EHRBaseChangePartitionOffset.from(partition, offset);
    }
}
