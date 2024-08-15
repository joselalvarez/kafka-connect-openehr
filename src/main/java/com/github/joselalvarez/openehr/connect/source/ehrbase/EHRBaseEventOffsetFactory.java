package com.github.joselalvarez.openehr.connect.source.ehrbase;

import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import com.github.joselalvarez.openehr.connect.source.ehrbase.entity.EHRBaseEventOffset;
import com.github.joselalvarez.openehr.connect.source.record.RecordOffset;
import com.github.joselalvarez.openehr.connect.source.record.RecordPartitionFactory;
import com.github.joselalvarez.openehr.connect.source.task.model.CompositionEvent;
import com.github.joselalvarez.openehr.connect.source.task.model.EhrStatusEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EHRBaseEventOffsetFactory implements RecordPartitionFactory {

    public OpenEHRSourceConnectorConfig config;

    public EHRBaseEventOffsetFactory(OpenEHRSourceConnectorConfig config) {
        this.config = config;
    }

    public int getTaskMax() {
        return config.getTaskMax();
    }

    public int getTablePartitionSize() {
        return config.getTablePatitionSize();
    }

    public long getPollBatchSize() {
        return config.getPollBatchSize();
    }

    @Override
    public List<Map<String, ?>> getTaskPartitions(int taskId) {
        List<Map<String, ?>> partitions = new ArrayList<>();
        for (int i = 0; i < config.getTablePatitionSize(); i++) {
            if (i % config.getTaskMax() == taskId) {
                partitions.add(new EHRBaseEventOffset(CompositionEvent.class, i).getPartitionMap());
                partitions.add(new EHRBaseEventOffset(EhrStatusEvent.class, i).getPartitionMap());
            }
        }
        return partitions;
    }

    @Override
    public RecordOffset build(Map<String, ?> partition, Map<String, ?> offset) {
        return EHRBaseEventOffset.from(partition, offset);
    }
}
