package com.github.joselalvarez.openehr.connect.source.service.ehrbase;

import com.github.joselalvarez.openehr.connect.source.repository.EHRBaseRepository;
import com.github.joselalvarez.openehr.connect.source.service.OpenEHREventLogService;
import com.github.joselalvarez.openehr.connect.source.service.ehrbase.mapper.EHRBaseEventLogMapper;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionEvent;
import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusEvent;
import com.github.joselalvarez.openehr.connect.source.service.model.EventLogFilter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class EHRBaseEventLogService implements OpenEHREventLogService {

    private final EHRBaseRepository ehrBaseRepository;
    private final static EHRBaseEventLogMapper mapper = new EHRBaseEventLogMapper();

    @Override
    public List<CompositionEvent> getCompositionEventList(EventLogFilter filter) {
        try {
            return mapper.mapCompositionEventList(ehrBaseRepository.findCompositionAggregateEventList(filter));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<EhrStatusEvent> getEhrStatusEventList(EventLogFilter filter) {
        try {
            return mapper.mapEhrStatusEventList(ehrBaseRepository.findEhrStatusAggregateEventList(filter));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
