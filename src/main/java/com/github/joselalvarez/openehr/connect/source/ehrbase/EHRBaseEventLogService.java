package com.github.joselalvarez.openehr.connect.source.ehrbase;

import com.github.joselalvarez.openehr.connect.source.task.OpenEHREventLogService;
import com.github.joselalvarez.openehr.connect.source.task.model.CompositionEvent;
import com.github.joselalvarez.openehr.connect.source.task.model.EhrStatusEvent;
import com.github.joselalvarez.openehr.connect.source.task.model.EventFilter;
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
    public List<CompositionEvent> getCompositionEventList(EventFilter filter) {
        try {
            return mapper.mapCompositionEventList(ehrBaseRepository.findCompositionEventList(filter));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<EhrStatusEvent> getEhrStatusEventList(EventFilter filter) {
        try {
            return mapper.mapEhrStatusEventList(ehrBaseRepository.findEhrStatusEventList(filter));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
