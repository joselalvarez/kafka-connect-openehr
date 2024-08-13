package com.github.joselalvarez.openehr.connect.source.service;

import com.github.joselalvarez.openehr.connect.source.service.model.CompositionEvent;
import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusEvent;
import com.github.joselalvarez.openehr.connect.source.service.model.EventLogFilter;

import java.util.List;

public interface OpenEHREventLogService {
    List<CompositionEvent> getCompositionEventList(EventLogFilter filter);
    List<EhrStatusEvent> getEhrStatusEventList(EventLogFilter filter);
}
