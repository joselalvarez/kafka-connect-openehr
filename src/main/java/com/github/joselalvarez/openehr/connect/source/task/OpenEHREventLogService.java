package com.github.joselalvarez.openehr.connect.source.task;

import com.github.joselalvarez.openehr.connect.source.task.model.CompositionEvent;
import com.github.joselalvarez.openehr.connect.source.task.model.EhrStatusEvent;
import com.github.joselalvarez.openehr.connect.source.task.model.EventFilter;

import java.util.List;

public interface OpenEHREventLogService {
    List<CompositionEvent> getCompositionEventList(EventFilter filter);
    List<EhrStatusEvent> getEhrStatusEventList(EventFilter filter);
}
