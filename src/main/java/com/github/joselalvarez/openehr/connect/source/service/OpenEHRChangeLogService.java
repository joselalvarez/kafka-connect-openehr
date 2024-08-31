package com.github.joselalvarez.openehr.connect.source.service;

import com.github.joselalvarez.openehr.connect.source.service.model.ChangeRequest;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionChange;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionChangeRequest;
import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusChange;

import java.util.stream.Stream;

public interface OpenEHRChangeLogService {
    Stream<CompositionChange> getCompositionChanges(CompositionChangeRequest request);
    Stream<EhrStatusChange> getEhrStatusChanges(ChangeRequest request);
}
