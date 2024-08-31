package com.github.joselalvarez.openehr.connect.source.ehrbase;

import com.github.joselalvarez.openehr.connect.source.service.OpenEHRChangeLogService;
import com.github.joselalvarez.openehr.connect.source.service.model.ChangeRequest;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionChange;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionChangeRequest;
import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusChange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class EHRBaseChangeLogService implements OpenEHRChangeLogService {

    private final EHRBaseRepository ehrBaseRepository;
    private final static EHRBaseChangeMapper mapper = new EHRBaseChangeMapper();

    @Override
    public Stream<CompositionChange> getCompositionChanges(CompositionChangeRequest request) {
        try {
            return ehrBaseRepository.findCompositionChanges(request).stream()
                    .map(mapper::mapCompositionChange);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Stream<EhrStatusChange> getEhrStatusChanges(ChangeRequest request) {
        try {
            return ehrBaseRepository.findEhrStatusChanges(request).stream()
                    .map(mapper::mapEhrStatusChange);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
