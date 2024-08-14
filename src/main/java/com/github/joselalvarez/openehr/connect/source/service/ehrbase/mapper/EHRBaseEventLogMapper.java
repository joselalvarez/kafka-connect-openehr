package com.github.joselalvarez.openehr.connect.source.service.ehrbase.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.joselalvarez.openehr.connect.source.repository.entity.CommonAggregateEvent;
import com.github.joselalvarez.openehr.connect.source.service.model.ChangeType;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionEvent;
import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusEvent;
import com.github.joselalvarez.openehr.connect.source.service.model.EventLogOffset;
import com.nedap.archie.rm.composition.Composition;
import com.nedap.archie.rm.ehr.EhrStatus;
import com.nedap.archie.rm.generic.PartyProxy;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import com.nedap.archie.rm.support.identification.GenericId;
import com.nedap.archie.rm.support.identification.ObjectVersionId;
import com.nedap.archie.rm.support.identification.PartyRef;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.ehrbase.openehr.sdk.dbformat.DbToRmFormat;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class EHRBaseEventLogMapper {

    private static final ObjectMapper mapper = new ObjectMapper();

    public CompositionEvent mapCompositionEvent(CompositionEvent target, CommonAggregateEvent source) {

        target.setChangeType(ChangeType.parse(source.getChangeType()));
        target.setTimeCommitted(source.getTimeCommitted());

        target.setEhrId(source.getEhrId());
        target.setUid(source.getUid());
        target.setVersion(source.getSysVersion());

        Composition composition = DbToRmFormat.reconstructRmObject(Composition.class, new String(source.getAggregate()));

        if (composition.getArchetypeDetails() != null) {
            target.setArchetypeId(composition.getArchetypeDetails().getArchetypeId());
            target.setTemplateId(composition.getArchetypeDetails().getTemplateId());
        }

        if (ChangeType.CREATION.equals(target.getChangeType()) ||
                ChangeType.MODIFICATION.equals(target.getChangeType())) {

            target.setComposition(composition);
            target.setCompositionId(composition.getUid());

            if (ChangeType.MODIFICATION.equals(target.getChangeType())) {
                try {
                    ObjectVersionId currentVersionId = new ObjectVersionId(composition.getUid().getValue());
                    target.setReplacedId(new ObjectVersionId(
                            currentVersionId.getObjectId().getValue(),
                            currentVersionId.getCreatingSystemId().getValue(),
                            String.valueOf(source.getSysVersion() - 1)));
                } catch (Exception e) {
                    log.error("Invalid composition identifier: {}", e);
                }
            }

        } else /*DELETED*/{ //In elimination events the recovered composition by the query is the previous one
            target.setReplacedId(composition.getUid());
        }

        target.setOffset(new EventLogOffset(
                source.getTablePartition(),
                source.getTimeCommitted(),
                source.getUid(),
                source.getSysVersion()));

        return target;
    }

    public List<CompositionEvent> mapCompositionEventList(List<CommonAggregateEvent> sourceList) {
        List<CompositionEvent> resultList = new ArrayList<>();
        for (CommonAggregateEvent source : sourceList) {
            resultList.add(mapCompositionEvent(new CompositionEvent(), source));
        }
        return resultList;
    }

    public EhrStatusEvent mapEhrStatusEvent(EhrStatusEvent target, CommonAggregateEvent source) {

        target.setChangeType(ChangeType.parse(source.getChangeType()));
        target.setTimeCommitted(source.getTimeCommitted());

        target.setEhrId(source.getEhrId());
        target.setUid(source.getUid());
        target.setVersion(source.getSysVersion());

        EhrStatus status = DbToRmFormat.reconstructRmObject(EhrStatus.class, new String(source.getAggregate()));

        if (status != null) {
            target.setEhrStatus(status);
            target.setArchetypeId(status.getArchetypeNodeId());
            target.setEhrStatusId(status.getUid());
            // There is no delete event in the ehr status
            if (ChangeType.MODIFICATION.equals(target.getChangeType())) {
                try {
                    ObjectVersionId currentVersionId = new ObjectVersionId(status.getUid().getValue());
                    target.setReplacedId(new ObjectVersionId(
                            currentVersionId.getObjectId().getValue(),
                            currentVersionId.getCreatingSystemId().getValue(),
                            String.valueOf(source.getSysVersion() - 1)));
                } catch (Exception e) {
                    log.error("Invalid composition identifier: {}", e);
                }
            }

            //Subject
            if (status.getSubject() != null && status.getSubject().getExternalRef() != null) {
                PartyRef ref = status.getSubject().getExternalRef();
                target.setSubjectNamespace(ref.getNamespace());
                target.setSubjectType(ref.getType());
                if (ref.getId() != null) {
                    target.setSubjectId(ref.getId().getValue());
                    if (ref.getId() instanceof GenericId) {
                        target.setSubjectIdScheme(((GenericId) ref.getId()).getScheme());
                    }
                }
            }
        }

        target.setOffset(new EventLogOffset(
                source.getTablePartition(),
                source.getTimeCommitted(),
                source.getUid(),
                source.getSysVersion()));

        return target;
    }

    public List<EhrStatusEvent> mapEhrStatusEventList(List<CommonAggregateEvent> sourceList) {
        List<EhrStatusEvent> resultList = new ArrayList<>();
        for (CommonAggregateEvent source : sourceList) {
            resultList.add(mapEhrStatusEvent(new EhrStatusEvent(), source));
        }
        return resultList;
    }

}
