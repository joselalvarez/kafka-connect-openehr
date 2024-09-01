package com.github.joselalvarez.openehr.connect.source.ehrbase;

import com.github.joselalvarez.openehr.connect.source.ehrbase.entity.EHRBaseChange;
import com.github.joselalvarez.openehr.connect.source.ehrbase.entity.EHRBaseChangePartitionOffset;
import com.github.joselalvarez.openehr.connect.source.exception.OpenEHRSourceConnectException;
import com.github.joselalvarez.openehr.connect.source.service.model.ChangeType;
import com.github.joselalvarez.openehr.connect.source.service.model.CompositionChange;
import com.github.joselalvarez.openehr.connect.source.service.model.EhrStatusChange;
import com.nedap.archie.rm.composition.Composition;
import com.nedap.archie.rm.ehr.EhrStatus;
import com.nedap.archie.rm.support.identification.GenericId;
import com.nedap.archie.rm.support.identification.ObjectVersionId;
import com.nedap.archie.rm.support.identification.PartyRef;
import lombok.extern.slf4j.Slf4j;
import org.ehrbase.openehr.sdk.dbformat.DbToRmFormat;

@Slf4j
public class EHRBaseChangeMapper {

    public CompositionChange mapCompositionChange(EHRBaseChange source) {

        CompositionChange target = new CompositionChange();

        target.setChangeType(ChangeType.parse(source.getChangeType()));
        target.setTimeCommitted(source.getTimeCommitted());

        target.setEhrId(source.getEhrId());
        target.setUid(source.getUid());
        target.setVersion(source.getSysVersion());

        // Partition offset
        target.setPartitionOffset(new EHRBaseChangePartitionOffset(
                target.getClass(),
                source.getTablePartition(),
                source.getTimeCommitted(),
                source.getUid(),
                source.getSysVersion()));

        try {
            Composition composition = DbToRmFormat.reconstructRmObject(Composition.class, new String(source.getAggregate()));

            // Archetype details
            if (composition.getArchetypeDetails() != null) {
                target.setArchetypeId(composition.getArchetypeDetails().getArchetypeId());
                target.setTemplateId(composition.getArchetypeDetails().getTemplateId());
            }

            // Resource
            if (ChangeType.CREATION.equals(target.getChangeType()) ||
                    ChangeType.MODIFICATION.equals(target.getChangeType())) {

                target.setComposition(composition);
                target.setCompositionId(composition.getUid());

                if (ChangeType.MODIFICATION.equals(target.getChangeType())) {
                    ObjectVersionId currentVersionId = new ObjectVersionId(composition.getUid().getValue());
                    target.setReplacedId(new ObjectVersionId(
                            currentVersionId.getObjectId().getValue(),
                            currentVersionId.getCreatingSystemId().getValue(),
                            String.valueOf(source.getSysVersion() - 1)));
                }

            } else /*DELETED*/ { //In elimination events the recovered composition by the query is the previous one
                target.setReplacedId(composition.getUid());
            }

        }catch (Exception e) {
            log.error("Mapping error: {}", e);
            OpenEHRSourceConnectException.mappingError(e, target.getPartitionOffset());
        }

        return target;
    }

    public EhrStatusChange mapEhrStatusChange(EHRBaseChange source) {

        EhrStatusChange target = new EhrStatusChange();

        target.setChangeType(ChangeType.parse(source.getChangeType()));
        target.setTimeCommitted(source.getTimeCommitted());

        target.setEhrId(source.getEhrId());
        target.setUid(source.getUid());
        target.setVersion(source.getSysVersion());

        // Partition offset
        target.setPartitionOffset(new EHRBaseChangePartitionOffset(
                target.getClass(),
                source.getTablePartition(),
                source.getTimeCommitted(),
                source.getUid(),
                source.getSysVersion()));

        try {
            EhrStatus status = DbToRmFormat.reconstructRmObject(EhrStatus.class, new String(source.getAggregate()));

            // Archetype details
            target.setArchetypeId(status.getArchetypeNodeId());

            // Resource
            if (ChangeType.CREATION.equals(target.getChangeType()) ||
                    ChangeType.MODIFICATION.equals(target.getChangeType())) {
                target.setEhrStatus(status);
                target.setEhrStatusId(status.getUid());
                if (ChangeType.MODIFICATION.equals(target.getChangeType())) {
                    ObjectVersionId currentVersionId = new ObjectVersionId(status.getUid().getValue());
                    target.setReplacedId(new ObjectVersionId(
                            currentVersionId.getObjectId().getValue(),
                            currentVersionId.getCreatingSystemId().getValue(),
                            String.valueOf(source.getSysVersion() - 1)));
                }
            } else /*DELETED*/ { //In elimination events the recovered ehr_status by the query is the previous one
                target.setReplacedId(status.getUid());
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
        }catch (Exception e) {
            log.error("Mapping error: {}", e);
            OpenEHRSourceConnectException.mappingError(e, target.getPartitionOffset());
        }

        return target;
    }

}
