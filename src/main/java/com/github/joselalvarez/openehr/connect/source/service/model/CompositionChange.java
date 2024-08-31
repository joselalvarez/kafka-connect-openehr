package com.github.joselalvarez.openehr.connect.source.service.model;

import com.github.joselalvarez.openehr.connect.source.task.offset.RecordOffset;
import com.nedap.archie.rm.archetyped.TemplateId;
import com.nedap.archie.rm.composition.Composition;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import com.nedap.archie.rm.support.identification.UIDBasedId;
import lombok.Data;

import java.time.ZonedDateTime;
import java.util.UUID;

@Data
public class CompositionChange {

    private ChangeType changeType;
    private ZonedDateTime timeCommitted;

    private UUID ehrId;
    private UUID uid;
    private Integer version;

    private ArchetypeID archetypeId;
    private TemplateId templateId;
    private UIDBasedId compositionId;
    private UIDBasedId replacedId;
    private Composition composition;

    private RecordOffset offset;

}
