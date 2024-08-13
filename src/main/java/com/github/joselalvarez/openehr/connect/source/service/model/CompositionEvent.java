package com.github.joselalvarez.openehr.connect.source.service.model;

import com.nedap.archie.rm.archetyped.TemplateId;
import com.nedap.archie.rm.composition.Composition;
import com.nedap.archie.rm.support.identification.ArchetypeID;
import com.nedap.archie.rm.support.identification.UIDBasedId;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.time.ZonedDateTime;
import java.util.UUID;

@Data
public class CompositionEvent {

    private ChangeType changeType;
    private ZonedDateTime timeCommitted;

    private UUID ehrId;
    private UUID uid;
    private Integer version;

    private ArchetypeID archetypeID;
    private TemplateId templateId;
    private UIDBasedId compositionId;
    private UIDBasedId replacedId;
    private Composition composition;

    private EventLogOffset offset;

}
