CREATE EXTERNAL TABLE [fhir].[AdverseEvent] (
    [resourceType] NVARCHAR(4000),
    [id] VARCHAR(64),
    [meta.id] NVARCHAR(100),
    [meta.extension] NVARCHAR(MAX),
    [meta.versionId] VARCHAR(64),
    [meta.lastUpdated] VARCHAR(64),
    [meta.source] VARCHAR(256),
    [meta.profile] VARCHAR(MAX),
    [meta.security] VARCHAR(MAX),
    [meta.tag] VARCHAR(MAX),
    [implicitRules] VARCHAR(256),
    [language] NVARCHAR(100),
    [text.id] NVARCHAR(100),
    [text.extension] NVARCHAR(MAX),
    [text.status] NVARCHAR(64),
    [text.div] NVARCHAR(MAX),
    [extension] NVARCHAR(MAX),
    [modifierExtension] NVARCHAR(MAX),
    [identifier] VARCHAR(MAX),
    [status] NVARCHAR(100),
    [actuality] NVARCHAR(4000),
    [category] VARCHAR(MAX),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [subject.id] NVARCHAR(100),
    [subject.extension] NVARCHAR(MAX),
    [subject.reference] NVARCHAR(4000),
    [subject.type] VARCHAR(256),
    [subject.identifier.id] NVARCHAR(100),
    [subject.identifier.extension] NVARCHAR(MAX),
    [subject.identifier.use] NVARCHAR(64),
    [subject.identifier.type] NVARCHAR(MAX),
    [subject.identifier.system] VARCHAR(256),
    [subject.identifier.value] NVARCHAR(4000),
    [subject.identifier.period] NVARCHAR(MAX),
    [subject.identifier.assigner] NVARCHAR(MAX),
    [subject.display] NVARCHAR(4000),
    [encounter.id] NVARCHAR(100),
    [encounter.extension] NVARCHAR(MAX),
    [encounter.reference] NVARCHAR(4000),
    [encounter.type] VARCHAR(256),
    [encounter.identifier.id] NVARCHAR(100),
    [encounter.identifier.extension] NVARCHAR(MAX),
    [encounter.identifier.use] NVARCHAR(64),
    [encounter.identifier.type] NVARCHAR(MAX),
    [encounter.identifier.system] VARCHAR(256),
    [encounter.identifier.value] NVARCHAR(4000),
    [encounter.identifier.period] NVARCHAR(MAX),
    [encounter.identifier.assigner] NVARCHAR(MAX),
    [encounter.display] NVARCHAR(4000),
    [detected] VARCHAR(64),
    [recordedDate] VARCHAR(64),
    [resultingCondition] VARCHAR(MAX),
    [location.id] NVARCHAR(100),
    [location.extension] NVARCHAR(MAX),
    [location.reference] NVARCHAR(4000),
    [location.type] VARCHAR(256),
    [location.identifier.id] NVARCHAR(100),
    [location.identifier.extension] NVARCHAR(MAX),
    [location.identifier.use] NVARCHAR(64),
    [location.identifier.type] NVARCHAR(MAX),
    [location.identifier.system] VARCHAR(256),
    [location.identifier.value] NVARCHAR(4000),
    [location.identifier.period] NVARCHAR(MAX),
    [location.identifier.assigner] NVARCHAR(MAX),
    [location.display] NVARCHAR(4000),
    [seriousness.id] NVARCHAR(100),
    [seriousness.extension] NVARCHAR(MAX),
    [seriousness.coding] VARCHAR(MAX),
    [seriousness.text] NVARCHAR(4000),
    [outcome] VARCHAR(MAX),
    [recorder.id] NVARCHAR(100),
    [recorder.extension] NVARCHAR(MAX),
    [recorder.reference] NVARCHAR(4000),
    [recorder.type] VARCHAR(256),
    [recorder.identifier.id] NVARCHAR(100),
    [recorder.identifier.extension] NVARCHAR(MAX),
    [recorder.identifier.use] NVARCHAR(64),
    [recorder.identifier.type] NVARCHAR(MAX),
    [recorder.identifier.system] VARCHAR(256),
    [recorder.identifier.value] NVARCHAR(4000),
    [recorder.identifier.period] NVARCHAR(MAX),
    [recorder.identifier.assigner] NVARCHAR(MAX),
    [recorder.display] NVARCHAR(4000),
    [participant] VARCHAR(MAX),
    [suspectEntity] VARCHAR(MAX),
    [contributingFactor] VARCHAR(MAX),
    [preventiveAction] VARCHAR(MAX),
    [mitigatingAction] VARCHAR(MAX),
    [supportingInfo] VARCHAR(MAX),
    [study] VARCHAR(MAX),
    [occurrence.dateTime] VARCHAR(64),
    [occurrence.period.id] NVARCHAR(100),
    [occurrence.period.extension] NVARCHAR(MAX),
    [occurrence.period.start] VARCHAR(64),
    [occurrence.period.end] VARCHAR(64),
    [occurrence.timing.id] NVARCHAR(100),
    [occurrence.timing.extension] NVARCHAR(MAX),
    [occurrence.timing.modifierExtension] NVARCHAR(MAX),
    [occurrence.timing.event] VARCHAR(MAX),
    [occurrence.timing.repeat.id] NVARCHAR(100),
    [occurrence.timing.repeat.extension] NVARCHAR(MAX),
    [occurrence.timing.repeat.modifierExtension] NVARCHAR(MAX),
    [occurrence.timing.repeat.count] bigint,
    [occurrence.timing.repeat.countMax] bigint,
    [occurrence.timing.repeat.duration] float,
    [occurrence.timing.repeat.durationMax] float,
    [occurrence.timing.repeat.durationUnit] NVARCHAR(64),
    [occurrence.timing.repeat.frequency] bigint,
    [occurrence.timing.repeat.frequencyMax] bigint,
    [occurrence.timing.repeat.period] float,
    [occurrence.timing.repeat.periodMax] float,
    [occurrence.timing.repeat.periodUnit] NVARCHAR(64),
    [occurrence.timing.repeat.dayOfWeek] NVARCHAR(MAX),
    [occurrence.timing.repeat.timeOfDay] NVARCHAR(MAX),
    [occurrence.timing.repeat.when] NVARCHAR(MAX),
    [occurrence.timing.repeat.offset] bigint,
    [occurrence.timing.repeat.bounds.duration] NVARCHAR(MAX),
    [occurrence.timing.repeat.bounds.range] NVARCHAR(MAX),
    [occurrence.timing.repeat.bounds.period] NVARCHAR(MAX),
    [occurrence.timing.code.id] NVARCHAR(100),
    [occurrence.timing.code.extension] NVARCHAR(MAX),
    [occurrence.timing.code.coding] NVARCHAR(MAX),
    [occurrence.timing.code.text] NVARCHAR(4000),
) WITH (
    LOCATION='/AdverseEvent/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.AdverseEventIdentifier AS
SELECT
    [id],
    [identifier.JSON],
    [identifier.id],
    [identifier.extension],
    [identifier.use],
    [identifier.type.id],
    [identifier.type.extension],
    [identifier.type.coding],
    [identifier.type.text],
    [identifier.system],
    [identifier.value],
    [identifier.period.id],
    [identifier.period.extension],
    [identifier.period.start],
    [identifier.period.end],
    [identifier.assigner.id],
    [identifier.assigner.extension],
    [identifier.assigner.reference],
    [identifier.assigner.type],
    [identifier.assigner.identifier],
    [identifier.assigner.display]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [identifier.JSON]  VARCHAR(MAX) '$.identifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[identifier.JSON]) with (
        [identifier.id]                NVARCHAR(100)       '$.id',
        [identifier.extension]         NVARCHAR(MAX)       '$.extension',
        [identifier.use]               NVARCHAR(64)        '$.use',
        [identifier.type.id]           NVARCHAR(100)       '$.type.id',
        [identifier.type.extension]    NVARCHAR(MAX)       '$.type.extension',
        [identifier.type.coding]       NVARCHAR(MAX)       '$.type.coding',
        [identifier.type.text]         NVARCHAR(4000)      '$.type.text',
        [identifier.system]            VARCHAR(256)        '$.system',
        [identifier.value]             NVARCHAR(4000)      '$.value',
        [identifier.period.id]         NVARCHAR(100)       '$.period.id',
        [identifier.period.extension]  NVARCHAR(MAX)       '$.period.extension',
        [identifier.period.start]      VARCHAR(64)         '$.period.start',
        [identifier.period.end]        VARCHAR(64)         '$.period.end',
        [identifier.assigner.id]       NVARCHAR(100)       '$.assigner.id',
        [identifier.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [identifier.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [identifier.assigner.type]     VARCHAR(256)        '$.assigner.type',
        [identifier.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [identifier.assigner.display]  NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.AdverseEventCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [category.JSON]  VARCHAR(MAX) '$.category'
    ) AS rowset
    CROSS APPLY openjson (rowset.[category.JSON]) with (
        [category.id]                  NVARCHAR(100)       '$.id',
        [category.extension]           NVARCHAR(MAX)       '$.extension',
        [category.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [category.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.AdverseEventResultingCondition AS
SELECT
    [id],
    [resultingCondition.JSON],
    [resultingCondition.id],
    [resultingCondition.extension],
    [resultingCondition.reference],
    [resultingCondition.type],
    [resultingCondition.identifier.id],
    [resultingCondition.identifier.extension],
    [resultingCondition.identifier.use],
    [resultingCondition.identifier.type],
    [resultingCondition.identifier.system],
    [resultingCondition.identifier.value],
    [resultingCondition.identifier.period],
    [resultingCondition.identifier.assigner],
    [resultingCondition.display]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [resultingCondition.JSON]  VARCHAR(MAX) '$.resultingCondition'
    ) AS rowset
    CROSS APPLY openjson (rowset.[resultingCondition.JSON]) with (
        [resultingCondition.id]        NVARCHAR(100)       '$.id',
        [resultingCondition.extension] NVARCHAR(MAX)       '$.extension',
        [resultingCondition.reference] NVARCHAR(4000)      '$.reference',
        [resultingCondition.type]      VARCHAR(256)        '$.type',
        [resultingCondition.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [resultingCondition.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [resultingCondition.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [resultingCondition.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [resultingCondition.identifier.system] VARCHAR(256)        '$.identifier.system',
        [resultingCondition.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [resultingCondition.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [resultingCondition.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [resultingCondition.display]   NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.AdverseEventOutcome AS
SELECT
    [id],
    [outcome.JSON],
    [outcome.id],
    [outcome.extension],
    [outcome.coding],
    [outcome.text]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [outcome.JSON]  VARCHAR(MAX) '$.outcome'
    ) AS rowset
    CROSS APPLY openjson (rowset.[outcome.JSON]) with (
        [outcome.id]                   NVARCHAR(100)       '$.id',
        [outcome.extension]            NVARCHAR(MAX)       '$.extension',
        [outcome.coding]               NVARCHAR(MAX)       '$.coding' AS JSON,
        [outcome.text]                 NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.AdverseEventParticipant AS
SELECT
    [id],
    [participant.JSON],
    [participant.id],
    [participant.extension],
    [participant.modifierExtension],
    [participant.function.id],
    [participant.function.extension],
    [participant.function.coding],
    [participant.function.text],
    [participant.actor.id],
    [participant.actor.extension],
    [participant.actor.reference],
    [participant.actor.type],
    [participant.actor.identifier],
    [participant.actor.display]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [participant.JSON]  VARCHAR(MAX) '$.participant'
    ) AS rowset
    CROSS APPLY openjson (rowset.[participant.JSON]) with (
        [participant.id]               NVARCHAR(100)       '$.id',
        [participant.extension]        NVARCHAR(MAX)       '$.extension',
        [participant.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [participant.function.id]      NVARCHAR(100)       '$.function.id',
        [participant.function.extension] NVARCHAR(MAX)       '$.function.extension',
        [participant.function.coding]  NVARCHAR(MAX)       '$.function.coding',
        [participant.function.text]    NVARCHAR(4000)      '$.function.text',
        [participant.actor.id]         NVARCHAR(100)       '$.actor.id',
        [participant.actor.extension]  NVARCHAR(MAX)       '$.actor.extension',
        [participant.actor.reference]  NVARCHAR(4000)      '$.actor.reference',
        [participant.actor.type]       VARCHAR(256)        '$.actor.type',
        [participant.actor.identifier] NVARCHAR(MAX)       '$.actor.identifier',
        [participant.actor.display]    NVARCHAR(4000)      '$.actor.display'
    ) j

GO

CREATE VIEW fhir.AdverseEventSuspectEntity AS
SELECT
    [id],
    [suspectEntity.JSON],
    [suspectEntity.id],
    [suspectEntity.extension],
    [suspectEntity.modifierExtension],
    [suspectEntity.causality.id],
    [suspectEntity.causality.extension],
    [suspectEntity.causality.modifierExtension],
    [suspectEntity.causality.assessmentMethod],
    [suspectEntity.causality.entityRelatedness],
    [suspectEntity.causality.author],
    [suspectEntity.instance.codeableConcept.id],
    [suspectEntity.instance.codeableConcept.extension],
    [suspectEntity.instance.codeableConcept.coding],
    [suspectEntity.instance.codeableConcept.text],
    [suspectEntity.instance.reference.id],
    [suspectEntity.instance.reference.extension],
    [suspectEntity.instance.reference.reference],
    [suspectEntity.instance.reference.type],
    [suspectEntity.instance.reference.identifier],
    [suspectEntity.instance.reference.display]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [suspectEntity.JSON]  VARCHAR(MAX) '$.suspectEntity'
    ) AS rowset
    CROSS APPLY openjson (rowset.[suspectEntity.JSON]) with (
        [suspectEntity.id]             NVARCHAR(100)       '$.id',
        [suspectEntity.extension]      NVARCHAR(MAX)       '$.extension',
        [suspectEntity.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [suspectEntity.causality.id]   NVARCHAR(100)       '$.causality.id',
        [suspectEntity.causality.extension] NVARCHAR(MAX)       '$.causality.extension',
        [suspectEntity.causality.modifierExtension] NVARCHAR(MAX)       '$.causality.modifierExtension',
        [suspectEntity.causality.assessmentMethod] NVARCHAR(MAX)       '$.causality.assessmentMethod',
        [suspectEntity.causality.entityRelatedness] NVARCHAR(MAX)       '$.causality.entityRelatedness',
        [suspectEntity.causality.author] NVARCHAR(MAX)       '$.causality.author',
        [suspectEntity.instance.codeableConcept.id] NVARCHAR(100)       '$.instance.codeableConcept.id',
        [suspectEntity.instance.codeableConcept.extension] NVARCHAR(MAX)       '$.instance.codeableConcept.extension',
        [suspectEntity.instance.codeableConcept.coding] NVARCHAR(MAX)       '$.instance.codeableConcept.coding',
        [suspectEntity.instance.codeableConcept.text] NVARCHAR(4000)      '$.instance.codeableConcept.text',
        [suspectEntity.instance.reference.id] NVARCHAR(100)       '$.instance.reference.id',
        [suspectEntity.instance.reference.extension] NVARCHAR(MAX)       '$.instance.reference.extension',
        [suspectEntity.instance.reference.reference] NVARCHAR(4000)      '$.instance.reference.reference',
        [suspectEntity.instance.reference.type] VARCHAR(256)        '$.instance.reference.type',
        [suspectEntity.instance.reference.identifier] NVARCHAR(MAX)       '$.instance.reference.identifier',
        [suspectEntity.instance.reference.display] NVARCHAR(4000)      '$.instance.reference.display'
    ) j

GO

CREATE VIEW fhir.AdverseEventContributingFactor AS
SELECT
    [id],
    [contributingFactor.JSON],
    [contributingFactor.id],
    [contributingFactor.extension],
    [contributingFactor.modifierExtension],
    [contributingFactor.item.reference.id],
    [contributingFactor.item.reference.extension],
    [contributingFactor.item.reference.reference],
    [contributingFactor.item.reference.type],
    [contributingFactor.item.reference.identifier],
    [contributingFactor.item.reference.display],
    [contributingFactor.item.codeableConcept.id],
    [contributingFactor.item.codeableConcept.extension],
    [contributingFactor.item.codeableConcept.coding],
    [contributingFactor.item.codeableConcept.text]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contributingFactor.JSON]  VARCHAR(MAX) '$.contributingFactor'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contributingFactor.JSON]) with (
        [contributingFactor.id]        NVARCHAR(100)       '$.id',
        [contributingFactor.extension] NVARCHAR(MAX)       '$.extension',
        [contributingFactor.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [contributingFactor.item.reference.id] NVARCHAR(100)       '$.item.reference.id',
        [contributingFactor.item.reference.extension] NVARCHAR(MAX)       '$.item.reference.extension',
        [contributingFactor.item.reference.reference] NVARCHAR(4000)      '$.item.reference.reference',
        [contributingFactor.item.reference.type] VARCHAR(256)        '$.item.reference.type',
        [contributingFactor.item.reference.identifier] NVARCHAR(MAX)       '$.item.reference.identifier',
        [contributingFactor.item.reference.display] NVARCHAR(4000)      '$.item.reference.display',
        [contributingFactor.item.codeableConcept.id] NVARCHAR(100)       '$.item.codeableConcept.id',
        [contributingFactor.item.codeableConcept.extension] NVARCHAR(MAX)       '$.item.codeableConcept.extension',
        [contributingFactor.item.codeableConcept.coding] NVARCHAR(MAX)       '$.item.codeableConcept.coding',
        [contributingFactor.item.codeableConcept.text] NVARCHAR(4000)      '$.item.codeableConcept.text'
    ) j

GO

CREATE VIEW fhir.AdverseEventPreventiveAction AS
SELECT
    [id],
    [preventiveAction.JSON],
    [preventiveAction.id],
    [preventiveAction.extension],
    [preventiveAction.modifierExtension],
    [preventiveAction.item.reference.id],
    [preventiveAction.item.reference.extension],
    [preventiveAction.item.reference.reference],
    [preventiveAction.item.reference.type],
    [preventiveAction.item.reference.identifier],
    [preventiveAction.item.reference.display],
    [preventiveAction.item.codeableConcept.id],
    [preventiveAction.item.codeableConcept.extension],
    [preventiveAction.item.codeableConcept.coding],
    [preventiveAction.item.codeableConcept.text]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [preventiveAction.JSON]  VARCHAR(MAX) '$.preventiveAction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[preventiveAction.JSON]) with (
        [preventiveAction.id]          NVARCHAR(100)       '$.id',
        [preventiveAction.extension]   NVARCHAR(MAX)       '$.extension',
        [preventiveAction.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [preventiveAction.item.reference.id] NVARCHAR(100)       '$.item.reference.id',
        [preventiveAction.item.reference.extension] NVARCHAR(MAX)       '$.item.reference.extension',
        [preventiveAction.item.reference.reference] NVARCHAR(4000)      '$.item.reference.reference',
        [preventiveAction.item.reference.type] VARCHAR(256)        '$.item.reference.type',
        [preventiveAction.item.reference.identifier] NVARCHAR(MAX)       '$.item.reference.identifier',
        [preventiveAction.item.reference.display] NVARCHAR(4000)      '$.item.reference.display',
        [preventiveAction.item.codeableConcept.id] NVARCHAR(100)       '$.item.codeableConcept.id',
        [preventiveAction.item.codeableConcept.extension] NVARCHAR(MAX)       '$.item.codeableConcept.extension',
        [preventiveAction.item.codeableConcept.coding] NVARCHAR(MAX)       '$.item.codeableConcept.coding',
        [preventiveAction.item.codeableConcept.text] NVARCHAR(4000)      '$.item.codeableConcept.text'
    ) j

GO

CREATE VIEW fhir.AdverseEventMitigatingAction AS
SELECT
    [id],
    [mitigatingAction.JSON],
    [mitigatingAction.id],
    [mitigatingAction.extension],
    [mitigatingAction.modifierExtension],
    [mitigatingAction.item.reference.id],
    [mitigatingAction.item.reference.extension],
    [mitigatingAction.item.reference.reference],
    [mitigatingAction.item.reference.type],
    [mitigatingAction.item.reference.identifier],
    [mitigatingAction.item.reference.display],
    [mitigatingAction.item.codeableConcept.id],
    [mitigatingAction.item.codeableConcept.extension],
    [mitigatingAction.item.codeableConcept.coding],
    [mitigatingAction.item.codeableConcept.text]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [mitigatingAction.JSON]  VARCHAR(MAX) '$.mitigatingAction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[mitigatingAction.JSON]) with (
        [mitigatingAction.id]          NVARCHAR(100)       '$.id',
        [mitigatingAction.extension]   NVARCHAR(MAX)       '$.extension',
        [mitigatingAction.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [mitigatingAction.item.reference.id] NVARCHAR(100)       '$.item.reference.id',
        [mitigatingAction.item.reference.extension] NVARCHAR(MAX)       '$.item.reference.extension',
        [mitigatingAction.item.reference.reference] NVARCHAR(4000)      '$.item.reference.reference',
        [mitigatingAction.item.reference.type] VARCHAR(256)        '$.item.reference.type',
        [mitigatingAction.item.reference.identifier] NVARCHAR(MAX)       '$.item.reference.identifier',
        [mitigatingAction.item.reference.display] NVARCHAR(4000)      '$.item.reference.display',
        [mitigatingAction.item.codeableConcept.id] NVARCHAR(100)       '$.item.codeableConcept.id',
        [mitigatingAction.item.codeableConcept.extension] NVARCHAR(MAX)       '$.item.codeableConcept.extension',
        [mitigatingAction.item.codeableConcept.coding] NVARCHAR(MAX)       '$.item.codeableConcept.coding',
        [mitigatingAction.item.codeableConcept.text] NVARCHAR(4000)      '$.item.codeableConcept.text'
    ) j

GO

CREATE VIEW fhir.AdverseEventSupportingInfo AS
SELECT
    [id],
    [supportingInfo.JSON],
    [supportingInfo.id],
    [supportingInfo.extension],
    [supportingInfo.modifierExtension],
    [supportingInfo.item.reference.id],
    [supportingInfo.item.reference.extension],
    [supportingInfo.item.reference.reference],
    [supportingInfo.item.reference.type],
    [supportingInfo.item.reference.identifier],
    [supportingInfo.item.reference.display],
    [supportingInfo.item.codeableConcept.id],
    [supportingInfo.item.codeableConcept.extension],
    [supportingInfo.item.codeableConcept.coding],
    [supportingInfo.item.codeableConcept.text]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [supportingInfo.JSON]  VARCHAR(MAX) '$.supportingInfo'
    ) AS rowset
    CROSS APPLY openjson (rowset.[supportingInfo.JSON]) with (
        [supportingInfo.id]            NVARCHAR(100)       '$.id',
        [supportingInfo.extension]     NVARCHAR(MAX)       '$.extension',
        [supportingInfo.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [supportingInfo.item.reference.id] NVARCHAR(100)       '$.item.reference.id',
        [supportingInfo.item.reference.extension] NVARCHAR(MAX)       '$.item.reference.extension',
        [supportingInfo.item.reference.reference] NVARCHAR(4000)      '$.item.reference.reference',
        [supportingInfo.item.reference.type] VARCHAR(256)        '$.item.reference.type',
        [supportingInfo.item.reference.identifier] NVARCHAR(MAX)       '$.item.reference.identifier',
        [supportingInfo.item.reference.display] NVARCHAR(4000)      '$.item.reference.display',
        [supportingInfo.item.codeableConcept.id] NVARCHAR(100)       '$.item.codeableConcept.id',
        [supportingInfo.item.codeableConcept.extension] NVARCHAR(MAX)       '$.item.codeableConcept.extension',
        [supportingInfo.item.codeableConcept.coding] NVARCHAR(MAX)       '$.item.codeableConcept.coding',
        [supportingInfo.item.codeableConcept.text] NVARCHAR(4000)      '$.item.codeableConcept.text'
    ) j

GO

CREATE VIEW fhir.AdverseEventStudy AS
SELECT
    [id],
    [study.JSON],
    [study.id],
    [study.extension],
    [study.reference],
    [study.type],
    [study.identifier.id],
    [study.identifier.extension],
    [study.identifier.use],
    [study.identifier.type],
    [study.identifier.system],
    [study.identifier.value],
    [study.identifier.period],
    [study.identifier.assigner],
    [study.display]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [study.JSON]  VARCHAR(MAX) '$.study'
    ) AS rowset
    CROSS APPLY openjson (rowset.[study.JSON]) with (
        [study.id]                     NVARCHAR(100)       '$.id',
        [study.extension]              NVARCHAR(MAX)       '$.extension',
        [study.reference]              NVARCHAR(4000)      '$.reference',
        [study.type]                   VARCHAR(256)        '$.type',
        [study.identifier.id]          NVARCHAR(100)       '$.identifier.id',
        [study.identifier.extension]   NVARCHAR(MAX)       '$.identifier.extension',
        [study.identifier.use]         NVARCHAR(64)        '$.identifier.use',
        [study.identifier.type]        NVARCHAR(MAX)       '$.identifier.type',
        [study.identifier.system]      VARCHAR(256)        '$.identifier.system',
        [study.identifier.value]       NVARCHAR(4000)      '$.identifier.value',
        [study.identifier.period]      NVARCHAR(MAX)       '$.identifier.period',
        [study.identifier.assigner]    NVARCHAR(MAX)       '$.identifier.assigner',
        [study.display]                NVARCHAR(4000)      '$.display'
    ) j
