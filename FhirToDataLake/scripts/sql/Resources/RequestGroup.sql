CREATE EXTERNAL TABLE [fhir].[RequestGroup] (
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
    [instantiatesCanonical] VARCHAR(MAX),
    [instantiatesUri] VARCHAR(MAX),
    [basedOn] VARCHAR(MAX),
    [replaces] VARCHAR(MAX),
    [groupIdentifier.id] NVARCHAR(100),
    [groupIdentifier.extension] NVARCHAR(MAX),
    [groupIdentifier.use] NVARCHAR(64),
    [groupIdentifier.type.id] NVARCHAR(100),
    [groupIdentifier.type.extension] NVARCHAR(MAX),
    [groupIdentifier.type.coding] NVARCHAR(MAX),
    [groupIdentifier.type.text] NVARCHAR(4000),
    [groupIdentifier.system] VARCHAR(256),
    [groupIdentifier.value] NVARCHAR(4000),
    [groupIdentifier.period.id] NVARCHAR(100),
    [groupIdentifier.period.extension] NVARCHAR(MAX),
    [groupIdentifier.period.start] VARCHAR(64),
    [groupIdentifier.period.end] VARCHAR(64),
    [groupIdentifier.assigner.id] NVARCHAR(100),
    [groupIdentifier.assigner.extension] NVARCHAR(MAX),
    [groupIdentifier.assigner.reference] NVARCHAR(4000),
    [groupIdentifier.assigner.type] VARCHAR(256),
    [groupIdentifier.assigner.identifier] NVARCHAR(MAX),
    [groupIdentifier.assigner.display] NVARCHAR(4000),
    [status] NVARCHAR(100),
    [intent] NVARCHAR(100),
    [priority] NVARCHAR(100),
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
    [authoredOn] VARCHAR(64),
    [author.id] NVARCHAR(100),
    [author.extension] NVARCHAR(MAX),
    [author.reference] NVARCHAR(4000),
    [author.type] VARCHAR(256),
    [author.identifier.id] NVARCHAR(100),
    [author.identifier.extension] NVARCHAR(MAX),
    [author.identifier.use] NVARCHAR(64),
    [author.identifier.type] NVARCHAR(MAX),
    [author.identifier.system] VARCHAR(256),
    [author.identifier.value] NVARCHAR(4000),
    [author.identifier.period] NVARCHAR(MAX),
    [author.identifier.assigner] NVARCHAR(MAX),
    [author.display] NVARCHAR(4000),
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [action] VARCHAR(MAX),
) WITH (
    LOCATION='/RequestGroup/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.RequestGroupIdentifier AS
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
        BULK 'RequestGroup/**',
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

CREATE VIEW fhir.RequestGroupInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'RequestGroup/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesCanonical.JSON]  VARCHAR(MAX) '$.instantiatesCanonical'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesCanonical.JSON]) with (
        [instantiatesCanonical]        NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.RequestGroupInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'RequestGroup/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesUri.JSON]  VARCHAR(MAX) '$.instantiatesUri'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesUri.JSON]) with (
        [instantiatesUri]              NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.RequestGroupBasedOn AS
SELECT
    [id],
    [basedOn.JSON],
    [basedOn.id],
    [basedOn.extension],
    [basedOn.reference],
    [basedOn.type],
    [basedOn.identifier.id],
    [basedOn.identifier.extension],
    [basedOn.identifier.use],
    [basedOn.identifier.type],
    [basedOn.identifier.system],
    [basedOn.identifier.value],
    [basedOn.identifier.period],
    [basedOn.identifier.assigner],
    [basedOn.display]
FROM openrowset (
        BULK 'RequestGroup/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [basedOn.JSON]  VARCHAR(MAX) '$.basedOn'
    ) AS rowset
    CROSS APPLY openjson (rowset.[basedOn.JSON]) with (
        [basedOn.id]                   NVARCHAR(100)       '$.id',
        [basedOn.extension]            NVARCHAR(MAX)       '$.extension',
        [basedOn.reference]            NVARCHAR(4000)      '$.reference',
        [basedOn.type]                 VARCHAR(256)        '$.type',
        [basedOn.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [basedOn.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [basedOn.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [basedOn.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [basedOn.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [basedOn.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [basedOn.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [basedOn.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [basedOn.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.RequestGroupReplaces AS
SELECT
    [id],
    [replaces.JSON],
    [replaces.id],
    [replaces.extension],
    [replaces.reference],
    [replaces.type],
    [replaces.identifier.id],
    [replaces.identifier.extension],
    [replaces.identifier.use],
    [replaces.identifier.type],
    [replaces.identifier.system],
    [replaces.identifier.value],
    [replaces.identifier.period],
    [replaces.identifier.assigner],
    [replaces.display]
FROM openrowset (
        BULK 'RequestGroup/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [replaces.JSON]  VARCHAR(MAX) '$.replaces'
    ) AS rowset
    CROSS APPLY openjson (rowset.[replaces.JSON]) with (
        [replaces.id]                  NVARCHAR(100)       '$.id',
        [replaces.extension]           NVARCHAR(MAX)       '$.extension',
        [replaces.reference]           NVARCHAR(4000)      '$.reference',
        [replaces.type]                VARCHAR(256)        '$.type',
        [replaces.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [replaces.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [replaces.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [replaces.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [replaces.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [replaces.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [replaces.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [replaces.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [replaces.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.RequestGroupReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'RequestGroup/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonCode.JSON]  VARCHAR(MAX) '$.reasonCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonCode.JSON]) with (
        [reasonCode.id]                NVARCHAR(100)       '$.id',
        [reasonCode.extension]         NVARCHAR(MAX)       '$.extension',
        [reasonCode.coding]            NVARCHAR(MAX)       '$.coding' AS JSON,
        [reasonCode.text]              NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.RequestGroupReasonReference AS
SELECT
    [id],
    [reasonReference.JSON],
    [reasonReference.id],
    [reasonReference.extension],
    [reasonReference.reference],
    [reasonReference.type],
    [reasonReference.identifier.id],
    [reasonReference.identifier.extension],
    [reasonReference.identifier.use],
    [reasonReference.identifier.type],
    [reasonReference.identifier.system],
    [reasonReference.identifier.value],
    [reasonReference.identifier.period],
    [reasonReference.identifier.assigner],
    [reasonReference.display]
FROM openrowset (
        BULK 'RequestGroup/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonReference.JSON]  VARCHAR(MAX) '$.reasonReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonReference.JSON]) with (
        [reasonReference.id]           NVARCHAR(100)       '$.id',
        [reasonReference.extension]    NVARCHAR(MAX)       '$.extension',
        [reasonReference.reference]    NVARCHAR(4000)      '$.reference',
        [reasonReference.type]         VARCHAR(256)        '$.type',
        [reasonReference.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [reasonReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [reasonReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [reasonReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [reasonReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [reasonReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [reasonReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [reasonReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [reasonReference.display]      NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.RequestGroupNote AS
SELECT
    [id],
    [note.JSON],
    [note.id],
    [note.extension],
    [note.time],
    [note.text],
    [note.author.reference.id],
    [note.author.reference.extension],
    [note.author.reference.reference],
    [note.author.reference.type],
    [note.author.reference.identifier],
    [note.author.reference.display],
    [note.author.string]
FROM openrowset (
        BULK 'RequestGroup/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [note.JSON]  VARCHAR(MAX) '$.note'
    ) AS rowset
    CROSS APPLY openjson (rowset.[note.JSON]) with (
        [note.id]                      NVARCHAR(100)       '$.id',
        [note.extension]               NVARCHAR(MAX)       '$.extension',
        [note.time]                    VARCHAR(64)         '$.time',
        [note.text]                    NVARCHAR(MAX)       '$.text',
        [note.author.reference.id]     NVARCHAR(100)       '$.author.reference.id',
        [note.author.reference.extension] NVARCHAR(MAX)       '$.author.reference.extension',
        [note.author.reference.reference] NVARCHAR(4000)      '$.author.reference.reference',
        [note.author.reference.type]   VARCHAR(256)        '$.author.reference.type',
        [note.author.reference.identifier] NVARCHAR(MAX)       '$.author.reference.identifier',
        [note.author.reference.display] NVARCHAR(4000)      '$.author.reference.display',
        [note.author.string]           NVARCHAR(4000)      '$.author.string'
    ) j

GO

CREATE VIEW fhir.RequestGroupAction AS
SELECT
    [id],
    [action.JSON],
    [action.id],
    [action.extension],
    [action.modifierExtension],
    [action.prefix],
    [action.title],
    [action.description],
    [action.textEquivalent],
    [action.priority],
    [action.code],
    [action.documentation],
    [action.condition],
    [action.relatedAction],
    [action.participant],
    [action.type.id],
    [action.type.extension],
    [action.type.coding],
    [action.type.text],
    [action.groupingBehavior],
    [action.selectionBehavior],
    [action.requiredBehavior],
    [action.precheckBehavior],
    [action.cardinalityBehavior],
    [action.resource.id],
    [action.resource.extension],
    [action.resource.reference],
    [action.resource.type],
    [action.resource.identifier],
    [action.resource.display],
    [action.action],
    [action.timing.dateTime],
    [action.timing.age.id],
    [action.timing.age.extension],
    [action.timing.age.value],
    [action.timing.age.comparator],
    [action.timing.age.unit],
    [action.timing.age.system],
    [action.timing.age.code],
    [action.timing.period.id],
    [action.timing.period.extension],
    [action.timing.period.start],
    [action.timing.period.end],
    [action.timing.duration.id],
    [action.timing.duration.extension],
    [action.timing.duration.value],
    [action.timing.duration.comparator],
    [action.timing.duration.unit],
    [action.timing.duration.system],
    [action.timing.duration.code],
    [action.timing.range.id],
    [action.timing.range.extension],
    [action.timing.range.low],
    [action.timing.range.high],
    [action.timing.timing.id],
    [action.timing.timing.extension],
    [action.timing.timing.modifierExtension],
    [action.timing.timing.event],
    [action.timing.timing.repeat],
    [action.timing.timing.code]
FROM openrowset (
        BULK 'RequestGroup/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [action.JSON]  VARCHAR(MAX) '$.action'
    ) AS rowset
    CROSS APPLY openjson (rowset.[action.JSON]) with (
        [action.id]                    NVARCHAR(100)       '$.id',
        [action.extension]             NVARCHAR(MAX)       '$.extension',
        [action.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [action.prefix]                NVARCHAR(500)       '$.prefix',
        [action.title]                 NVARCHAR(4000)      '$.title',
        [action.description]           NVARCHAR(4000)      '$.description',
        [action.textEquivalent]        NVARCHAR(4000)      '$.textEquivalent',
        [action.priority]              NVARCHAR(100)       '$.priority',
        [action.code]                  NVARCHAR(MAX)       '$.code' AS JSON,
        [action.documentation]         NVARCHAR(MAX)       '$.documentation' AS JSON,
        [action.condition]             NVARCHAR(MAX)       '$.condition' AS JSON,
        [action.relatedAction]         NVARCHAR(MAX)       '$.relatedAction' AS JSON,
        [action.participant]           NVARCHAR(MAX)       '$.participant' AS JSON,
        [action.type.id]               NVARCHAR(100)       '$.type.id',
        [action.type.extension]        NVARCHAR(MAX)       '$.type.extension',
        [action.type.coding]           NVARCHAR(MAX)       '$.type.coding',
        [action.type.text]             NVARCHAR(4000)      '$.type.text',
        [action.groupingBehavior]      NVARCHAR(100)       '$.groupingBehavior',
        [action.selectionBehavior]     NVARCHAR(100)       '$.selectionBehavior',
        [action.requiredBehavior]      NVARCHAR(100)       '$.requiredBehavior',
        [action.precheckBehavior]      NVARCHAR(100)       '$.precheckBehavior',
        [action.cardinalityBehavior]   NVARCHAR(100)       '$.cardinalityBehavior',
        [action.resource.id]           NVARCHAR(100)       '$.resource.id',
        [action.resource.extension]    NVARCHAR(MAX)       '$.resource.extension',
        [action.resource.reference]    NVARCHAR(4000)      '$.resource.reference',
        [action.resource.type]         VARCHAR(256)        '$.resource.type',
        [action.resource.identifier]   NVARCHAR(MAX)       '$.resource.identifier',
        [action.resource.display]      NVARCHAR(4000)      '$.resource.display',
        [action.action]                NVARCHAR(MAX)       '$.action' AS JSON,
        [action.timing.dateTime]       VARCHAR(64)         '$.timing.dateTime',
        [action.timing.age.id]         NVARCHAR(100)       '$.timing.age.id',
        [action.timing.age.extension]  NVARCHAR(MAX)       '$.timing.age.extension',
        [action.timing.age.value]      float               '$.timing.age.value',
        [action.timing.age.comparator] NVARCHAR(64)        '$.timing.age.comparator',
        [action.timing.age.unit]       NVARCHAR(100)       '$.timing.age.unit',
        [action.timing.age.system]     VARCHAR(256)        '$.timing.age.system',
        [action.timing.age.code]       NVARCHAR(4000)      '$.timing.age.code',
        [action.timing.period.id]      NVARCHAR(100)       '$.timing.period.id',
        [action.timing.period.extension] NVARCHAR(MAX)       '$.timing.period.extension',
        [action.timing.period.start]   VARCHAR(64)         '$.timing.period.start',
        [action.timing.period.end]     VARCHAR(64)         '$.timing.period.end',
        [action.timing.duration.id]    NVARCHAR(100)       '$.timing.duration.id',
        [action.timing.duration.extension] NVARCHAR(MAX)       '$.timing.duration.extension',
        [action.timing.duration.value] float               '$.timing.duration.value',
        [action.timing.duration.comparator] NVARCHAR(64)        '$.timing.duration.comparator',
        [action.timing.duration.unit]  NVARCHAR(100)       '$.timing.duration.unit',
        [action.timing.duration.system] VARCHAR(256)        '$.timing.duration.system',
        [action.timing.duration.code]  NVARCHAR(4000)      '$.timing.duration.code',
        [action.timing.range.id]       NVARCHAR(100)       '$.timing.range.id',
        [action.timing.range.extension] NVARCHAR(MAX)       '$.timing.range.extension',
        [action.timing.range.low]      NVARCHAR(MAX)       '$.timing.range.low',
        [action.timing.range.high]     NVARCHAR(MAX)       '$.timing.range.high',
        [action.timing.timing.id]      NVARCHAR(100)       '$.timing.timing.id',
        [action.timing.timing.extension] NVARCHAR(MAX)       '$.timing.timing.extension',
        [action.timing.timing.modifierExtension] NVARCHAR(MAX)       '$.timing.timing.modifierExtension',
        [action.timing.timing.event]   NVARCHAR(MAX)       '$.timing.timing.event',
        [action.timing.timing.repeat]  NVARCHAR(MAX)       '$.timing.timing.repeat',
        [action.timing.timing.code]    NVARCHAR(MAX)       '$.timing.timing.code'
    ) j
