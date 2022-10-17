CREATE EXTERNAL TABLE [fhir].[Goal] (
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
    [lifecycleStatus] NVARCHAR(64),
    [achievementStatus.id] NVARCHAR(100),
    [achievementStatus.extension] NVARCHAR(MAX),
    [achievementStatus.coding] VARCHAR(MAX),
    [achievementStatus.text] NVARCHAR(4000),
    [category] VARCHAR(MAX),
    [priority.id] NVARCHAR(100),
    [priority.extension] NVARCHAR(MAX),
    [priority.coding] VARCHAR(MAX),
    [priority.text] NVARCHAR(4000),
    [description.id] NVARCHAR(100),
    [description.extension] NVARCHAR(MAX),
    [description.coding] VARCHAR(MAX),
    [description.text] NVARCHAR(4000),
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
    [target] VARCHAR(MAX),
    [statusDate] VARCHAR(64),
    [statusReason] NVARCHAR(4000),
    [expressedBy.id] NVARCHAR(100),
    [expressedBy.extension] NVARCHAR(MAX),
    [expressedBy.reference] NVARCHAR(4000),
    [expressedBy.type] VARCHAR(256),
    [expressedBy.identifier.id] NVARCHAR(100),
    [expressedBy.identifier.extension] NVARCHAR(MAX),
    [expressedBy.identifier.use] NVARCHAR(64),
    [expressedBy.identifier.type] NVARCHAR(MAX),
    [expressedBy.identifier.system] VARCHAR(256),
    [expressedBy.identifier.value] NVARCHAR(4000),
    [expressedBy.identifier.period] NVARCHAR(MAX),
    [expressedBy.identifier.assigner] NVARCHAR(MAX),
    [expressedBy.display] NVARCHAR(4000),
    [addresses] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [outcomeCode] VARCHAR(MAX),
    [outcomeReference] VARCHAR(MAX),
    [start.date] VARCHAR(64),
    [start.codeableConcept.id] NVARCHAR(100),
    [start.codeableConcept.extension] NVARCHAR(MAX),
    [start.codeableConcept.coding] VARCHAR(MAX),
    [start.codeableConcept.text] NVARCHAR(4000),
) WITH (
    LOCATION='/Goal/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.GoalIdentifier AS
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
        BULK 'Goal/**',
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

CREATE VIEW fhir.GoalCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'Goal/**',
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

CREATE VIEW fhir.GoalTarget AS
SELECT
    [id],
    [target.JSON],
    [target.id],
    [target.extension],
    [target.modifierExtension],
    [target.measure.id],
    [target.measure.extension],
    [target.measure.coding],
    [target.measure.text],
    [target.detail.quantity.id],
    [target.detail.quantity.extension],
    [target.detail.quantity.value],
    [target.detail.quantity.comparator],
    [target.detail.quantity.unit],
    [target.detail.quantity.system],
    [target.detail.quantity.code],
    [target.detail.range.id],
    [target.detail.range.extension],
    [target.detail.range.low],
    [target.detail.range.high],
    [target.detail.codeableConcept.id],
    [target.detail.codeableConcept.extension],
    [target.detail.codeableConcept.coding],
    [target.detail.codeableConcept.text],
    [target.detail.string],
    [target.detail.boolean],
    [target.detail.integer],
    [target.detail.ratio.id],
    [target.detail.ratio.extension],
    [target.detail.ratio.numerator],
    [target.detail.ratio.denominator],
    [target.due.date],
    [target.due.duration.id],
    [target.due.duration.extension],
    [target.due.duration.value],
    [target.due.duration.comparator],
    [target.due.duration.unit],
    [target.due.duration.system],
    [target.due.duration.code]
FROM openrowset (
        BULK 'Goal/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [target.JSON]  VARCHAR(MAX) '$.target'
    ) AS rowset
    CROSS APPLY openjson (rowset.[target.JSON]) with (
        [target.id]                    NVARCHAR(100)       '$.id',
        [target.extension]             NVARCHAR(MAX)       '$.extension',
        [target.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [target.measure.id]            NVARCHAR(100)       '$.measure.id',
        [target.measure.extension]     NVARCHAR(MAX)       '$.measure.extension',
        [target.measure.coding]        NVARCHAR(MAX)       '$.measure.coding',
        [target.measure.text]          NVARCHAR(4000)      '$.measure.text',
        [target.detail.quantity.id]    NVARCHAR(100)       '$.detail.quantity.id',
        [target.detail.quantity.extension] NVARCHAR(MAX)       '$.detail.quantity.extension',
        [target.detail.quantity.value] float               '$.detail.quantity.value',
        [target.detail.quantity.comparator] NVARCHAR(64)        '$.detail.quantity.comparator',
        [target.detail.quantity.unit]  NVARCHAR(100)       '$.detail.quantity.unit',
        [target.detail.quantity.system] VARCHAR(256)        '$.detail.quantity.system',
        [target.detail.quantity.code]  NVARCHAR(4000)      '$.detail.quantity.code',
        [target.detail.range.id]       NVARCHAR(100)       '$.detail.range.id',
        [target.detail.range.extension] NVARCHAR(MAX)       '$.detail.range.extension',
        [target.detail.range.low]      NVARCHAR(MAX)       '$.detail.range.low',
        [target.detail.range.high]     NVARCHAR(MAX)       '$.detail.range.high',
        [target.detail.codeableConcept.id] NVARCHAR(100)       '$.detail.codeableConcept.id',
        [target.detail.codeableConcept.extension] NVARCHAR(MAX)       '$.detail.codeableConcept.extension',
        [target.detail.codeableConcept.coding] NVARCHAR(MAX)       '$.detail.codeableConcept.coding',
        [target.detail.codeableConcept.text] NVARCHAR(4000)      '$.detail.codeableConcept.text',
        [target.detail.string]         NVARCHAR(4000)      '$.detail.string',
        [target.detail.boolean]        bit                 '$.detail.boolean',
        [target.detail.integer]        bigint              '$.detail.integer',
        [target.detail.ratio.id]       NVARCHAR(100)       '$.detail.ratio.id',
        [target.detail.ratio.extension] NVARCHAR(MAX)       '$.detail.ratio.extension',
        [target.detail.ratio.numerator] NVARCHAR(MAX)       '$.detail.ratio.numerator',
        [target.detail.ratio.denominator] NVARCHAR(MAX)       '$.detail.ratio.denominator',
        [target.due.date]              VARCHAR(64)         '$.due.date',
        [target.due.duration.id]       NVARCHAR(100)       '$.due.duration.id',
        [target.due.duration.extension] NVARCHAR(MAX)       '$.due.duration.extension',
        [target.due.duration.value]    float               '$.due.duration.value',
        [target.due.duration.comparator] NVARCHAR(64)        '$.due.duration.comparator',
        [target.due.duration.unit]     NVARCHAR(100)       '$.due.duration.unit',
        [target.due.duration.system]   VARCHAR(256)        '$.due.duration.system',
        [target.due.duration.code]     NVARCHAR(4000)      '$.due.duration.code'
    ) j

GO

CREATE VIEW fhir.GoalAddresses AS
SELECT
    [id],
    [addresses.JSON],
    [addresses.id],
    [addresses.extension],
    [addresses.reference],
    [addresses.type],
    [addresses.identifier.id],
    [addresses.identifier.extension],
    [addresses.identifier.use],
    [addresses.identifier.type],
    [addresses.identifier.system],
    [addresses.identifier.value],
    [addresses.identifier.period],
    [addresses.identifier.assigner],
    [addresses.display]
FROM openrowset (
        BULK 'Goal/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [addresses.JSON]  VARCHAR(MAX) '$.addresses'
    ) AS rowset
    CROSS APPLY openjson (rowset.[addresses.JSON]) with (
        [addresses.id]                 NVARCHAR(100)       '$.id',
        [addresses.extension]          NVARCHAR(MAX)       '$.extension',
        [addresses.reference]          NVARCHAR(4000)      '$.reference',
        [addresses.type]               VARCHAR(256)        '$.type',
        [addresses.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [addresses.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [addresses.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [addresses.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [addresses.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [addresses.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [addresses.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [addresses.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [addresses.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.GoalNote AS
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
        BULK 'Goal/**',
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

CREATE VIEW fhir.GoalOutcomeCode AS
SELECT
    [id],
    [outcomeCode.JSON],
    [outcomeCode.id],
    [outcomeCode.extension],
    [outcomeCode.coding],
    [outcomeCode.text]
FROM openrowset (
        BULK 'Goal/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [outcomeCode.JSON]  VARCHAR(MAX) '$.outcomeCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[outcomeCode.JSON]) with (
        [outcomeCode.id]               NVARCHAR(100)       '$.id',
        [outcomeCode.extension]        NVARCHAR(MAX)       '$.extension',
        [outcomeCode.coding]           NVARCHAR(MAX)       '$.coding' AS JSON,
        [outcomeCode.text]             NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.GoalOutcomeReference AS
SELECT
    [id],
    [outcomeReference.JSON],
    [outcomeReference.id],
    [outcomeReference.extension],
    [outcomeReference.reference],
    [outcomeReference.type],
    [outcomeReference.identifier.id],
    [outcomeReference.identifier.extension],
    [outcomeReference.identifier.use],
    [outcomeReference.identifier.type],
    [outcomeReference.identifier.system],
    [outcomeReference.identifier.value],
    [outcomeReference.identifier.period],
    [outcomeReference.identifier.assigner],
    [outcomeReference.display]
FROM openrowset (
        BULK 'Goal/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [outcomeReference.JSON]  VARCHAR(MAX) '$.outcomeReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[outcomeReference.JSON]) with (
        [outcomeReference.id]          NVARCHAR(100)       '$.id',
        [outcomeReference.extension]   NVARCHAR(MAX)       '$.extension',
        [outcomeReference.reference]   NVARCHAR(4000)      '$.reference',
        [outcomeReference.type]        VARCHAR(256)        '$.type',
        [outcomeReference.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [outcomeReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [outcomeReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [outcomeReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [outcomeReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [outcomeReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [outcomeReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [outcomeReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [outcomeReference.display]     NVARCHAR(4000)      '$.display'
    ) j
