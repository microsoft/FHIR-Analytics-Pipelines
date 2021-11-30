CREATE EXTERNAL TABLE [fhir].[Goal] (
    [resourceType] NVARCHAR(4000),
    [id] VARCHAR(64),
    [meta.id] NVARCHAR(4000),
    [meta.extension] NVARCHAR(MAX),
    [meta.versionId] VARCHAR(64),
    [meta.lastUpdated] VARCHAR(30),
    [meta.source] VARCHAR(256),
    [meta.profile] VARCHAR(MAX),
    [meta.security] VARCHAR(MAX),
    [meta.tag] VARCHAR(MAX),
    [implicitRules] VARCHAR(256),
    [language] NVARCHAR(4000),
    [text.id] NVARCHAR(4000),
    [text.extension] NVARCHAR(MAX),
    [text.status] NVARCHAR(64),
    [text.div] NVARCHAR(MAX),
    [extension] NVARCHAR(MAX),
    [modifierExtension] NVARCHAR(MAX),
    [identifier] VARCHAR(MAX),
    [lifecycleStatus] NVARCHAR(64),
    [achievementStatus.id] NVARCHAR(4000),
    [achievementStatus.extension] NVARCHAR(MAX),
    [achievementStatus.coding] VARCHAR(MAX),
    [achievementStatus.text] NVARCHAR(4000),
    [category] VARCHAR(MAX),
    [priority.id] NVARCHAR(4000),
    [priority.extension] NVARCHAR(MAX),
    [priority.coding] VARCHAR(MAX),
    [priority.text] NVARCHAR(4000),
    [description.id] NVARCHAR(4000),
    [description.extension] NVARCHAR(MAX),
    [description.coding] VARCHAR(MAX),
    [description.text] NVARCHAR(4000),
    [subject.id] NVARCHAR(4000),
    [subject.extension] NVARCHAR(MAX),
    [subject.reference] NVARCHAR(4000),
    [subject.type] VARCHAR(256),
    [subject.identifier.id] NVARCHAR(4000),
    [subject.identifier.extension] NVARCHAR(MAX),
    [subject.identifier.use] NVARCHAR(64),
    [subject.identifier.type] NVARCHAR(MAX),
    [subject.identifier.system] VARCHAR(256),
    [subject.identifier.value] NVARCHAR(4000),
    [subject.identifier.period] NVARCHAR(MAX),
    [subject.identifier.assigner] NVARCHAR(MAX),
    [subject.display] NVARCHAR(4000),
    [target] VARCHAR(MAX),
    [statusDate] VARCHAR(10),
    [statusReason] NVARCHAR(4000),
    [expressedBy.id] NVARCHAR(4000),
    [expressedBy.extension] NVARCHAR(MAX),
    [expressedBy.reference] NVARCHAR(4000),
    [expressedBy.type] VARCHAR(256),
    [expressedBy.identifier.id] NVARCHAR(4000),
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
    [start.date] VARCHAR(10),
    [start.CodeableConcept.id] NVARCHAR(4000),
    [start.CodeableConcept.extension] NVARCHAR(MAX),
    [start.CodeableConcept.coding] VARCHAR(MAX),
    [start.CodeableConcept.text] NVARCHAR(4000),
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
        [identifier.id]                NVARCHAR(4000)      '$.id',
        [identifier.extension]         NVARCHAR(MAX)       '$.extension',
        [identifier.use]               NVARCHAR(64)        '$.use',
        [identifier.type.id]           NVARCHAR(4000)      '$.type.id',
        [identifier.type.extension]    NVARCHAR(MAX)       '$.type.extension',
        [identifier.type.coding]       NVARCHAR(MAX)       '$.type.coding',
        [identifier.type.text]         NVARCHAR(4000)      '$.type.text',
        [identifier.system]            VARCHAR(256)        '$.system',
        [identifier.value]             NVARCHAR(4000)      '$.value',
        [identifier.period.id]         NVARCHAR(4000)      '$.period.id',
        [identifier.period.extension]  NVARCHAR(MAX)       '$.period.extension',
        [identifier.period.start]      VARCHAR(30)         '$.period.start',
        [identifier.period.end]        VARCHAR(30)         '$.period.end',
        [identifier.assigner.id]       NVARCHAR(4000)      '$.assigner.id',
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
        [category.id]                  NVARCHAR(4000)      '$.id',
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
    [target.detail.Quantity.id],
    [target.detail.Quantity.extension],
    [target.detail.Quantity.value],
    [target.detail.Quantity.comparator],
    [target.detail.Quantity.unit],
    [target.detail.Quantity.system],
    [target.detail.Quantity.code],
    [target.detail.Range.id],
    [target.detail.Range.extension],
    [target.detail.Range.low],
    [target.detail.Range.high],
    [target.detail.CodeableConcept.id],
    [target.detail.CodeableConcept.extension],
    [target.detail.CodeableConcept.coding],
    [target.detail.CodeableConcept.text],
    [target.detail.string],
    [target.detail.boolean],
    [target.detail.integer],
    [target.detail.Ratio.id],
    [target.detail.Ratio.extension],
    [target.detail.Ratio.numerator],
    [target.detail.Ratio.denominator],
    [target.due.date],
    [target.due.Duration.id],
    [target.due.Duration.extension],
    [target.due.Duration.value],
    [target.due.Duration.comparator],
    [target.due.Duration.unit],
    [target.due.Duration.system],
    [target.due.Duration.code]
FROM openrowset (
        BULK 'Goal/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [target.JSON]  VARCHAR(MAX) '$.target'
    ) AS rowset
    CROSS APPLY openjson (rowset.[target.JSON]) with (
        [target.id]                    NVARCHAR(4000)      '$.id',
        [target.extension]             NVARCHAR(MAX)       '$.extension',
        [target.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [target.measure.id]            NVARCHAR(4000)      '$.measure.id',
        [target.measure.extension]     NVARCHAR(MAX)       '$.measure.extension',
        [target.measure.coding]        NVARCHAR(MAX)       '$.measure.coding',
        [target.measure.text]          NVARCHAR(4000)      '$.measure.text',
        [target.detail.Quantity.id]    NVARCHAR(4000)      '$.detail.Quantity.id',
        [target.detail.Quantity.extension] NVARCHAR(MAX)       '$.detail.Quantity.extension',
        [target.detail.Quantity.value] float               '$.detail.Quantity.value',
        [target.detail.Quantity.comparator] NVARCHAR(64)        '$.detail.Quantity.comparator',
        [target.detail.Quantity.unit]  NVARCHAR(4000)      '$.detail.Quantity.unit',
        [target.detail.Quantity.system] VARCHAR(256)        '$.detail.Quantity.system',
        [target.detail.Quantity.code]  NVARCHAR(4000)      '$.detail.Quantity.code',
        [target.detail.Range.id]       NVARCHAR(4000)      '$.detail.Range.id',
        [target.detail.Range.extension] NVARCHAR(MAX)       '$.detail.Range.extension',
        [target.detail.Range.low]      NVARCHAR(MAX)       '$.detail.Range.low',
        [target.detail.Range.high]     NVARCHAR(MAX)       '$.detail.Range.high',
        [target.detail.CodeableConcept.id] NVARCHAR(4000)      '$.detail.CodeableConcept.id',
        [target.detail.CodeableConcept.extension] NVARCHAR(MAX)       '$.detail.CodeableConcept.extension',
        [target.detail.CodeableConcept.coding] NVARCHAR(MAX)       '$.detail.CodeableConcept.coding',
        [target.detail.CodeableConcept.text] NVARCHAR(4000)      '$.detail.CodeableConcept.text',
        [target.detail.string]         NVARCHAR(4000)      '$.detail.string',
        [target.detail.boolean]        bit                 '$.detail.boolean',
        [target.detail.integer]        bigint              '$.detail.integer',
        [target.detail.Ratio.id]       NVARCHAR(4000)      '$.detail.Ratio.id',
        [target.detail.Ratio.extension] NVARCHAR(MAX)       '$.detail.Ratio.extension',
        [target.detail.Ratio.numerator] NVARCHAR(MAX)       '$.detail.Ratio.numerator',
        [target.detail.Ratio.denominator] NVARCHAR(MAX)       '$.detail.Ratio.denominator',
        [target.due.date]              VARCHAR(10)         '$.due.date',
        [target.due.Duration.id]       NVARCHAR(4000)      '$.due.Duration.id',
        [target.due.Duration.extension] NVARCHAR(MAX)       '$.due.Duration.extension',
        [target.due.Duration.value]    float               '$.due.Duration.value',
        [target.due.Duration.comparator] NVARCHAR(64)        '$.due.Duration.comparator',
        [target.due.Duration.unit]     NVARCHAR(4000)      '$.due.Duration.unit',
        [target.due.Duration.system]   VARCHAR(256)        '$.due.Duration.system',
        [target.due.Duration.code]     NVARCHAR(4000)      '$.due.Duration.code'
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
        [addresses.id]                 NVARCHAR(4000)      '$.id',
        [addresses.extension]          NVARCHAR(MAX)       '$.extension',
        [addresses.reference]          NVARCHAR(4000)      '$.reference',
        [addresses.type]               VARCHAR(256)        '$.type',
        [addresses.identifier.id]      NVARCHAR(4000)      '$.identifier.id',
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
    [note.author.Reference.id],
    [note.author.Reference.extension],
    [note.author.Reference.reference],
    [note.author.Reference.type],
    [note.author.Reference.identifier],
    [note.author.Reference.display],
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
        [note.id]                      NVARCHAR(4000)      '$.id',
        [note.extension]               NVARCHAR(MAX)       '$.extension',
        [note.time]                    VARCHAR(30)         '$.time',
        [note.text]                    NVARCHAR(MAX)       '$.text',
        [note.author.Reference.id]     NVARCHAR(4000)      '$.author.Reference.id',
        [note.author.Reference.extension] NVARCHAR(MAX)       '$.author.Reference.extension',
        [note.author.Reference.reference] NVARCHAR(4000)      '$.author.Reference.reference',
        [note.author.Reference.type]   VARCHAR(256)        '$.author.Reference.type',
        [note.author.Reference.identifier] NVARCHAR(MAX)       '$.author.Reference.identifier',
        [note.author.Reference.display] NVARCHAR(4000)      '$.author.Reference.display',
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
        [outcomeCode.id]               NVARCHAR(4000)      '$.id',
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
        [outcomeReference.id]          NVARCHAR(4000)      '$.id',
        [outcomeReference.extension]   NVARCHAR(MAX)       '$.extension',
        [outcomeReference.reference]   NVARCHAR(4000)      '$.reference',
        [outcomeReference.type]        VARCHAR(256)        '$.type',
        [outcomeReference.identifier.id] NVARCHAR(4000)      '$.identifier.id',
        [outcomeReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [outcomeReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [outcomeReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [outcomeReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [outcomeReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [outcomeReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [outcomeReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [outcomeReference.display]     NVARCHAR(4000)      '$.display'
    ) j
