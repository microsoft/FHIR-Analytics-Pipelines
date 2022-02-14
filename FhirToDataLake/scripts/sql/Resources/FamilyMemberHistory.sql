CREATE EXTERNAL TABLE [fhir].[FamilyMemberHistory] (
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
    [status] NVARCHAR(64),
    [dataAbsentReason.id] NVARCHAR(100),
    [dataAbsentReason.extension] NVARCHAR(MAX),
    [dataAbsentReason.coding] VARCHAR(MAX),
    [dataAbsentReason.text] NVARCHAR(4000),
    [patient.id] NVARCHAR(100),
    [patient.extension] NVARCHAR(MAX),
    [patient.reference] NVARCHAR(4000),
    [patient.type] VARCHAR(256),
    [patient.identifier.id] NVARCHAR(100),
    [patient.identifier.extension] NVARCHAR(MAX),
    [patient.identifier.use] NVARCHAR(64),
    [patient.identifier.type] NVARCHAR(MAX),
    [patient.identifier.system] VARCHAR(256),
    [patient.identifier.value] NVARCHAR(4000),
    [patient.identifier.period] NVARCHAR(MAX),
    [patient.identifier.assigner] NVARCHAR(MAX),
    [patient.display] NVARCHAR(4000),
    [date] VARCHAR(64),
    [name] NVARCHAR(500),
    [relationship.id] NVARCHAR(100),
    [relationship.extension] NVARCHAR(MAX),
    [relationship.coding] VARCHAR(MAX),
    [relationship.text] NVARCHAR(4000),
    [sex.id] NVARCHAR(100),
    [sex.extension] NVARCHAR(MAX),
    [sex.coding] VARCHAR(MAX),
    [sex.text] NVARCHAR(4000),
    [estimatedAge] bit,
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [condition] VARCHAR(MAX),
    [born.period.id] NVARCHAR(100),
    [born.period.extension] NVARCHAR(MAX),
    [born.period.start] VARCHAR(64),
    [born.period.end] VARCHAR(64),
    [born.date] VARCHAR(64),
    [born.string] NVARCHAR(4000),
    [age.age.id] NVARCHAR(100),
    [age.age.extension] NVARCHAR(MAX),
    [age.age.value] float,
    [age.age.comparator] NVARCHAR(64),
    [age.age.unit] NVARCHAR(100),
    [age.age.system] VARCHAR(256),
    [age.age.code] NVARCHAR(4000),
    [age.range.id] NVARCHAR(100),
    [age.range.extension] NVARCHAR(MAX),
    [age.range.low.id] NVARCHAR(100),
    [age.range.low.extension] NVARCHAR(MAX),
    [age.range.low.value] float,
    [age.range.low.comparator] NVARCHAR(64),
    [age.range.low.unit] NVARCHAR(100),
    [age.range.low.system] VARCHAR(256),
    [age.range.low.code] NVARCHAR(4000),
    [age.range.high.id] NVARCHAR(100),
    [age.range.high.extension] NVARCHAR(MAX),
    [age.range.high.value] float,
    [age.range.high.comparator] NVARCHAR(64),
    [age.range.high.unit] NVARCHAR(100),
    [age.range.high.system] VARCHAR(256),
    [age.range.high.code] NVARCHAR(4000),
    [age.string] NVARCHAR(4000),
    [deceased.boolean] bit,
    [deceased.age.id] NVARCHAR(100),
    [deceased.age.extension] NVARCHAR(MAX),
    [deceased.age.value] float,
    [deceased.age.comparator] NVARCHAR(64),
    [deceased.age.unit] NVARCHAR(100),
    [deceased.age.system] VARCHAR(256),
    [deceased.age.code] NVARCHAR(4000),
    [deceased.range.id] NVARCHAR(100),
    [deceased.range.extension] NVARCHAR(MAX),
    [deceased.range.low.id] NVARCHAR(100),
    [deceased.range.low.extension] NVARCHAR(MAX),
    [deceased.range.low.value] float,
    [deceased.range.low.comparator] NVARCHAR(64),
    [deceased.range.low.unit] NVARCHAR(100),
    [deceased.range.low.system] VARCHAR(256),
    [deceased.range.low.code] NVARCHAR(4000),
    [deceased.range.high.id] NVARCHAR(100),
    [deceased.range.high.extension] NVARCHAR(MAX),
    [deceased.range.high.value] float,
    [deceased.range.high.comparator] NVARCHAR(64),
    [deceased.range.high.unit] NVARCHAR(100),
    [deceased.range.high.system] VARCHAR(256),
    [deceased.range.high.code] NVARCHAR(4000),
    [deceased.date] VARCHAR(64),
    [deceased.string] NVARCHAR(4000),
) WITH (
    LOCATION='/FamilyMemberHistory/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.FamilyMemberHistoryIdentifier AS
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
        BULK 'FamilyMemberHistory/**',
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

CREATE VIEW fhir.FamilyMemberHistoryInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'FamilyMemberHistory/**',
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

CREATE VIEW fhir.FamilyMemberHistoryInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'FamilyMemberHistory/**',
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

CREATE VIEW fhir.FamilyMemberHistoryReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'FamilyMemberHistory/**',
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

CREATE VIEW fhir.FamilyMemberHistoryReasonReference AS
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
        BULK 'FamilyMemberHistory/**',
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

CREATE VIEW fhir.FamilyMemberHistoryNote AS
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
        BULK 'FamilyMemberHistory/**',
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

CREATE VIEW fhir.FamilyMemberHistoryCondition AS
SELECT
    [id],
    [condition.JSON],
    [condition.id],
    [condition.extension],
    [condition.modifierExtension],
    [condition.code.id],
    [condition.code.extension],
    [condition.code.coding],
    [condition.code.text],
    [condition.outcome.id],
    [condition.outcome.extension],
    [condition.outcome.coding],
    [condition.outcome.text],
    [condition.contributedToDeath],
    [condition.note],
    [condition.onset.age.id],
    [condition.onset.age.extension],
    [condition.onset.age.value],
    [condition.onset.age.comparator],
    [condition.onset.age.unit],
    [condition.onset.age.system],
    [condition.onset.age.code],
    [condition.onset.range.id],
    [condition.onset.range.extension],
    [condition.onset.range.low],
    [condition.onset.range.high],
    [condition.onset.period.id],
    [condition.onset.period.extension],
    [condition.onset.period.start],
    [condition.onset.period.end],
    [condition.onset.string]
FROM openrowset (
        BULK 'FamilyMemberHistory/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [condition.JSON]  VARCHAR(MAX) '$.condition'
    ) AS rowset
    CROSS APPLY openjson (rowset.[condition.JSON]) with (
        [condition.id]                 NVARCHAR(100)       '$.id',
        [condition.extension]          NVARCHAR(MAX)       '$.extension',
        [condition.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [condition.code.id]            NVARCHAR(100)       '$.code.id',
        [condition.code.extension]     NVARCHAR(MAX)       '$.code.extension',
        [condition.code.coding]        NVARCHAR(MAX)       '$.code.coding',
        [condition.code.text]          NVARCHAR(4000)      '$.code.text',
        [condition.outcome.id]         NVARCHAR(100)       '$.outcome.id',
        [condition.outcome.extension]  NVARCHAR(MAX)       '$.outcome.extension',
        [condition.outcome.coding]     NVARCHAR(MAX)       '$.outcome.coding',
        [condition.outcome.text]       NVARCHAR(4000)      '$.outcome.text',
        [condition.contributedToDeath] bit                 '$.contributedToDeath',
        [condition.note]               NVARCHAR(MAX)       '$.note' AS JSON,
        [condition.onset.age.id]       NVARCHAR(100)       '$.onset.age.id',
        [condition.onset.age.extension] NVARCHAR(MAX)       '$.onset.age.extension',
        [condition.onset.age.value]    float               '$.onset.age.value',
        [condition.onset.age.comparator] NVARCHAR(64)        '$.onset.age.comparator',
        [condition.onset.age.unit]     NVARCHAR(100)       '$.onset.age.unit',
        [condition.onset.age.system]   VARCHAR(256)        '$.onset.age.system',
        [condition.onset.age.code]     NVARCHAR(4000)      '$.onset.age.code',
        [condition.onset.range.id]     NVARCHAR(100)       '$.onset.range.id',
        [condition.onset.range.extension] NVARCHAR(MAX)       '$.onset.range.extension',
        [condition.onset.range.low]    NVARCHAR(MAX)       '$.onset.range.low',
        [condition.onset.range.high]   NVARCHAR(MAX)       '$.onset.range.high',
        [condition.onset.period.id]    NVARCHAR(100)       '$.onset.period.id',
        [condition.onset.period.extension] NVARCHAR(MAX)       '$.onset.period.extension',
        [condition.onset.period.start] VARCHAR(64)         '$.onset.period.start',
        [condition.onset.period.end]   VARCHAR(64)         '$.onset.period.end',
        [condition.onset.string]       NVARCHAR(4000)      '$.onset.string'
    ) j
