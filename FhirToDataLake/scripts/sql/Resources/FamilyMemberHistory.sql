CREATE EXTERNAL TABLE [fhir].[FamilyMemberHistory] (
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
    [instantiatesCanonical] VARCHAR(MAX),
    [instantiatesUri] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [dataAbsentReason.id] NVARCHAR(4000),
    [dataAbsentReason.extension] NVARCHAR(MAX),
    [dataAbsentReason.coding] VARCHAR(MAX),
    [dataAbsentReason.text] NVARCHAR(4000),
    [patient.id] NVARCHAR(4000),
    [patient.extension] NVARCHAR(MAX),
    [patient.reference] NVARCHAR(4000),
    [patient.type] VARCHAR(256),
    [patient.identifier.id] NVARCHAR(4000),
    [patient.identifier.extension] NVARCHAR(MAX),
    [patient.identifier.use] NVARCHAR(64),
    [patient.identifier.type] NVARCHAR(MAX),
    [patient.identifier.system] VARCHAR(256),
    [patient.identifier.value] NVARCHAR(4000),
    [patient.identifier.period] NVARCHAR(MAX),
    [patient.identifier.assigner] NVARCHAR(MAX),
    [patient.display] NVARCHAR(4000),
    [date] VARCHAR(30),
    [name] NVARCHAR(4000),
    [relationship.id] NVARCHAR(4000),
    [relationship.extension] NVARCHAR(MAX),
    [relationship.coding] VARCHAR(MAX),
    [relationship.text] NVARCHAR(4000),
    [sex.id] NVARCHAR(4000),
    [sex.extension] NVARCHAR(MAX),
    [sex.coding] VARCHAR(MAX),
    [sex.text] NVARCHAR(4000),
    [estimatedAge] bit,
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [condition] VARCHAR(MAX),
    [born.Period.id] NVARCHAR(4000),
    [born.Period.extension] NVARCHAR(MAX),
    [born.Period.start] VARCHAR(30),
    [born.Period.end] VARCHAR(30),
    [born.date] VARCHAR(10),
    [born.string] NVARCHAR(4000),
    [age.Age.id] NVARCHAR(4000),
    [age.Age.extension] NVARCHAR(MAX),
    [age.Age.value] float,
    [age.Age.comparator] NVARCHAR(64),
    [age.Age.unit] NVARCHAR(4000),
    [age.Age.system] VARCHAR(256),
    [age.Age.code] NVARCHAR(4000),
    [age.Range.id] NVARCHAR(4000),
    [age.Range.extension] NVARCHAR(MAX),
    [age.Range.low.id] NVARCHAR(4000),
    [age.Range.low.extension] NVARCHAR(MAX),
    [age.Range.low.value] float,
    [age.Range.low.comparator] NVARCHAR(64),
    [age.Range.low.unit] NVARCHAR(4000),
    [age.Range.low.system] VARCHAR(256),
    [age.Range.low.code] NVARCHAR(4000),
    [age.Range.high.id] NVARCHAR(4000),
    [age.Range.high.extension] NVARCHAR(MAX),
    [age.Range.high.value] float,
    [age.Range.high.comparator] NVARCHAR(64),
    [age.Range.high.unit] NVARCHAR(4000),
    [age.Range.high.system] VARCHAR(256),
    [age.Range.high.code] NVARCHAR(4000),
    [age.string] NVARCHAR(4000),
    [deceased.boolean] bit,
    [deceased.Age.id] NVARCHAR(4000),
    [deceased.Age.extension] NVARCHAR(MAX),
    [deceased.Age.value] float,
    [deceased.Age.comparator] NVARCHAR(64),
    [deceased.Age.unit] NVARCHAR(4000),
    [deceased.Age.system] VARCHAR(256),
    [deceased.Age.code] NVARCHAR(4000),
    [deceased.Range.id] NVARCHAR(4000),
    [deceased.Range.extension] NVARCHAR(MAX),
    [deceased.Range.low.id] NVARCHAR(4000),
    [deceased.Range.low.extension] NVARCHAR(MAX),
    [deceased.Range.low.value] float,
    [deceased.Range.low.comparator] NVARCHAR(64),
    [deceased.Range.low.unit] NVARCHAR(4000),
    [deceased.Range.low.system] VARCHAR(256),
    [deceased.Range.low.code] NVARCHAR(4000),
    [deceased.Range.high.id] NVARCHAR(4000),
    [deceased.Range.high.extension] NVARCHAR(MAX),
    [deceased.Range.high.value] float,
    [deceased.Range.high.comparator] NVARCHAR(64),
    [deceased.Range.high.unit] NVARCHAR(4000),
    [deceased.Range.high.system] VARCHAR(256),
    [deceased.Range.high.code] NVARCHAR(4000),
    [deceased.date] VARCHAR(10),
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
        [reasonCode.id]                NVARCHAR(4000)      '$.id',
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
        [reasonReference.id]           NVARCHAR(4000)      '$.id',
        [reasonReference.extension]    NVARCHAR(MAX)       '$.extension',
        [reasonReference.reference]    NVARCHAR(4000)      '$.reference',
        [reasonReference.type]         VARCHAR(256)        '$.type',
        [reasonReference.identifier.id] NVARCHAR(4000)      '$.identifier.id',
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
    [note.author.Reference.id],
    [note.author.Reference.extension],
    [note.author.Reference.reference],
    [note.author.Reference.type],
    [note.author.Reference.identifier],
    [note.author.Reference.display],
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
    [condition.onset.Age.id],
    [condition.onset.Age.extension],
    [condition.onset.Age.value],
    [condition.onset.Age.comparator],
    [condition.onset.Age.unit],
    [condition.onset.Age.system],
    [condition.onset.Age.code],
    [condition.onset.Range.id],
    [condition.onset.Range.extension],
    [condition.onset.Range.low],
    [condition.onset.Range.high],
    [condition.onset.Period.id],
    [condition.onset.Period.extension],
    [condition.onset.Period.start],
    [condition.onset.Period.end],
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
        [condition.id]                 NVARCHAR(4000)      '$.id',
        [condition.extension]          NVARCHAR(MAX)       '$.extension',
        [condition.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [condition.code.id]            NVARCHAR(4000)      '$.code.id',
        [condition.code.extension]     NVARCHAR(MAX)       '$.code.extension',
        [condition.code.coding]        NVARCHAR(MAX)       '$.code.coding',
        [condition.code.text]          NVARCHAR(4000)      '$.code.text',
        [condition.outcome.id]         NVARCHAR(4000)      '$.outcome.id',
        [condition.outcome.extension]  NVARCHAR(MAX)       '$.outcome.extension',
        [condition.outcome.coding]     NVARCHAR(MAX)       '$.outcome.coding',
        [condition.outcome.text]       NVARCHAR(4000)      '$.outcome.text',
        [condition.contributedToDeath] bit                 '$.contributedToDeath',
        [condition.note]               NVARCHAR(MAX)       '$.note' AS JSON,
        [condition.onset.Age.id]       NVARCHAR(4000)      '$.onset.Age.id',
        [condition.onset.Age.extension] NVARCHAR(MAX)       '$.onset.Age.extension',
        [condition.onset.Age.value]    float               '$.onset.Age.value',
        [condition.onset.Age.comparator] NVARCHAR(64)        '$.onset.Age.comparator',
        [condition.onset.Age.unit]     NVARCHAR(4000)      '$.onset.Age.unit',
        [condition.onset.Age.system]   VARCHAR(256)        '$.onset.Age.system',
        [condition.onset.Age.code]     NVARCHAR(4000)      '$.onset.Age.code',
        [condition.onset.Range.id]     NVARCHAR(4000)      '$.onset.Range.id',
        [condition.onset.Range.extension] NVARCHAR(MAX)       '$.onset.Range.extension',
        [condition.onset.Range.low]    NVARCHAR(MAX)       '$.onset.Range.low',
        [condition.onset.Range.high]   NVARCHAR(MAX)       '$.onset.Range.high',
        [condition.onset.Period.id]    NVARCHAR(4000)      '$.onset.Period.id',
        [condition.onset.Period.extension] NVARCHAR(MAX)       '$.onset.Period.extension',
        [condition.onset.Period.start] VARCHAR(30)         '$.onset.Period.start',
        [condition.onset.Period.end]   VARCHAR(30)         '$.onset.Period.end',
        [condition.onset.string]       NVARCHAR(4000)      '$.onset.string'
    ) j
