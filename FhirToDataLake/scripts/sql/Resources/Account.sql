CREATE EXTERNAL TABLE [fhir].[Account] (
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
    [status] NVARCHAR(64),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [name] NVARCHAR(500),
    [subject] VARCHAR(MAX),
    [servicePeriod.id] NVARCHAR(100),
    [servicePeriod.extension] NVARCHAR(MAX),
    [servicePeriod.start] VARCHAR(64),
    [servicePeriod.end] VARCHAR(64),
    [coverage] VARCHAR(MAX),
    [owner.id] NVARCHAR(100),
    [owner.extension] NVARCHAR(MAX),
    [owner.reference] NVARCHAR(4000),
    [owner.type] VARCHAR(256),
    [owner.identifier.id] NVARCHAR(100),
    [owner.identifier.extension] NVARCHAR(MAX),
    [owner.identifier.use] NVARCHAR(64),
    [owner.identifier.type] NVARCHAR(MAX),
    [owner.identifier.system] VARCHAR(256),
    [owner.identifier.value] NVARCHAR(4000),
    [owner.identifier.period] NVARCHAR(MAX),
    [owner.identifier.assigner] NVARCHAR(MAX),
    [owner.display] NVARCHAR(4000),
    [description] NVARCHAR(4000),
    [guarantor] VARCHAR(MAX),
    [partOf.id] NVARCHAR(100),
    [partOf.extension] NVARCHAR(MAX),
    [partOf.reference] NVARCHAR(4000),
    [partOf.type] VARCHAR(256),
    [partOf.identifier.id] NVARCHAR(100),
    [partOf.identifier.extension] NVARCHAR(MAX),
    [partOf.identifier.use] NVARCHAR(64),
    [partOf.identifier.type] NVARCHAR(MAX),
    [partOf.identifier.system] VARCHAR(256),
    [partOf.identifier.value] NVARCHAR(4000),
    [partOf.identifier.period] NVARCHAR(MAX),
    [partOf.identifier.assigner] NVARCHAR(MAX),
    [partOf.display] NVARCHAR(4000),
) WITH (
    LOCATION='/Account/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.AccountIdentifier AS
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
        BULK 'Account/**',
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

CREATE VIEW fhir.AccountSubject AS
SELECT
    [id],
    [subject.JSON],
    [subject.id],
    [subject.extension],
    [subject.reference],
    [subject.type],
    [subject.identifier.id],
    [subject.identifier.extension],
    [subject.identifier.use],
    [subject.identifier.type],
    [subject.identifier.system],
    [subject.identifier.value],
    [subject.identifier.period],
    [subject.identifier.assigner],
    [subject.display]
FROM openrowset (
        BULK 'Account/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subject.JSON]  VARCHAR(MAX) '$.subject'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subject.JSON]) with (
        [subject.id]                   NVARCHAR(100)       '$.id',
        [subject.extension]            NVARCHAR(MAX)       '$.extension',
        [subject.reference]            NVARCHAR(4000)      '$.reference',
        [subject.type]                 VARCHAR(256)        '$.type',
        [subject.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [subject.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [subject.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [subject.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [subject.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [subject.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [subject.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [subject.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [subject.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.AccountCoverage AS
SELECT
    [id],
    [coverage.JSON],
    [coverage.id],
    [coverage.extension],
    [coverage.modifierExtension],
    [coverage.coverage.id],
    [coverage.coverage.extension],
    [coverage.coverage.reference],
    [coverage.coverage.type],
    [coverage.coverage.identifier],
    [coverage.coverage.display],
    [coverage.priority]
FROM openrowset (
        BULK 'Account/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [coverage.JSON]  VARCHAR(MAX) '$.coverage'
    ) AS rowset
    CROSS APPLY openjson (rowset.[coverage.JSON]) with (
        [coverage.id]                  NVARCHAR(100)       '$.id',
        [coverage.extension]           NVARCHAR(MAX)       '$.extension',
        [coverage.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [coverage.coverage.id]         NVARCHAR(100)       '$.coverage.id',
        [coverage.coverage.extension]  NVARCHAR(MAX)       '$.coverage.extension',
        [coverage.coverage.reference]  NVARCHAR(4000)      '$.coverage.reference',
        [coverage.coverage.type]       VARCHAR(256)        '$.coverage.type',
        [coverage.coverage.identifier] NVARCHAR(MAX)       '$.coverage.identifier',
        [coverage.coverage.display]    NVARCHAR(4000)      '$.coverage.display',
        [coverage.priority]            bigint              '$.priority'
    ) j

GO

CREATE VIEW fhir.AccountGuarantor AS
SELECT
    [id],
    [guarantor.JSON],
    [guarantor.id],
    [guarantor.extension],
    [guarantor.modifierExtension],
    [guarantor.party.id],
    [guarantor.party.extension],
    [guarantor.party.reference],
    [guarantor.party.type],
    [guarantor.party.identifier],
    [guarantor.party.display],
    [guarantor.onHold],
    [guarantor.period.id],
    [guarantor.period.extension],
    [guarantor.period.start],
    [guarantor.period.end]
FROM openrowset (
        BULK 'Account/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [guarantor.JSON]  VARCHAR(MAX) '$.guarantor'
    ) AS rowset
    CROSS APPLY openjson (rowset.[guarantor.JSON]) with (
        [guarantor.id]                 NVARCHAR(100)       '$.id',
        [guarantor.extension]          NVARCHAR(MAX)       '$.extension',
        [guarantor.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [guarantor.party.id]           NVARCHAR(100)       '$.party.id',
        [guarantor.party.extension]    NVARCHAR(MAX)       '$.party.extension',
        [guarantor.party.reference]    NVARCHAR(4000)      '$.party.reference',
        [guarantor.party.type]         VARCHAR(256)        '$.party.type',
        [guarantor.party.identifier]   NVARCHAR(MAX)       '$.party.identifier',
        [guarantor.party.display]      NVARCHAR(4000)      '$.party.display',
        [guarantor.onHold]             bit                 '$.onHold',
        [guarantor.period.id]          NVARCHAR(100)       '$.period.id',
        [guarantor.period.extension]   NVARCHAR(MAX)       '$.period.extension',
        [guarantor.period.start]       VARCHAR(64)         '$.period.start',
        [guarantor.period.end]         VARCHAR(64)         '$.period.end'
    ) j
