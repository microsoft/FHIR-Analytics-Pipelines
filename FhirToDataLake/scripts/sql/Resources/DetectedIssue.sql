CREATE EXTERNAL TABLE [fhir].[DetectedIssue] (
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
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [severity] NVARCHAR(64),
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
    [implicated] VARCHAR(MAX),
    [evidence] VARCHAR(MAX),
    [detail] NVARCHAR(4000),
    [reference] VARCHAR(256),
    [mitigation] VARCHAR(MAX),
    [identified.dateTime] VARCHAR(64),
    [identified.period.id] NVARCHAR(100),
    [identified.period.extension] NVARCHAR(MAX),
    [identified.period.start] VARCHAR(64),
    [identified.period.end] VARCHAR(64),
) WITH (
    LOCATION='/DetectedIssue/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.DetectedIssueIdentifier AS
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
        BULK 'DetectedIssue/**',
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

CREATE VIEW fhir.DetectedIssueImplicated AS
SELECT
    [id],
    [implicated.JSON],
    [implicated.id],
    [implicated.extension],
    [implicated.reference],
    [implicated.type],
    [implicated.identifier.id],
    [implicated.identifier.extension],
    [implicated.identifier.use],
    [implicated.identifier.type],
    [implicated.identifier.system],
    [implicated.identifier.value],
    [implicated.identifier.period],
    [implicated.identifier.assigner],
    [implicated.display]
FROM openrowset (
        BULK 'DetectedIssue/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [implicated.JSON]  VARCHAR(MAX) '$.implicated'
    ) AS rowset
    CROSS APPLY openjson (rowset.[implicated.JSON]) with (
        [implicated.id]                NVARCHAR(100)       '$.id',
        [implicated.extension]         NVARCHAR(MAX)       '$.extension',
        [implicated.reference]         NVARCHAR(4000)      '$.reference',
        [implicated.type]              VARCHAR(256)        '$.type',
        [implicated.identifier.id]     NVARCHAR(100)       '$.identifier.id',
        [implicated.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [implicated.identifier.use]    NVARCHAR(64)        '$.identifier.use',
        [implicated.identifier.type]   NVARCHAR(MAX)       '$.identifier.type',
        [implicated.identifier.system] VARCHAR(256)        '$.identifier.system',
        [implicated.identifier.value]  NVARCHAR(4000)      '$.identifier.value',
        [implicated.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [implicated.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [implicated.display]           NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DetectedIssueEvidence AS
SELECT
    [id],
    [evidence.JSON],
    [evidence.id],
    [evidence.extension],
    [evidence.modifierExtension],
    [evidence.code],
    [evidence.detail]
FROM openrowset (
        BULK 'DetectedIssue/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [evidence.JSON]  VARCHAR(MAX) '$.evidence'
    ) AS rowset
    CROSS APPLY openjson (rowset.[evidence.JSON]) with (
        [evidence.id]                  NVARCHAR(100)       '$.id',
        [evidence.extension]           NVARCHAR(MAX)       '$.extension',
        [evidence.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [evidence.code]                NVARCHAR(MAX)       '$.code' AS JSON,
        [evidence.detail]              NVARCHAR(MAX)       '$.detail' AS JSON
    ) j

GO

CREATE VIEW fhir.DetectedIssueMitigation AS
SELECT
    [id],
    [mitigation.JSON],
    [mitigation.id],
    [mitigation.extension],
    [mitigation.modifierExtension],
    [mitigation.action.id],
    [mitigation.action.extension],
    [mitigation.action.coding],
    [mitigation.action.text],
    [mitigation.date],
    [mitigation.author.id],
    [mitigation.author.extension],
    [mitigation.author.reference],
    [mitigation.author.type],
    [mitigation.author.identifier],
    [mitigation.author.display]
FROM openrowset (
        BULK 'DetectedIssue/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [mitigation.JSON]  VARCHAR(MAX) '$.mitigation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[mitigation.JSON]) with (
        [mitigation.id]                NVARCHAR(100)       '$.id',
        [mitigation.extension]         NVARCHAR(MAX)       '$.extension',
        [mitigation.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [mitigation.action.id]         NVARCHAR(100)       '$.action.id',
        [mitigation.action.extension]  NVARCHAR(MAX)       '$.action.extension',
        [mitigation.action.coding]     NVARCHAR(MAX)       '$.action.coding',
        [mitigation.action.text]       NVARCHAR(4000)      '$.action.text',
        [mitigation.date]              VARCHAR(64)         '$.date',
        [mitigation.author.id]         NVARCHAR(100)       '$.author.id',
        [mitigation.author.extension]  NVARCHAR(MAX)       '$.author.extension',
        [mitigation.author.reference]  NVARCHAR(4000)      '$.author.reference',
        [mitigation.author.type]       VARCHAR(256)        '$.author.type',
        [mitigation.author.identifier] NVARCHAR(MAX)       '$.author.identifier',
        [mitigation.author.display]    NVARCHAR(4000)      '$.author.display'
    ) j
