CREATE EXTERNAL TABLE [fhir].[MeasureReport] (
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
    [type] NVARCHAR(64),
    [measure] VARCHAR(256),
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
    [date] VARCHAR(64),
    [reporter.id] NVARCHAR(100),
    [reporter.extension] NVARCHAR(MAX),
    [reporter.reference] NVARCHAR(4000),
    [reporter.type] VARCHAR(256),
    [reporter.identifier.id] NVARCHAR(100),
    [reporter.identifier.extension] NVARCHAR(MAX),
    [reporter.identifier.use] NVARCHAR(64),
    [reporter.identifier.type] NVARCHAR(MAX),
    [reporter.identifier.system] VARCHAR(256),
    [reporter.identifier.value] NVARCHAR(4000),
    [reporter.identifier.period] NVARCHAR(MAX),
    [reporter.identifier.assigner] NVARCHAR(MAX),
    [reporter.display] NVARCHAR(4000),
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [improvementNotation.id] NVARCHAR(100),
    [improvementNotation.extension] NVARCHAR(MAX),
    [improvementNotation.coding] VARCHAR(MAX),
    [improvementNotation.text] NVARCHAR(4000),
    [group] VARCHAR(MAX),
    [evaluatedResource] VARCHAR(MAX),
) WITH (
    LOCATION='/MeasureReport/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MeasureReportIdentifier AS
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
        BULK 'MeasureReport/**',
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

CREATE VIEW fhir.MeasureReportGroup AS
SELECT
    [id],
    [group.JSON],
    [group.id],
    [group.extension],
    [group.modifierExtension],
    [group.code.id],
    [group.code.extension],
    [group.code.coding],
    [group.code.text],
    [group.population],
    [group.measureScore.id],
    [group.measureScore.extension],
    [group.measureScore.value],
    [group.measureScore.comparator],
    [group.measureScore.unit],
    [group.measureScore.system],
    [group.measureScore.code],
    [group.stratifier]
FROM openrowset (
        BULK 'MeasureReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [group.JSON]  VARCHAR(MAX) '$.group'
    ) AS rowset
    CROSS APPLY openjson (rowset.[group.JSON]) with (
        [group.id]                     NVARCHAR(100)       '$.id',
        [group.extension]              NVARCHAR(MAX)       '$.extension',
        [group.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [group.code.id]                NVARCHAR(100)       '$.code.id',
        [group.code.extension]         NVARCHAR(MAX)       '$.code.extension',
        [group.code.coding]            NVARCHAR(MAX)       '$.code.coding',
        [group.code.text]              NVARCHAR(4000)      '$.code.text',
        [group.population]             NVARCHAR(MAX)       '$.population' AS JSON,
        [group.measureScore.id]        NVARCHAR(100)       '$.measureScore.id',
        [group.measureScore.extension] NVARCHAR(MAX)       '$.measureScore.extension',
        [group.measureScore.value]     float               '$.measureScore.value',
        [group.measureScore.comparator] NVARCHAR(64)        '$.measureScore.comparator',
        [group.measureScore.unit]      NVARCHAR(100)       '$.measureScore.unit',
        [group.measureScore.system]    VARCHAR(256)        '$.measureScore.system',
        [group.measureScore.code]      NVARCHAR(4000)      '$.measureScore.code',
        [group.stratifier]             NVARCHAR(MAX)       '$.stratifier' AS JSON
    ) j

GO

CREATE VIEW fhir.MeasureReportEvaluatedResource AS
SELECT
    [id],
    [evaluatedResource.JSON],
    [evaluatedResource.id],
    [evaluatedResource.extension],
    [evaluatedResource.reference],
    [evaluatedResource.type],
    [evaluatedResource.identifier.id],
    [evaluatedResource.identifier.extension],
    [evaluatedResource.identifier.use],
    [evaluatedResource.identifier.type],
    [evaluatedResource.identifier.system],
    [evaluatedResource.identifier.value],
    [evaluatedResource.identifier.period],
    [evaluatedResource.identifier.assigner],
    [evaluatedResource.display]
FROM openrowset (
        BULK 'MeasureReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [evaluatedResource.JSON]  VARCHAR(MAX) '$.evaluatedResource'
    ) AS rowset
    CROSS APPLY openjson (rowset.[evaluatedResource.JSON]) with (
        [evaluatedResource.id]         NVARCHAR(100)       '$.id',
        [evaluatedResource.extension]  NVARCHAR(MAX)       '$.extension',
        [evaluatedResource.reference]  NVARCHAR(4000)      '$.reference',
        [evaluatedResource.type]       VARCHAR(256)        '$.type',
        [evaluatedResource.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [evaluatedResource.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [evaluatedResource.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [evaluatedResource.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [evaluatedResource.identifier.system] VARCHAR(256)        '$.identifier.system',
        [evaluatedResource.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [evaluatedResource.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [evaluatedResource.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [evaluatedResource.display]    NVARCHAR(4000)      '$.display'
    ) j
