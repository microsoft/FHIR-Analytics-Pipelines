CREATE EXTERNAL TABLE [fhir].[DiagnosticReport] (
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
    [basedOn] VARCHAR(MAX),
    [status] NVARCHAR(64),
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
    [issued] VARCHAR(64),
    [performer] VARCHAR(MAX),
    [resultsInterpreter] VARCHAR(MAX),
    [specimen] VARCHAR(MAX),
    [result] VARCHAR(MAX),
    [imagingStudy] VARCHAR(MAX),
    [media] VARCHAR(MAX),
    [conclusion] NVARCHAR(4000),
    [conclusionCode] VARCHAR(MAX),
    [presentedForm] VARCHAR(MAX),
    [effective.dateTime] VARCHAR(64),
    [effective.period.id] NVARCHAR(100),
    [effective.period.extension] NVARCHAR(MAX),
    [effective.period.start] VARCHAR(64),
    [effective.period.end] VARCHAR(64),
) WITH (
    LOCATION='/DiagnosticReport/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.DiagnosticReportIdentifier AS
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
        BULK 'DiagnosticReport/**',
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

CREATE VIEW fhir.DiagnosticReportBasedOn AS
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
        BULK 'DiagnosticReport/**',
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

CREATE VIEW fhir.DiagnosticReportCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'DiagnosticReport/**',
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

CREATE VIEW fhir.DiagnosticReportPerformer AS
SELECT
    [id],
    [performer.JSON],
    [performer.id],
    [performer.extension],
    [performer.reference],
    [performer.type],
    [performer.identifier.id],
    [performer.identifier.extension],
    [performer.identifier.use],
    [performer.identifier.type],
    [performer.identifier.system],
    [performer.identifier.value],
    [performer.identifier.period],
    [performer.identifier.assigner],
    [performer.display]
FROM openrowset (
        BULK 'DiagnosticReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [performer.JSON]  VARCHAR(MAX) '$.performer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[performer.JSON]) with (
        [performer.id]                 NVARCHAR(100)       '$.id',
        [performer.extension]          NVARCHAR(MAX)       '$.extension',
        [performer.reference]          NVARCHAR(4000)      '$.reference',
        [performer.type]               VARCHAR(256)        '$.type',
        [performer.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [performer.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [performer.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [performer.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [performer.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [performer.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [performer.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [performer.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [performer.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DiagnosticReportResultsInterpreter AS
SELECT
    [id],
    [resultsInterpreter.JSON],
    [resultsInterpreter.id],
    [resultsInterpreter.extension],
    [resultsInterpreter.reference],
    [resultsInterpreter.type],
    [resultsInterpreter.identifier.id],
    [resultsInterpreter.identifier.extension],
    [resultsInterpreter.identifier.use],
    [resultsInterpreter.identifier.type],
    [resultsInterpreter.identifier.system],
    [resultsInterpreter.identifier.value],
    [resultsInterpreter.identifier.period],
    [resultsInterpreter.identifier.assigner],
    [resultsInterpreter.display]
FROM openrowset (
        BULK 'DiagnosticReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [resultsInterpreter.JSON]  VARCHAR(MAX) '$.resultsInterpreter'
    ) AS rowset
    CROSS APPLY openjson (rowset.[resultsInterpreter.JSON]) with (
        [resultsInterpreter.id]        NVARCHAR(100)       '$.id',
        [resultsInterpreter.extension] NVARCHAR(MAX)       '$.extension',
        [resultsInterpreter.reference] NVARCHAR(4000)      '$.reference',
        [resultsInterpreter.type]      VARCHAR(256)        '$.type',
        [resultsInterpreter.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [resultsInterpreter.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [resultsInterpreter.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [resultsInterpreter.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [resultsInterpreter.identifier.system] VARCHAR(256)        '$.identifier.system',
        [resultsInterpreter.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [resultsInterpreter.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [resultsInterpreter.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [resultsInterpreter.display]   NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DiagnosticReportSpecimen AS
SELECT
    [id],
    [specimen.JSON],
    [specimen.id],
    [specimen.extension],
    [specimen.reference],
    [specimen.type],
    [specimen.identifier.id],
    [specimen.identifier.extension],
    [specimen.identifier.use],
    [specimen.identifier.type],
    [specimen.identifier.system],
    [specimen.identifier.value],
    [specimen.identifier.period],
    [specimen.identifier.assigner],
    [specimen.display]
FROM openrowset (
        BULK 'DiagnosticReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specimen.JSON]  VARCHAR(MAX) '$.specimen'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specimen.JSON]) with (
        [specimen.id]                  NVARCHAR(100)       '$.id',
        [specimen.extension]           NVARCHAR(MAX)       '$.extension',
        [specimen.reference]           NVARCHAR(4000)      '$.reference',
        [specimen.type]                VARCHAR(256)        '$.type',
        [specimen.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [specimen.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [specimen.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [specimen.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [specimen.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [specimen.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [specimen.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [specimen.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [specimen.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DiagnosticReportResult AS
SELECT
    [id],
    [result.JSON],
    [result.id],
    [result.extension],
    [result.reference],
    [result.type],
    [result.identifier.id],
    [result.identifier.extension],
    [result.identifier.use],
    [result.identifier.type],
    [result.identifier.system],
    [result.identifier.value],
    [result.identifier.period],
    [result.identifier.assigner],
    [result.display]
FROM openrowset (
        BULK 'DiagnosticReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [result.JSON]  VARCHAR(MAX) '$.result'
    ) AS rowset
    CROSS APPLY openjson (rowset.[result.JSON]) with (
        [result.id]                    NVARCHAR(100)       '$.id',
        [result.extension]             NVARCHAR(MAX)       '$.extension',
        [result.reference]             NVARCHAR(4000)      '$.reference',
        [result.type]                  VARCHAR(256)        '$.type',
        [result.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [result.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [result.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [result.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [result.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [result.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [result.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [result.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [result.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DiagnosticReportImagingStudy AS
SELECT
    [id],
    [imagingStudy.JSON],
    [imagingStudy.id],
    [imagingStudy.extension],
    [imagingStudy.reference],
    [imagingStudy.type],
    [imagingStudy.identifier.id],
    [imagingStudy.identifier.extension],
    [imagingStudy.identifier.use],
    [imagingStudy.identifier.type],
    [imagingStudy.identifier.system],
    [imagingStudy.identifier.value],
    [imagingStudy.identifier.period],
    [imagingStudy.identifier.assigner],
    [imagingStudy.display]
FROM openrowset (
        BULK 'DiagnosticReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [imagingStudy.JSON]  VARCHAR(MAX) '$.imagingStudy'
    ) AS rowset
    CROSS APPLY openjson (rowset.[imagingStudy.JSON]) with (
        [imagingStudy.id]              NVARCHAR(100)       '$.id',
        [imagingStudy.extension]       NVARCHAR(MAX)       '$.extension',
        [imagingStudy.reference]       NVARCHAR(4000)      '$.reference',
        [imagingStudy.type]            VARCHAR(256)        '$.type',
        [imagingStudy.identifier.id]   NVARCHAR(100)       '$.identifier.id',
        [imagingStudy.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [imagingStudy.identifier.use]  NVARCHAR(64)        '$.identifier.use',
        [imagingStudy.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [imagingStudy.identifier.system] VARCHAR(256)        '$.identifier.system',
        [imagingStudy.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [imagingStudy.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [imagingStudy.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [imagingStudy.display]         NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DiagnosticReportMedia AS
SELECT
    [id],
    [media.JSON],
    [media.id],
    [media.extension],
    [media.modifierExtension],
    [media.comment],
    [media.link.id],
    [media.link.extension],
    [media.link.reference],
    [media.link.type],
    [media.link.identifier],
    [media.link.display]
FROM openrowset (
        BULK 'DiagnosticReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [media.JSON]  VARCHAR(MAX) '$.media'
    ) AS rowset
    CROSS APPLY openjson (rowset.[media.JSON]) with (
        [media.id]                     NVARCHAR(100)       '$.id',
        [media.extension]              NVARCHAR(MAX)       '$.extension',
        [media.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [media.comment]                NVARCHAR(4000)      '$.comment',
        [media.link.id]                NVARCHAR(100)       '$.link.id',
        [media.link.extension]         NVARCHAR(MAX)       '$.link.extension',
        [media.link.reference]         NVARCHAR(4000)      '$.link.reference',
        [media.link.type]              VARCHAR(256)        '$.link.type',
        [media.link.identifier]        NVARCHAR(MAX)       '$.link.identifier',
        [media.link.display]           NVARCHAR(4000)      '$.link.display'
    ) j

GO

CREATE VIEW fhir.DiagnosticReportConclusionCode AS
SELECT
    [id],
    [conclusionCode.JSON],
    [conclusionCode.id],
    [conclusionCode.extension],
    [conclusionCode.coding],
    [conclusionCode.text]
FROM openrowset (
        BULK 'DiagnosticReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [conclusionCode.JSON]  VARCHAR(MAX) '$.conclusionCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[conclusionCode.JSON]) with (
        [conclusionCode.id]            NVARCHAR(100)       '$.id',
        [conclusionCode.extension]     NVARCHAR(MAX)       '$.extension',
        [conclusionCode.coding]        NVARCHAR(MAX)       '$.coding' AS JSON,
        [conclusionCode.text]          NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.DiagnosticReportPresentedForm AS
SELECT
    [id],
    [presentedForm.JSON],
    [presentedForm.id],
    [presentedForm.extension],
    [presentedForm.contentType],
    [presentedForm.language],
    [presentedForm.data],
    [presentedForm.url],
    [presentedForm.size],
    [presentedForm.hash],
    [presentedForm.title],
    [presentedForm.creation]
FROM openrowset (
        BULK 'DiagnosticReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [presentedForm.JSON]  VARCHAR(MAX) '$.presentedForm'
    ) AS rowset
    CROSS APPLY openjson (rowset.[presentedForm.JSON]) with (
        [presentedForm.id]             NVARCHAR(100)       '$.id',
        [presentedForm.extension]      NVARCHAR(MAX)       '$.extension',
        [presentedForm.contentType]    NVARCHAR(100)       '$.contentType',
        [presentedForm.language]       NVARCHAR(100)       '$.language',
        [presentedForm.data]           NVARCHAR(MAX)       '$.data',
        [presentedForm.url]            VARCHAR(256)        '$.url',
        [presentedForm.size]           bigint              '$.size',
        [presentedForm.hash]           NVARCHAR(MAX)       '$.hash',
        [presentedForm.title]          NVARCHAR(4000)      '$.title',
        [presentedForm.creation]       VARCHAR(64)         '$.creation'
    ) j
