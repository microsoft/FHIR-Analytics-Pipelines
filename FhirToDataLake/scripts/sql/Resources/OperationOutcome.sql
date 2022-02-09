CREATE EXTERNAL TABLE [fhir].[OperationOutcome] (
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
    [issue] VARCHAR(MAX),
) WITH (
    LOCATION='/OperationOutcome/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.OperationOutcomeIssue AS
SELECT
    [id],
    [issue.JSON],
    [issue.id],
    [issue.extension],
    [issue.modifierExtension],
    [issue.severity],
    [issue.code],
    [issue.details.id],
    [issue.details.extension],
    [issue.details.coding],
    [issue.details.text],
    [issue.diagnostics],
    [issue.location],
    [issue.expression]
FROM openrowset (
        BULK 'OperationOutcome/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [issue.JSON]  VARCHAR(MAX) '$.issue'
    ) AS rowset
    CROSS APPLY openjson (rowset.[issue.JSON]) with (
        [issue.id]                     NVARCHAR(100)       '$.id',
        [issue.extension]              NVARCHAR(MAX)       '$.extension',
        [issue.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [issue.severity]               NVARCHAR(64)        '$.severity',
        [issue.code]                   NVARCHAR(64)        '$.code',
        [issue.details.id]             NVARCHAR(100)       '$.details.id',
        [issue.details.extension]      NVARCHAR(MAX)       '$.details.extension',
        [issue.details.coding]         NVARCHAR(MAX)       '$.details.coding',
        [issue.details.text]           NVARCHAR(4000)      '$.details.text',
        [issue.diagnostics]            NVARCHAR(4000)      '$.diagnostics',
        [issue.location]               NVARCHAR(MAX)       '$.location' AS JSON,
        [issue.expression]             NVARCHAR(MAX)       '$.expression' AS JSON
    ) j
