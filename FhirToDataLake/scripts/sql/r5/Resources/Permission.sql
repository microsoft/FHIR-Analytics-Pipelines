CREATE EXTERNAL TABLE [fhir].[Permission] (
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
    [status] NVARCHAR(100),
    [intent.id] NVARCHAR(100),
    [intent.extension] NVARCHAR(MAX),
    [intent.coding] VARCHAR(MAX),
    [intent.text] NVARCHAR(4000),
    [asserter.id] NVARCHAR(100),
    [asserter.extension] NVARCHAR(MAX),
    [asserter.reference] NVARCHAR(4000),
    [asserter.type] VARCHAR(256),
    [asserter.identifier.id] NVARCHAR(100),
    [asserter.identifier.extension] NVARCHAR(MAX),
    [asserter.identifier.use] NVARCHAR(64),
    [asserter.identifier.type] NVARCHAR(MAX),
    [asserter.identifier.system] VARCHAR(256),
    [asserter.identifier.value] NVARCHAR(4000),
    [asserter.identifier.period] NVARCHAR(MAX),
    [asserter.identifier.assigner] NVARCHAR(MAX),
    [asserter.display] NVARCHAR(4000),
    [assertionDate] VARCHAR(MAX),
    [validity.id] NVARCHAR(100),
    [validity.extension] NVARCHAR(MAX),
    [validity.start] VARCHAR(64),
    [validity.end] VARCHAR(64),
    [purpose] VARCHAR(MAX),
    [dataScope] VARCHAR(MAX),
    [processingActivity] VARCHAR(MAX),
    [justification.id] NVARCHAR(100),
    [justification.extension] NVARCHAR(MAX),
    [justification.modifierExtension] NVARCHAR(MAX),
    [justification.evidence] VARCHAR(MAX),
    [justification.grounds] VARCHAR(MAX),
    [usageLimitations] VARCHAR(MAX),
) WITH (
    LOCATION='/Permission/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.PermissionAssertionDate AS
SELECT
    [id],
    [assertionDate.JSON],
    [assertionDate]
FROM openrowset (
        BULK 'Permission/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [assertionDate.JSON]  VARCHAR(MAX) '$.assertionDate'
    ) AS rowset
    CROSS APPLY openjson (rowset.[assertionDate.JSON]) with (
        [assertionDate]                NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.PermissionPurpose AS
SELECT
    [id],
    [purpose.JSON],
    [purpose.id],
    [purpose.extension],
    [purpose.coding],
    [purpose.text]
FROM openrowset (
        BULK 'Permission/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [purpose.JSON]  VARCHAR(MAX) '$.purpose'
    ) AS rowset
    CROSS APPLY openjson (rowset.[purpose.JSON]) with (
        [purpose.id]                   NVARCHAR(100)       '$.id',
        [purpose.extension]            NVARCHAR(MAX)       '$.extension',
        [purpose.coding]               NVARCHAR(MAX)       '$.coding' AS JSON,
        [purpose.text]                 NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.PermissionDataScope AS
SELECT
    [id],
    [dataScope.JSON],
    [dataScope.id],
    [dataScope.extension],
    [dataScope.description],
    [dataScope.name],
    [dataScope.language],
    [dataScope.expression],
    [dataScope.reference]
FROM openrowset (
        BULK 'Permission/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [dataScope.JSON]  VARCHAR(MAX) '$.dataScope'
    ) AS rowset
    CROSS APPLY openjson (rowset.[dataScope.JSON]) with (
        [dataScope.id]                 NVARCHAR(100)       '$.id',
        [dataScope.extension]          NVARCHAR(MAX)       '$.extension',
        [dataScope.description]        NVARCHAR(4000)      '$.description',
        [dataScope.name]               VARCHAR(64)         '$.name',
        [dataScope.language]           NVARCHAR(100)       '$.language',
        [dataScope.expression]         NVARCHAR(4000)      '$.expression',
        [dataScope.reference]          VARCHAR(256)        '$.reference'
    ) j

GO

CREATE VIEW fhir.PermissionProcessingActivity AS
SELECT
    [id],
    [processingActivity.JSON],
    [processingActivity.id],
    [processingActivity.extension],
    [processingActivity.modifierExtension],
    [processingActivity.partyReference],
    [processingActivity.partyCodeableConcept],
    [processingActivity.purpose]
FROM openrowset (
        BULK 'Permission/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [processingActivity.JSON]  VARCHAR(MAX) '$.processingActivity'
    ) AS rowset
    CROSS APPLY openjson (rowset.[processingActivity.JSON]) with (
        [processingActivity.id]        NVARCHAR(100)       '$.id',
        [processingActivity.extension] NVARCHAR(MAX)       '$.extension',
        [processingActivity.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [processingActivity.partyReference] NVARCHAR(MAX)       '$.partyReference' AS JSON,
        [processingActivity.partyCodeableConcept] NVARCHAR(MAX)       '$.partyCodeableConcept' AS JSON,
        [processingActivity.purpose]   NVARCHAR(MAX)       '$.purpose' AS JSON
    ) j

GO

CREATE VIEW fhir.PermissionUsageLimitations AS
SELECT
    [id],
    [usageLimitations.JSON],
    [usageLimitations.id],
    [usageLimitations.extension],
    [usageLimitations.coding],
    [usageLimitations.text]
FROM openrowset (
        BULK 'Permission/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [usageLimitations.JSON]  VARCHAR(MAX) '$.usageLimitations'
    ) AS rowset
    CROSS APPLY openjson (rowset.[usageLimitations.JSON]) with (
        [usageLimitations.id]          NVARCHAR(100)       '$.id',
        [usageLimitations.extension]   NVARCHAR(MAX)       '$.extension',
        [usageLimitations.coding]      NVARCHAR(MAX)       '$.coding' AS JSON,
        [usageLimitations.text]        NVARCHAR(4000)      '$.text'
    ) j
