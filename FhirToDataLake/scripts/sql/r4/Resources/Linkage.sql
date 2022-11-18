CREATE EXTERNAL TABLE [fhir].[Linkage] (
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
    [active] bit,
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
    [item] VARCHAR(MAX),
) WITH (
    LOCATION='/Linkage/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.LinkageItem AS
SELECT
    [id],
    [item.JSON],
    [item.id],
    [item.extension],
    [item.modifierExtension],
    [item.type],
    [item.resource.id],
    [item.resource.extension],
    [item.resource.reference],
    [item.resource.type],
    [item.resource.identifier],
    [item.resource.display]
FROM openrowset (
        BULK 'Linkage/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [item.JSON]  VARCHAR(MAX) '$.item'
    ) AS rowset
    CROSS APPLY openjson (rowset.[item.JSON]) with (
        [item.id]                      NVARCHAR(100)       '$.id',
        [item.extension]               NVARCHAR(MAX)       '$.extension',
        [item.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [item.type]                    NVARCHAR(64)        '$.type',
        [item.resource.id]             NVARCHAR(100)       '$.resource.id',
        [item.resource.extension]      NVARCHAR(MAX)       '$.resource.extension',
        [item.resource.reference]      NVARCHAR(4000)      '$.resource.reference',
        [item.resource.type]           VARCHAR(256)        '$.resource.type',
        [item.resource.identifier]     NVARCHAR(MAX)       '$.resource.identifier',
        [item.resource.display]        NVARCHAR(4000)      '$.resource.display'
    ) j
