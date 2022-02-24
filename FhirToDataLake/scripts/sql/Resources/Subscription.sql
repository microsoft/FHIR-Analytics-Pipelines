CREATE EXTERNAL TABLE [fhir].[Subscription] (
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
    [status] NVARCHAR(64),
    [contact] VARCHAR(MAX),
    [end] VARCHAR(64),
    [reason] NVARCHAR(4000),
    [criteria] NVARCHAR(4000),
    [error] NVARCHAR(4000),
    [channel.id] NVARCHAR(100),
    [channel.extension] NVARCHAR(MAX),
    [channel.modifierExtension] NVARCHAR(MAX),
    [channel.type] NVARCHAR(64),
    [channel.endpoint] VARCHAR(256),
    [channel.payload] NVARCHAR(100),
    [channel.header] VARCHAR(MAX),
) WITH (
    LOCATION='/Subscription/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SubscriptionContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.system],
    [contact.value],
    [contact.use],
    [contact.rank],
    [contact.period.id],
    [contact.period.extension],
    [contact.period.start],
    [contact.period.end]
FROM openrowset (
        BULK 'Subscription/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contact.JSON]  VARCHAR(MAX) '$.contact'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contact.JSON]) with (
        [contact.id]                   NVARCHAR(100)       '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.system]               NVARCHAR(64)        '$.system',
        [contact.value]                NVARCHAR(4000)      '$.value',
        [contact.use]                  NVARCHAR(64)        '$.use',
        [contact.rank]                 bigint              '$.rank',
        [contact.period.id]            NVARCHAR(100)       '$.period.id',
        [contact.period.extension]     NVARCHAR(MAX)       '$.period.extension',
        [contact.period.start]         VARCHAR(64)         '$.period.start',
        [contact.period.end]           VARCHAR(64)         '$.period.end'
    ) j
