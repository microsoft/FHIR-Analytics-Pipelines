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
    [identifier] VARCHAR(MAX),
    [name] NVARCHAR(500),
    [status] NVARCHAR(100),
    [topic] VARCHAR(256),
    [contact] VARCHAR(MAX),
    [end] VARCHAR(64),
    [reason] NVARCHAR(4000),
    [filterBy] VARCHAR(MAX),
    [channelType.id] NVARCHAR(100),
    [channelType.extension] NVARCHAR(MAX),
    [channelType.system] VARCHAR(256),
    [channelType.version] NVARCHAR(100),
    [channelType.code] NVARCHAR(4000),
    [channelType.display] NVARCHAR(4000),
    [channelType.userSelected] bit,
    [endpoint] VARCHAR(256),
    [header] VARCHAR(MAX),
    [heartbeatPeriod] bigint,
    [timeout] bigint,
    [contentType] NVARCHAR(100),
    [content] NVARCHAR(4000),
    [notificationUrlLocation] NVARCHAR(4000),
    [maxCount] bigint,
) WITH (
    LOCATION='/Subscription/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SubscriptionIdentifier AS
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
        BULK 'Subscription/**',
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

GO

CREATE VIEW fhir.SubscriptionFilterBy AS
SELECT
    [id],
    [filterBy.JSON],
    [filterBy.id],
    [filterBy.extension],
    [filterBy.modifierExtension],
    [filterBy.resourceType],
    [filterBy.searchParamName],
    [filterBy.searchModifier],
    [filterBy.value]
FROM openrowset (
        BULK 'Subscription/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [filterBy.JSON]  VARCHAR(MAX) '$.filterBy'
    ) AS rowset
    CROSS APPLY openjson (rowset.[filterBy.JSON]) with (
        [filterBy.id]                  NVARCHAR(100)       '$.id',
        [filterBy.extension]           NVARCHAR(MAX)       '$.extension',
        [filterBy.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [filterBy.resourceType]        NVARCHAR(4000)      '$.resourceType',
        [filterBy.searchParamName]     NVARCHAR(4000)      '$.searchParamName',
        [filterBy.searchModifier]      NVARCHAR(4000)      '$.searchModifier',
        [filterBy.value]               NVARCHAR(4000)      '$.value'
    ) j

GO

CREATE VIEW fhir.SubscriptionHeader AS
SELECT
    [id],
    [header.JSON],
    [header]
FROM openrowset (
        BULK 'Subscription/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [header.JSON]  VARCHAR(MAX) '$.header'
    ) AS rowset
    CROSS APPLY openjson (rowset.[header.JSON]) with (
        [header]                       NVARCHAR(MAX)       '$'
    ) j
