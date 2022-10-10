CREATE EXTERNAL TABLE [fhir].[SubscriptionStatus] (
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
    [type] NVARCHAR(100),
    [eventsSinceSubscriptionStart] NVARCHAR(MAX),
    [eventsInNotification] bigint,
    [notificationEvent] VARCHAR(MAX),
    [subscription.id] NVARCHAR(100),
    [subscription.extension] NVARCHAR(MAX),
    [subscription.reference] NVARCHAR(4000),
    [subscription.type] VARCHAR(256),
    [subscription.identifier.id] NVARCHAR(100),
    [subscription.identifier.extension] NVARCHAR(MAX),
    [subscription.identifier.use] NVARCHAR(64),
    [subscription.identifier.type] NVARCHAR(MAX),
    [subscription.identifier.system] VARCHAR(256),
    [subscription.identifier.value] NVARCHAR(4000),
    [subscription.identifier.period] NVARCHAR(MAX),
    [subscription.identifier.assigner] NVARCHAR(MAX),
    [subscription.display] NVARCHAR(4000),
    [topic] VARCHAR(256),
    [error] VARCHAR(MAX),
) WITH (
    LOCATION='/SubscriptionStatus/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SubscriptionStatusNotificationEvent AS
SELECT
    [id],
    [notificationEvent.JSON],
    [notificationEvent.id],
    [notificationEvent.extension],
    [notificationEvent.modifierExtension],
    [notificationEvent.eventNumber],
    [notificationEvent.timestamp],
    [notificationEvent.focus.id],
    [notificationEvent.focus.extension],
    [notificationEvent.focus.reference],
    [notificationEvent.focus.type],
    [notificationEvent.focus.identifier],
    [notificationEvent.focus.display],
    [notificationEvent.additionalContext]
FROM openrowset (
        BULK 'SubscriptionStatus/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [notificationEvent.JSON]  VARCHAR(MAX) '$.notificationEvent'
    ) AS rowset
    CROSS APPLY openjson (rowset.[notificationEvent.JSON]) with (
        [notificationEvent.id]         NVARCHAR(100)       '$.id',
        [notificationEvent.extension]  NVARCHAR(MAX)       '$.extension',
        [notificationEvent.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [notificationEvent.eventNumber] NVARCHAR(MAX)       '$.eventNumber',
        [notificationEvent.timestamp]  VARCHAR(64)         '$.timestamp',
        [notificationEvent.focus.id]   NVARCHAR(100)       '$.focus.id',
        [notificationEvent.focus.extension] NVARCHAR(MAX)       '$.focus.extension',
        [notificationEvent.focus.reference] NVARCHAR(4000)      '$.focus.reference',
        [notificationEvent.focus.type] VARCHAR(256)        '$.focus.type',
        [notificationEvent.focus.identifier] NVARCHAR(MAX)       '$.focus.identifier',
        [notificationEvent.focus.display] NVARCHAR(4000)      '$.focus.display',
        [notificationEvent.additionalContext] NVARCHAR(MAX)       '$.additionalContext' AS JSON
    ) j

GO

CREATE VIEW fhir.SubscriptionStatusError AS
SELECT
    [id],
    [error.JSON],
    [error.id],
    [error.extension],
    [error.coding],
    [error.text]
FROM openrowset (
        BULK 'SubscriptionStatus/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [error.JSON]  VARCHAR(MAX) '$.error'
    ) AS rowset
    CROSS APPLY openjson (rowset.[error.JSON]) with (
        [error.id]                     NVARCHAR(100)       '$.id',
        [error.extension]              NVARCHAR(MAX)       '$.extension',
        [error.coding]                 NVARCHAR(MAX)       '$.coding' AS JSON,
        [error.text]                   NVARCHAR(4000)      '$.text'
    ) j
