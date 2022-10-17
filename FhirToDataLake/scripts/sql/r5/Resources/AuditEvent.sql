CREATE EXTERNAL TABLE [fhir].[AuditEvent] (
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
    [category] VARCHAR(MAX),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [action] NVARCHAR(4000),
    [severity] NVARCHAR(4000),
    [recorded] VARCHAR(64),
    [outcome.id] NVARCHAR(100),
    [outcome.extension] NVARCHAR(MAX),
    [outcome.modifierExtension] NVARCHAR(MAX),
    [outcome.code.id] NVARCHAR(100),
    [outcome.code.extension] NVARCHAR(MAX),
    [outcome.code.system] VARCHAR(256),
    [outcome.code.version] NVARCHAR(100),
    [outcome.code.code] NVARCHAR(4000),
    [outcome.code.display] NVARCHAR(4000),
    [outcome.code.userSelected] bit,
    [outcome.detail] VARCHAR(MAX),
    [authorization] VARCHAR(MAX),
    [basedOn] VARCHAR(MAX),
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
    [agent] VARCHAR(MAX),
    [source.id] NVARCHAR(100),
    [source.extension] NVARCHAR(MAX),
    [source.modifierExtension] NVARCHAR(MAX),
    [source.site.id] NVARCHAR(100),
    [source.site.extension] NVARCHAR(MAX),
    [source.site.reference] NVARCHAR(4000),
    [source.site.type] VARCHAR(256),
    [source.site.identifier] NVARCHAR(MAX),
    [source.site.display] NVARCHAR(4000),
    [source.observer.id] NVARCHAR(100),
    [source.observer.extension] NVARCHAR(MAX),
    [source.observer.reference] NVARCHAR(4000),
    [source.observer.type] VARCHAR(256),
    [source.observer.identifier] NVARCHAR(MAX),
    [source.observer.display] NVARCHAR(4000),
    [source.type] VARCHAR(MAX),
    [entity] VARCHAR(MAX),
    [occurred.period.id] NVARCHAR(100),
    [occurred.period.extension] NVARCHAR(MAX),
    [occurred.period.start] VARCHAR(64),
    [occurred.period.end] VARCHAR(64),
    [occurred.dateTime] VARCHAR(64),
) WITH (
    LOCATION='/AuditEvent/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.AuditEventCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'AuditEvent/**',
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

CREATE VIEW fhir.AuditEventAuthorization AS
SELECT
    [id],
    [authorization.JSON],
    [authorization.id],
    [authorization.extension],
    [authorization.coding],
    [authorization.text]
FROM openrowset (
        BULK 'AuditEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [authorization.JSON]  VARCHAR(MAX) '$.authorization'
    ) AS rowset
    CROSS APPLY openjson (rowset.[authorization.JSON]) with (
        [authorization.id]             NVARCHAR(100)       '$.id',
        [authorization.extension]      NVARCHAR(MAX)       '$.extension',
        [authorization.coding]         NVARCHAR(MAX)       '$.coding' AS JSON,
        [authorization.text]           NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.AuditEventBasedOn AS
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
        BULK 'AuditEvent/**',
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

CREATE VIEW fhir.AuditEventAgent AS
SELECT
    [id],
    [agent.JSON],
    [agent.id],
    [agent.extension],
    [agent.modifierExtension],
    [agent.type.id],
    [agent.type.extension],
    [agent.type.coding],
    [agent.type.text],
    [agent.role],
    [agent.who.id],
    [agent.who.extension],
    [agent.who.reference],
    [agent.who.type],
    [agent.who.identifier],
    [agent.who.display],
    [agent.requestor],
    [agent.location.id],
    [agent.location.extension],
    [agent.location.reference],
    [agent.location.type],
    [agent.location.identifier],
    [agent.location.display],
    [agent.policy],
    [agent.authorization],
    [agent.network.reference.id],
    [agent.network.reference.extension],
    [agent.network.reference.reference],
    [agent.network.reference.type],
    [agent.network.reference.identifier],
    [agent.network.reference.display],
    [agent.network.uri],
    [agent.network.string]
FROM openrowset (
        BULK 'AuditEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [agent.JSON]  VARCHAR(MAX) '$.agent'
    ) AS rowset
    CROSS APPLY openjson (rowset.[agent.JSON]) with (
        [agent.id]                     NVARCHAR(100)       '$.id',
        [agent.extension]              NVARCHAR(MAX)       '$.extension',
        [agent.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [agent.type.id]                NVARCHAR(100)       '$.type.id',
        [agent.type.extension]         NVARCHAR(MAX)       '$.type.extension',
        [agent.type.coding]            NVARCHAR(MAX)       '$.type.coding',
        [agent.type.text]              NVARCHAR(4000)      '$.type.text',
        [agent.role]                   NVARCHAR(MAX)       '$.role' AS JSON,
        [agent.who.id]                 NVARCHAR(100)       '$.who.id',
        [agent.who.extension]          NVARCHAR(MAX)       '$.who.extension',
        [agent.who.reference]          NVARCHAR(4000)      '$.who.reference',
        [agent.who.type]               VARCHAR(256)        '$.who.type',
        [agent.who.identifier]         NVARCHAR(MAX)       '$.who.identifier',
        [agent.who.display]            NVARCHAR(4000)      '$.who.display',
        [agent.requestor]              bit                 '$.requestor',
        [agent.location.id]            NVARCHAR(100)       '$.location.id',
        [agent.location.extension]     NVARCHAR(MAX)       '$.location.extension',
        [agent.location.reference]     NVARCHAR(4000)      '$.location.reference',
        [agent.location.type]          VARCHAR(256)        '$.location.type',
        [agent.location.identifier]    NVARCHAR(MAX)       '$.location.identifier',
        [agent.location.display]       NVARCHAR(4000)      '$.location.display',
        [agent.policy]                 NVARCHAR(MAX)       '$.policy' AS JSON,
        [agent.authorization]          NVARCHAR(MAX)       '$.authorization' AS JSON,
        [agent.network.reference.id]   NVARCHAR(100)       '$.network.reference.id',
        [agent.network.reference.extension] NVARCHAR(MAX)       '$.network.reference.extension',
        [agent.network.reference.reference] NVARCHAR(4000)      '$.network.reference.reference',
        [agent.network.reference.type] VARCHAR(256)        '$.network.reference.type',
        [agent.network.reference.identifier] NVARCHAR(MAX)       '$.network.reference.identifier',
        [agent.network.reference.display] NVARCHAR(4000)      '$.network.reference.display',
        [agent.network.uri]            VARCHAR(256)        '$.network.uri',
        [agent.network.string]         NVARCHAR(4000)      '$.network.string'
    ) j

GO

CREATE VIEW fhir.AuditEventEntity AS
SELECT
    [id],
    [entity.JSON],
    [entity.id],
    [entity.extension],
    [entity.modifierExtension],
    [entity.what.id],
    [entity.what.extension],
    [entity.what.reference],
    [entity.what.type],
    [entity.what.identifier],
    [entity.what.display],
    [entity.role.id],
    [entity.role.extension],
    [entity.role.coding],
    [entity.role.text],
    [entity.securityLabel],
    [entity.query],
    [entity.detail],
    [entity.agent]
FROM openrowset (
        BULK 'AuditEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [entity.JSON]  VARCHAR(MAX) '$.entity'
    ) AS rowset
    CROSS APPLY openjson (rowset.[entity.JSON]) with (
        [entity.id]                    NVARCHAR(100)       '$.id',
        [entity.extension]             NVARCHAR(MAX)       '$.extension',
        [entity.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [entity.what.id]               NVARCHAR(100)       '$.what.id',
        [entity.what.extension]        NVARCHAR(MAX)       '$.what.extension',
        [entity.what.reference]        NVARCHAR(4000)      '$.what.reference',
        [entity.what.type]             VARCHAR(256)        '$.what.type',
        [entity.what.identifier]       NVARCHAR(MAX)       '$.what.identifier',
        [entity.what.display]          NVARCHAR(4000)      '$.what.display',
        [entity.role.id]               NVARCHAR(100)       '$.role.id',
        [entity.role.extension]        NVARCHAR(MAX)       '$.role.extension',
        [entity.role.coding]           NVARCHAR(MAX)       '$.role.coding',
        [entity.role.text]             NVARCHAR(4000)      '$.role.text',
        [entity.securityLabel]         NVARCHAR(MAX)       '$.securityLabel' AS JSON,
        [entity.query]                 NVARCHAR(MAX)       '$.query',
        [entity.detail]                NVARCHAR(MAX)       '$.detail' AS JSON,
        [entity.agent]                 NVARCHAR(MAX)       '$.agent' AS JSON
    ) j
