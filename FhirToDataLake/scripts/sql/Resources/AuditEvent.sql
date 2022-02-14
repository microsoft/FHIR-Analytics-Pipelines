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
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.system] VARCHAR(256),
    [type.version] NVARCHAR(100),
    [type.code] NVARCHAR(4000),
    [type.display] NVARCHAR(4000),
    [type.userSelected] bit,
    [subtype] VARCHAR(MAX),
    [action] NVARCHAR(64),
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [recorded] VARCHAR(64),
    [outcome] NVARCHAR(64),
    [outcomeDesc] NVARCHAR(4000),
    [purposeOfEvent] VARCHAR(MAX),
    [agent] VARCHAR(MAX),
    [source.id] NVARCHAR(100),
    [source.extension] NVARCHAR(MAX),
    [source.modifierExtension] NVARCHAR(MAX),
    [source.site] NVARCHAR(500),
    [source.observer.id] NVARCHAR(100),
    [source.observer.extension] NVARCHAR(MAX),
    [source.observer.reference] NVARCHAR(4000),
    [source.observer.type] VARCHAR(256),
    [source.observer.identifier] NVARCHAR(MAX),
    [source.observer.display] NVARCHAR(4000),
    [source.type] VARCHAR(MAX),
    [entity] VARCHAR(MAX),
) WITH (
    LOCATION='/AuditEvent/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.AuditEventSubtype AS
SELECT
    [id],
    [subtype.JSON],
    [subtype.id],
    [subtype.extension],
    [subtype.system],
    [subtype.version],
    [subtype.code],
    [subtype.display],
    [subtype.userSelected]
FROM openrowset (
        BULK 'AuditEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subtype.JSON]  VARCHAR(MAX) '$.subtype'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subtype.JSON]) with (
        [subtype.id]                   NVARCHAR(100)       '$.id',
        [subtype.extension]            NVARCHAR(MAX)       '$.extension',
        [subtype.system]               VARCHAR(256)        '$.system',
        [subtype.version]              NVARCHAR(100)       '$.version',
        [subtype.code]                 NVARCHAR(4000)      '$.code',
        [subtype.display]              NVARCHAR(4000)      '$.display',
        [subtype.userSelected]         bit                 '$.userSelected'
    ) j

GO

CREATE VIEW fhir.AuditEventPurposeOfEvent AS
SELECT
    [id],
    [purposeOfEvent.JSON],
    [purposeOfEvent.id],
    [purposeOfEvent.extension],
    [purposeOfEvent.coding],
    [purposeOfEvent.text]
FROM openrowset (
        BULK 'AuditEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [purposeOfEvent.JSON]  VARCHAR(MAX) '$.purposeOfEvent'
    ) AS rowset
    CROSS APPLY openjson (rowset.[purposeOfEvent.JSON]) with (
        [purposeOfEvent.id]            NVARCHAR(100)       '$.id',
        [purposeOfEvent.extension]     NVARCHAR(MAX)       '$.extension',
        [purposeOfEvent.coding]        NVARCHAR(MAX)       '$.coding' AS JSON,
        [purposeOfEvent.text]          NVARCHAR(4000)      '$.text'
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
    [agent.altId],
    [agent.name],
    [agent.requestor],
    [agent.location.id],
    [agent.location.extension],
    [agent.location.reference],
    [agent.location.type],
    [agent.location.identifier],
    [agent.location.display],
    [agent.policy],
    [agent.media.id],
    [agent.media.extension],
    [agent.media.system],
    [agent.media.version],
    [agent.media.code],
    [agent.media.display],
    [agent.media.userSelected],
    [agent.network.id],
    [agent.network.extension],
    [agent.network.modifierExtension],
    [agent.network.address],
    [agent.network.type],
    [agent.purposeOfUse]
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
        [agent.altId]                  NVARCHAR(100)       '$.altId',
        [agent.name]                   NVARCHAR(500)       '$.name',
        [agent.requestor]              bit                 '$.requestor',
        [agent.location.id]            NVARCHAR(100)       '$.location.id',
        [agent.location.extension]     NVARCHAR(MAX)       '$.location.extension',
        [agent.location.reference]     NVARCHAR(4000)      '$.location.reference',
        [agent.location.type]          VARCHAR(256)        '$.location.type',
        [agent.location.identifier]    NVARCHAR(MAX)       '$.location.identifier',
        [agent.location.display]       NVARCHAR(4000)      '$.location.display',
        [agent.policy]                 NVARCHAR(MAX)       '$.policy' AS JSON,
        [agent.media.id]               NVARCHAR(100)       '$.media.id',
        [agent.media.extension]        NVARCHAR(MAX)       '$.media.extension',
        [agent.media.system]           VARCHAR(256)        '$.media.system',
        [agent.media.version]          NVARCHAR(100)       '$.media.version',
        [agent.media.code]             NVARCHAR(4000)      '$.media.code',
        [agent.media.display]          NVARCHAR(4000)      '$.media.display',
        [agent.media.userSelected]     bit                 '$.media.userSelected',
        [agent.network.id]             NVARCHAR(100)       '$.network.id',
        [agent.network.extension]      NVARCHAR(MAX)       '$.network.extension',
        [agent.network.modifierExtension] NVARCHAR(MAX)       '$.network.modifierExtension',
        [agent.network.address]        NVARCHAR(4000)      '$.network.address',
        [agent.network.type]           NVARCHAR(64)        '$.network.type',
        [agent.purposeOfUse]           NVARCHAR(MAX)       '$.purposeOfUse' AS JSON
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
    [entity.type.id],
    [entity.type.extension],
    [entity.type.system],
    [entity.type.version],
    [entity.type.code],
    [entity.type.display],
    [entity.type.userSelected],
    [entity.role.id],
    [entity.role.extension],
    [entity.role.system],
    [entity.role.version],
    [entity.role.code],
    [entity.role.display],
    [entity.role.userSelected],
    [entity.lifecycle.id],
    [entity.lifecycle.extension],
    [entity.lifecycle.system],
    [entity.lifecycle.version],
    [entity.lifecycle.code],
    [entity.lifecycle.display],
    [entity.lifecycle.userSelected],
    [entity.securityLabel],
    [entity.name],
    [entity.description],
    [entity.query],
    [entity.detail]
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
        [entity.type.id]               NVARCHAR(100)       '$.type.id',
        [entity.type.extension]        NVARCHAR(MAX)       '$.type.extension',
        [entity.type.system]           VARCHAR(256)        '$.type.system',
        [entity.type.version]          NVARCHAR(100)       '$.type.version',
        [entity.type.code]             NVARCHAR(4000)      '$.type.code',
        [entity.type.display]          NVARCHAR(4000)      '$.type.display',
        [entity.type.userSelected]     bit                 '$.type.userSelected',
        [entity.role.id]               NVARCHAR(100)       '$.role.id',
        [entity.role.extension]        NVARCHAR(MAX)       '$.role.extension',
        [entity.role.system]           VARCHAR(256)        '$.role.system',
        [entity.role.version]          NVARCHAR(100)       '$.role.version',
        [entity.role.code]             NVARCHAR(4000)      '$.role.code',
        [entity.role.display]          NVARCHAR(4000)      '$.role.display',
        [entity.role.userSelected]     bit                 '$.role.userSelected',
        [entity.lifecycle.id]          NVARCHAR(100)       '$.lifecycle.id',
        [entity.lifecycle.extension]   NVARCHAR(MAX)       '$.lifecycle.extension',
        [entity.lifecycle.system]      VARCHAR(256)        '$.lifecycle.system',
        [entity.lifecycle.version]     NVARCHAR(100)       '$.lifecycle.version',
        [entity.lifecycle.code]        NVARCHAR(4000)      '$.lifecycle.code',
        [entity.lifecycle.display]     NVARCHAR(4000)      '$.lifecycle.display',
        [entity.lifecycle.userSelected] bit                 '$.lifecycle.userSelected',
        [entity.securityLabel]         NVARCHAR(MAX)       '$.securityLabel' AS JSON,
        [entity.name]                  NVARCHAR(500)       '$.name',
        [entity.description]           NVARCHAR(4000)      '$.description',
        [entity.query]                 NVARCHAR(MAX)       '$.query',
        [entity.detail]                NVARCHAR(MAX)       '$.detail' AS JSON
    ) j
