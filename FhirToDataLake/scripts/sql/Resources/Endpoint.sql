CREATE EXTERNAL TABLE [fhir].[Endpoint] (
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
    [connectionType.id] NVARCHAR(100),
    [connectionType.extension] NVARCHAR(MAX),
    [connectionType.system] VARCHAR(256),
    [connectionType.version] NVARCHAR(100),
    [connectionType.code] NVARCHAR(4000),
    [connectionType.display] NVARCHAR(4000),
    [connectionType.userSelected] bit,
    [name] NVARCHAR(500),
    [managingOrganization.id] NVARCHAR(100),
    [managingOrganization.extension] NVARCHAR(MAX),
    [managingOrganization.reference] NVARCHAR(4000),
    [managingOrganization.type] VARCHAR(256),
    [managingOrganization.identifier.id] NVARCHAR(100),
    [managingOrganization.identifier.extension] NVARCHAR(MAX),
    [managingOrganization.identifier.use] NVARCHAR(64),
    [managingOrganization.identifier.type] NVARCHAR(MAX),
    [managingOrganization.identifier.system] VARCHAR(256),
    [managingOrganization.identifier.value] NVARCHAR(4000),
    [managingOrganization.identifier.period] NVARCHAR(MAX),
    [managingOrganization.identifier.assigner] NVARCHAR(MAX),
    [managingOrganization.display] NVARCHAR(4000),
    [contact] VARCHAR(MAX),
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [payloadType] VARCHAR(MAX),
    [payloadMimeType] VARCHAR(MAX),
    [address] VARCHAR(256),
    [header] VARCHAR(MAX),
) WITH (
    LOCATION='/Endpoint/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.EndpointIdentifier AS
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
        BULK 'Endpoint/**',
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

CREATE VIEW fhir.EndpointContact AS
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
        BULK 'Endpoint/**',
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

CREATE VIEW fhir.EndpointPayloadType AS
SELECT
    [id],
    [payloadType.JSON],
    [payloadType.id],
    [payloadType.extension],
    [payloadType.coding],
    [payloadType.text]
FROM openrowset (
        BULK 'Endpoint/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [payloadType.JSON]  VARCHAR(MAX) '$.payloadType'
    ) AS rowset
    CROSS APPLY openjson (rowset.[payloadType.JSON]) with (
        [payloadType.id]               NVARCHAR(100)       '$.id',
        [payloadType.extension]        NVARCHAR(MAX)       '$.extension',
        [payloadType.coding]           NVARCHAR(MAX)       '$.coding' AS JSON,
        [payloadType.text]             NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.EndpointPayloadMimeType AS
SELECT
    [id],
    [payloadMimeType.JSON],
    [payloadMimeType]
FROM openrowset (
        BULK 'Endpoint/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [payloadMimeType.JSON]  VARCHAR(MAX) '$.payloadMimeType'
    ) AS rowset
    CROSS APPLY openjson (rowset.[payloadMimeType.JSON]) with (
        [payloadMimeType]              NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.EndpointHeader AS
SELECT
    [id],
    [header.JSON],
    [header]
FROM openrowset (
        BULK 'Endpoint/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [header.JSON]  VARCHAR(MAX) '$.header'
    ) AS rowset
    CROSS APPLY openjson (rowset.[header.JSON]) with (
        [header]                       NVARCHAR(MAX)       '$'
    ) j
