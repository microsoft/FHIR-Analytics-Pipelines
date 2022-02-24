CREATE EXTERNAL TABLE [fhir].[Organization] (
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
    [active] bit,
    [type] VARCHAR(MAX),
    [name] NVARCHAR(500),
    [alias] VARCHAR(MAX),
    [telecom] VARCHAR(MAX),
    [address] VARCHAR(MAX),
    [partOf.id] NVARCHAR(100),
    [partOf.extension] NVARCHAR(MAX),
    [partOf.reference] NVARCHAR(4000),
    [partOf.type] VARCHAR(256),
    [partOf.identifier.id] NVARCHAR(100),
    [partOf.identifier.extension] NVARCHAR(MAX),
    [partOf.identifier.use] NVARCHAR(64),
    [partOf.identifier.type] NVARCHAR(MAX),
    [partOf.identifier.system] VARCHAR(256),
    [partOf.identifier.value] NVARCHAR(4000),
    [partOf.identifier.period] NVARCHAR(MAX),
    [partOf.identifier.assigner] NVARCHAR(MAX),
    [partOf.display] NVARCHAR(4000),
    [contact] VARCHAR(MAX),
    [endpoint] VARCHAR(MAX),
) WITH (
    LOCATION='/Organization/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.OrganizationIdentifier AS
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
        BULK 'Organization/**',
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

CREATE VIEW fhir.OrganizationType AS
SELECT
    [id],
    [type.JSON],
    [type.id],
    [type.extension],
    [type.coding],
    [type.text]
FROM openrowset (
        BULK 'Organization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [type.JSON]  VARCHAR(MAX) '$.type'
    ) AS rowset
    CROSS APPLY openjson (rowset.[type.JSON]) with (
        [type.id]                      NVARCHAR(100)       '$.id',
        [type.extension]               NVARCHAR(MAX)       '$.extension',
        [type.coding]                  NVARCHAR(MAX)       '$.coding' AS JSON,
        [type.text]                    NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.OrganizationAlias AS
SELECT
    [id],
    [alias.JSON],
    [alias]
FROM openrowset (
        BULK 'Organization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [alias.JSON]  VARCHAR(MAX) '$.alias'
    ) AS rowset
    CROSS APPLY openjson (rowset.[alias.JSON]) with (
        [alias]                        NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.OrganizationTelecom AS
SELECT
    [id],
    [telecom.JSON],
    [telecom.id],
    [telecom.extension],
    [telecom.system],
    [telecom.value],
    [telecom.use],
    [telecom.rank],
    [telecom.period.id],
    [telecom.period.extension],
    [telecom.period.start],
    [telecom.period.end]
FROM openrowset (
        BULK 'Organization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [telecom.JSON]  VARCHAR(MAX) '$.telecom'
    ) AS rowset
    CROSS APPLY openjson (rowset.[telecom.JSON]) with (
        [telecom.id]                   NVARCHAR(100)       '$.id',
        [telecom.extension]            NVARCHAR(MAX)       '$.extension',
        [telecom.system]               NVARCHAR(64)        '$.system',
        [telecom.value]                NVARCHAR(4000)      '$.value',
        [telecom.use]                  NVARCHAR(64)        '$.use',
        [telecom.rank]                 bigint              '$.rank',
        [telecom.period.id]            NVARCHAR(100)       '$.period.id',
        [telecom.period.extension]     NVARCHAR(MAX)       '$.period.extension',
        [telecom.period.start]         VARCHAR(64)         '$.period.start',
        [telecom.period.end]           VARCHAR(64)         '$.period.end'
    ) j

GO

CREATE VIEW fhir.OrganizationAddress AS
SELECT
    [id],
    [address.JSON],
    [address.id],
    [address.extension],
    [address.use],
    [address.type],
    [address.text],
    [address.line],
    [address.city],
    [address.district],
    [address.state],
    [address.postalCode],
    [address.country],
    [address.period.id],
    [address.period.extension],
    [address.period.start],
    [address.period.end]
FROM openrowset (
        BULK 'Organization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [address.JSON]  VARCHAR(MAX) '$.address'
    ) AS rowset
    CROSS APPLY openjson (rowset.[address.JSON]) with (
        [address.id]                   NVARCHAR(100)       '$.id',
        [address.extension]            NVARCHAR(MAX)       '$.extension',
        [address.use]                  NVARCHAR(64)        '$.use',
        [address.type]                 NVARCHAR(64)        '$.type',
        [address.text]                 NVARCHAR(4000)      '$.text',
        [address.line]                 NVARCHAR(MAX)       '$.line' AS JSON,
        [address.city]                 NVARCHAR(500)       '$.city',
        [address.district]             NVARCHAR(500)       '$.district',
        [address.state]                NVARCHAR(500)       '$.state',
        [address.postalCode]           NVARCHAR(100)       '$.postalCode',
        [address.country]              NVARCHAR(500)       '$.country',
        [address.period.id]            NVARCHAR(100)       '$.period.id',
        [address.period.extension]     NVARCHAR(MAX)       '$.period.extension',
        [address.period.start]         VARCHAR(64)         '$.period.start',
        [address.period.end]           VARCHAR(64)         '$.period.end'
    ) j

GO

CREATE VIEW fhir.OrganizationContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.modifierExtension],
    [contact.purpose.id],
    [contact.purpose.extension],
    [contact.purpose.coding],
    [contact.purpose.text],
    [contact.name.id],
    [contact.name.extension],
    [contact.name.use],
    [contact.name.text],
    [contact.name.family],
    [contact.name.given],
    [contact.name.prefix],
    [contact.name.suffix],
    [contact.name.period],
    [contact.telecom],
    [contact.address.id],
    [contact.address.extension],
    [contact.address.use],
    [contact.address.type],
    [contact.address.text],
    [contact.address.line],
    [contact.address.city],
    [contact.address.district],
    [contact.address.state],
    [contact.address.postalCode],
    [contact.address.country],
    [contact.address.period]
FROM openrowset (
        BULK 'Organization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contact.JSON]  VARCHAR(MAX) '$.contact'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contact.JSON]) with (
        [contact.id]                   NVARCHAR(100)       '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [contact.purpose.id]           NVARCHAR(100)       '$.purpose.id',
        [contact.purpose.extension]    NVARCHAR(MAX)       '$.purpose.extension',
        [contact.purpose.coding]       NVARCHAR(MAX)       '$.purpose.coding',
        [contact.purpose.text]         NVARCHAR(4000)      '$.purpose.text',
        [contact.name.id]              NVARCHAR(100)       '$.name.id',
        [contact.name.extension]       NVARCHAR(MAX)       '$.name.extension',
        [contact.name.use]             NVARCHAR(64)        '$.name.use',
        [contact.name.text]            NVARCHAR(4000)      '$.name.text',
        [contact.name.family]          NVARCHAR(500)       '$.name.family',
        [contact.name.given]           NVARCHAR(MAX)       '$.name.given',
        [contact.name.prefix]          NVARCHAR(MAX)       '$.name.prefix',
        [contact.name.suffix]          NVARCHAR(MAX)       '$.name.suffix',
        [contact.name.period]          NVARCHAR(MAX)       '$.name.period',
        [contact.telecom]              NVARCHAR(MAX)       '$.telecom' AS JSON,
        [contact.address.id]           NVARCHAR(100)       '$.address.id',
        [contact.address.extension]    NVARCHAR(MAX)       '$.address.extension',
        [contact.address.use]          NVARCHAR(64)        '$.address.use',
        [contact.address.type]         NVARCHAR(64)        '$.address.type',
        [contact.address.text]         NVARCHAR(4000)      '$.address.text',
        [contact.address.line]         NVARCHAR(MAX)       '$.address.line',
        [contact.address.city]         NVARCHAR(500)       '$.address.city',
        [contact.address.district]     NVARCHAR(500)       '$.address.district',
        [contact.address.state]        NVARCHAR(500)       '$.address.state',
        [contact.address.postalCode]   NVARCHAR(100)       '$.address.postalCode',
        [contact.address.country]      NVARCHAR(500)       '$.address.country',
        [contact.address.period]       NVARCHAR(MAX)       '$.address.period'
    ) j

GO

CREATE VIEW fhir.OrganizationEndpoint AS
SELECT
    [id],
    [endpoint.JSON],
    [endpoint.id],
    [endpoint.extension],
    [endpoint.reference],
    [endpoint.type],
    [endpoint.identifier.id],
    [endpoint.identifier.extension],
    [endpoint.identifier.use],
    [endpoint.identifier.type],
    [endpoint.identifier.system],
    [endpoint.identifier.value],
    [endpoint.identifier.period],
    [endpoint.identifier.assigner],
    [endpoint.display]
FROM openrowset (
        BULK 'Organization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [endpoint.JSON]  VARCHAR(MAX) '$.endpoint'
    ) AS rowset
    CROSS APPLY openjson (rowset.[endpoint.JSON]) with (
        [endpoint.id]                  NVARCHAR(100)       '$.id',
        [endpoint.extension]           NVARCHAR(MAX)       '$.extension',
        [endpoint.reference]           NVARCHAR(4000)      '$.reference',
        [endpoint.type]                VARCHAR(256)        '$.type',
        [endpoint.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [endpoint.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [endpoint.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [endpoint.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [endpoint.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [endpoint.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [endpoint.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [endpoint.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [endpoint.display]             NVARCHAR(4000)      '$.display'
    ) j
