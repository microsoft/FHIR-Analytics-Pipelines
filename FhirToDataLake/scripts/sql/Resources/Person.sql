CREATE EXTERNAL TABLE [fhir].[Person] (
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
    [name] VARCHAR(MAX),
    [telecom] VARCHAR(MAX),
    [gender] NVARCHAR(64),
    [birthDate] VARCHAR(64),
    [address] VARCHAR(MAX),
    [photo.id] NVARCHAR(100),
    [photo.extension] NVARCHAR(MAX),
    [photo.contentType] NVARCHAR(100),
    [photo.language] NVARCHAR(100),
    [photo.data] NVARCHAR(MAX),
    [photo.url] VARCHAR(256),
    [photo.size] bigint,
    [photo.hash] NVARCHAR(MAX),
    [photo.title] NVARCHAR(4000),
    [photo.creation] VARCHAR(64),
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
    [active] bit,
    [link] VARCHAR(MAX),
) WITH (
    LOCATION='/Person/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.PersonIdentifier AS
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
        BULK 'Person/**',
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

CREATE VIEW fhir.PersonName AS
SELECT
    [id],
    [name.JSON],
    [name.id],
    [name.extension],
    [name.use],
    [name.text],
    [name.family],
    [name.given],
    [name.prefix],
    [name.suffix],
    [name.period.id],
    [name.period.extension],
    [name.period.start],
    [name.period.end]
FROM openrowset (
        BULK 'Person/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [name.JSON]  VARCHAR(MAX) '$.name'
    ) AS rowset
    CROSS APPLY openjson (rowset.[name.JSON]) with (
        [name.id]                      NVARCHAR(100)       '$.id',
        [name.extension]               NVARCHAR(MAX)       '$.extension',
        [name.use]                     NVARCHAR(64)        '$.use',
        [name.text]                    NVARCHAR(4000)      '$.text',
        [name.family]                  NVARCHAR(500)       '$.family',
        [name.given]                   NVARCHAR(MAX)       '$.given' AS JSON,
        [name.prefix]                  NVARCHAR(MAX)       '$.prefix' AS JSON,
        [name.suffix]                  NVARCHAR(MAX)       '$.suffix' AS JSON,
        [name.period.id]               NVARCHAR(100)       '$.period.id',
        [name.period.extension]        NVARCHAR(MAX)       '$.period.extension',
        [name.period.start]            VARCHAR(64)         '$.period.start',
        [name.period.end]              VARCHAR(64)         '$.period.end'
    ) j

GO

CREATE VIEW fhir.PersonTelecom AS
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
        BULK 'Person/**',
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

CREATE VIEW fhir.PersonAddress AS
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
        BULK 'Person/**',
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

CREATE VIEW fhir.PersonLink AS
SELECT
    [id],
    [link.JSON],
    [link.id],
    [link.extension],
    [link.modifierExtension],
    [link.target.id],
    [link.target.extension],
    [link.target.reference],
    [link.target.type],
    [link.target.identifier],
    [link.target.display],
    [link.assurance]
FROM openrowset (
        BULK 'Person/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [link.JSON]  VARCHAR(MAX) '$.link'
    ) AS rowset
    CROSS APPLY openjson (rowset.[link.JSON]) with (
        [link.id]                      NVARCHAR(100)       '$.id',
        [link.extension]               NVARCHAR(MAX)       '$.extension',
        [link.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [link.target.id]               NVARCHAR(100)       '$.target.id',
        [link.target.extension]        NVARCHAR(MAX)       '$.target.extension',
        [link.target.reference]        NVARCHAR(4000)      '$.target.reference',
        [link.target.type]             VARCHAR(256)        '$.target.type',
        [link.target.identifier]       NVARCHAR(MAX)       '$.target.identifier',
        [link.target.display]          NVARCHAR(4000)      '$.target.display',
        [link.assurance]               NVARCHAR(64)        '$.assurance'
    ) j
