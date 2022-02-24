CREATE EXTERNAL TABLE [fhir].[RelatedPerson] (
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
    [patient.id] NVARCHAR(100),
    [patient.extension] NVARCHAR(MAX),
    [patient.reference] NVARCHAR(4000),
    [patient.type] VARCHAR(256),
    [patient.identifier.id] NVARCHAR(100),
    [patient.identifier.extension] NVARCHAR(MAX),
    [patient.identifier.use] NVARCHAR(64),
    [patient.identifier.type] NVARCHAR(MAX),
    [patient.identifier.system] VARCHAR(256),
    [patient.identifier.value] NVARCHAR(4000),
    [patient.identifier.period] NVARCHAR(MAX),
    [patient.identifier.assigner] NVARCHAR(MAX),
    [patient.display] NVARCHAR(4000),
    [relationship] VARCHAR(MAX),
    [name] VARCHAR(MAX),
    [telecom] VARCHAR(MAX),
    [gender] NVARCHAR(64),
    [birthDate] VARCHAR(64),
    [address] VARCHAR(MAX),
    [photo] VARCHAR(MAX),
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [communication] VARCHAR(MAX),
) WITH (
    LOCATION='/RelatedPerson/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.RelatedPersonIdentifier AS
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
        BULK 'RelatedPerson/**',
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

CREATE VIEW fhir.RelatedPersonRelationship AS
SELECT
    [id],
    [relationship.JSON],
    [relationship.id],
    [relationship.extension],
    [relationship.coding],
    [relationship.text]
FROM openrowset (
        BULK 'RelatedPerson/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [relationship.JSON]  VARCHAR(MAX) '$.relationship'
    ) AS rowset
    CROSS APPLY openjson (rowset.[relationship.JSON]) with (
        [relationship.id]              NVARCHAR(100)       '$.id',
        [relationship.extension]       NVARCHAR(MAX)       '$.extension',
        [relationship.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [relationship.text]            NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.RelatedPersonName AS
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
        BULK 'RelatedPerson/**',
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

CREATE VIEW fhir.RelatedPersonTelecom AS
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
        BULK 'RelatedPerson/**',
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

CREATE VIEW fhir.RelatedPersonAddress AS
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
        BULK 'RelatedPerson/**',
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

CREATE VIEW fhir.RelatedPersonPhoto AS
SELECT
    [id],
    [photo.JSON],
    [photo.id],
    [photo.extension],
    [photo.contentType],
    [photo.language],
    [photo.data],
    [photo.url],
    [photo.size],
    [photo.hash],
    [photo.title],
    [photo.creation]
FROM openrowset (
        BULK 'RelatedPerson/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [photo.JSON]  VARCHAR(MAX) '$.photo'
    ) AS rowset
    CROSS APPLY openjson (rowset.[photo.JSON]) with (
        [photo.id]                     NVARCHAR(100)       '$.id',
        [photo.extension]              NVARCHAR(MAX)       '$.extension',
        [photo.contentType]            NVARCHAR(100)       '$.contentType',
        [photo.language]               NVARCHAR(100)       '$.language',
        [photo.data]                   NVARCHAR(MAX)       '$.data',
        [photo.url]                    VARCHAR(256)        '$.url',
        [photo.size]                   bigint              '$.size',
        [photo.hash]                   NVARCHAR(MAX)       '$.hash',
        [photo.title]                  NVARCHAR(4000)      '$.title',
        [photo.creation]               VARCHAR(64)         '$.creation'
    ) j

GO

CREATE VIEW fhir.RelatedPersonCommunication AS
SELECT
    [id],
    [communication.JSON],
    [communication.id],
    [communication.extension],
    [communication.modifierExtension],
    [communication.language.id],
    [communication.language.extension],
    [communication.language.coding],
    [communication.language.text],
    [communication.preferred]
FROM openrowset (
        BULK 'RelatedPerson/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [communication.JSON]  VARCHAR(MAX) '$.communication'
    ) AS rowset
    CROSS APPLY openjson (rowset.[communication.JSON]) with (
        [communication.id]             NVARCHAR(100)       '$.id',
        [communication.extension]      NVARCHAR(MAX)       '$.extension',
        [communication.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [communication.language.id]    NVARCHAR(100)       '$.language.id',
        [communication.language.extension] NVARCHAR(MAX)       '$.language.extension',
        [communication.language.coding] NVARCHAR(MAX)       '$.language.coding',
        [communication.language.text]  NVARCHAR(4000)      '$.language.text',
        [communication.preferred]      bit                 '$.preferred'
    ) j
