CREATE EXTERNAL TABLE [fhir].[Patient] (
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
    [name] VARCHAR(MAX),
    [telecom] VARCHAR(MAX),
    [gender] NVARCHAR(64),
    [birthDate] VARCHAR(64),
    [address] VARCHAR(MAX),
    [maritalStatus.id] NVARCHAR(100),
    [maritalStatus.extension] NVARCHAR(MAX),
    [maritalStatus.coding] VARCHAR(MAX),
    [maritalStatus.text] NVARCHAR(4000),
    [photo] VARCHAR(MAX),
    [contact] VARCHAR(MAX),
    [communication] VARCHAR(MAX),
    [generalPractitioner] VARCHAR(MAX),
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
    [link] VARCHAR(MAX),
    [deceased.boolean] bit,
    [deceased.dateTime] VARCHAR(64),
    [multipleBirth.boolean] bit,
    [multipleBirth.integer] bigint,
) WITH (
    LOCATION='/Patient/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.PatientIdentifier AS
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
        BULK 'Patient/**',
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

CREATE VIEW fhir.PatientName AS
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
        BULK 'Patient/**',
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

CREATE VIEW fhir.PatientTelecom AS
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
        BULK 'Patient/**',
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

CREATE VIEW fhir.PatientAddress AS
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
        BULK 'Patient/**',
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

CREATE VIEW fhir.PatientPhoto AS
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
        BULK 'Patient/**',
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

CREATE VIEW fhir.PatientContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.modifierExtension],
    [contact.relationship],
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
    [contact.address.period],
    [contact.gender],
    [contact.organization.id],
    [contact.organization.extension],
    [contact.organization.reference],
    [contact.organization.type],
    [contact.organization.identifier],
    [contact.organization.display],
    [contact.period.id],
    [contact.period.extension],
    [contact.period.start],
    [contact.period.end]
FROM openrowset (
        BULK 'Patient/**',
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
        [contact.relationship]         NVARCHAR(MAX)       '$.relationship' AS JSON,
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
        [contact.address.period]       NVARCHAR(MAX)       '$.address.period',
        [contact.gender]               NVARCHAR(64)        '$.gender',
        [contact.organization.id]      NVARCHAR(100)       '$.organization.id',
        [contact.organization.extension] NVARCHAR(MAX)       '$.organization.extension',
        [contact.organization.reference] NVARCHAR(4000)      '$.organization.reference',
        [contact.organization.type]    VARCHAR(256)        '$.organization.type',
        [contact.organization.identifier] NVARCHAR(MAX)       '$.organization.identifier',
        [contact.organization.display] NVARCHAR(4000)      '$.organization.display',
        [contact.period.id]            NVARCHAR(100)       '$.period.id',
        [contact.period.extension]     NVARCHAR(MAX)       '$.period.extension',
        [contact.period.start]         VARCHAR(64)         '$.period.start',
        [contact.period.end]           VARCHAR(64)         '$.period.end'
    ) j

GO

CREATE VIEW fhir.PatientCommunication AS
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
        BULK 'Patient/**',
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

GO

CREATE VIEW fhir.PatientGeneralPractitioner AS
SELECT
    [id],
    [generalPractitioner.JSON],
    [generalPractitioner.id],
    [generalPractitioner.extension],
    [generalPractitioner.reference],
    [generalPractitioner.type],
    [generalPractitioner.identifier.id],
    [generalPractitioner.identifier.extension],
    [generalPractitioner.identifier.use],
    [generalPractitioner.identifier.type],
    [generalPractitioner.identifier.system],
    [generalPractitioner.identifier.value],
    [generalPractitioner.identifier.period],
    [generalPractitioner.identifier.assigner],
    [generalPractitioner.display]
FROM openrowset (
        BULK 'Patient/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [generalPractitioner.JSON]  VARCHAR(MAX) '$.generalPractitioner'
    ) AS rowset
    CROSS APPLY openjson (rowset.[generalPractitioner.JSON]) with (
        [generalPractitioner.id]       NVARCHAR(100)       '$.id',
        [generalPractitioner.extension] NVARCHAR(MAX)       '$.extension',
        [generalPractitioner.reference] NVARCHAR(4000)      '$.reference',
        [generalPractitioner.type]     VARCHAR(256)        '$.type',
        [generalPractitioner.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [generalPractitioner.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [generalPractitioner.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [generalPractitioner.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [generalPractitioner.identifier.system] VARCHAR(256)        '$.identifier.system',
        [generalPractitioner.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [generalPractitioner.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [generalPractitioner.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [generalPractitioner.display]  NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.PatientLink AS
SELECT
    [id],
    [link.JSON],
    [link.id],
    [link.extension],
    [link.modifierExtension],
    [link.other.id],
    [link.other.extension],
    [link.other.reference],
    [link.other.type],
    [link.other.identifier],
    [link.other.display],
    [link.type]
FROM openrowset (
        BULK 'Patient/**',
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
        [link.other.id]                NVARCHAR(100)       '$.other.id',
        [link.other.extension]         NVARCHAR(MAX)       '$.other.extension',
        [link.other.reference]         NVARCHAR(4000)      '$.other.reference',
        [link.other.type]              VARCHAR(256)        '$.other.type',
        [link.other.identifier]        NVARCHAR(MAX)       '$.other.identifier',
        [link.other.display]           NVARCHAR(4000)      '$.other.display',
        [link.type]                    NVARCHAR(64)        '$.type'
    ) j
