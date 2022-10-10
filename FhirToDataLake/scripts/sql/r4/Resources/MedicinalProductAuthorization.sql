CREATE EXTERNAL TABLE [fhir].[MedicinalProductAuthorization] (
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
    [subject.id] NVARCHAR(100),
    [subject.extension] NVARCHAR(MAX),
    [subject.reference] NVARCHAR(4000),
    [subject.type] VARCHAR(256),
    [subject.identifier.id] NVARCHAR(100),
    [subject.identifier.extension] NVARCHAR(MAX),
    [subject.identifier.use] NVARCHAR(64),
    [subject.identifier.type] NVARCHAR(MAX),
    [subject.identifier.system] VARCHAR(256),
    [subject.identifier.value] NVARCHAR(4000),
    [subject.identifier.period] NVARCHAR(MAX),
    [subject.identifier.assigner] NVARCHAR(MAX),
    [subject.display] NVARCHAR(4000),
    [country] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [status.id] NVARCHAR(100),
    [status.extension] NVARCHAR(MAX),
    [status.coding] VARCHAR(MAX),
    [status.text] NVARCHAR(4000),
    [statusDate] VARCHAR(64),
    [restoreDate] VARCHAR(64),
    [validityPeriod.id] NVARCHAR(100),
    [validityPeriod.extension] NVARCHAR(MAX),
    [validityPeriod.start] VARCHAR(64),
    [validityPeriod.end] VARCHAR(64),
    [dataExclusivityPeriod.id] NVARCHAR(100),
    [dataExclusivityPeriod.extension] NVARCHAR(MAX),
    [dataExclusivityPeriod.start] VARCHAR(64),
    [dataExclusivityPeriod.end] VARCHAR(64),
    [dateOfFirstAuthorization] VARCHAR(64),
    [internationalBirthDate] VARCHAR(64),
    [legalBasis.id] NVARCHAR(100),
    [legalBasis.extension] NVARCHAR(MAX),
    [legalBasis.coding] VARCHAR(MAX),
    [legalBasis.text] NVARCHAR(4000),
    [jurisdictionalAuthorization] VARCHAR(MAX),
    [holder.id] NVARCHAR(100),
    [holder.extension] NVARCHAR(MAX),
    [holder.reference] NVARCHAR(4000),
    [holder.type] VARCHAR(256),
    [holder.identifier.id] NVARCHAR(100),
    [holder.identifier.extension] NVARCHAR(MAX),
    [holder.identifier.use] NVARCHAR(64),
    [holder.identifier.type] NVARCHAR(MAX),
    [holder.identifier.system] VARCHAR(256),
    [holder.identifier.value] NVARCHAR(4000),
    [holder.identifier.period] NVARCHAR(MAX),
    [holder.identifier.assigner] NVARCHAR(MAX),
    [holder.display] NVARCHAR(4000),
    [regulator.id] NVARCHAR(100),
    [regulator.extension] NVARCHAR(MAX),
    [regulator.reference] NVARCHAR(4000),
    [regulator.type] VARCHAR(256),
    [regulator.identifier.id] NVARCHAR(100),
    [regulator.identifier.extension] NVARCHAR(MAX),
    [regulator.identifier.use] NVARCHAR(64),
    [regulator.identifier.type] NVARCHAR(MAX),
    [regulator.identifier.system] VARCHAR(256),
    [regulator.identifier.value] NVARCHAR(4000),
    [regulator.identifier.period] NVARCHAR(MAX),
    [regulator.identifier.assigner] NVARCHAR(MAX),
    [regulator.display] NVARCHAR(4000),
    [procedure.id] NVARCHAR(100),
    [procedure.extension] NVARCHAR(MAX),
    [procedure.modifierExtension] NVARCHAR(MAX),
    [procedure.identifier.id] NVARCHAR(100),
    [procedure.identifier.extension] NVARCHAR(MAX),
    [procedure.identifier.use] NVARCHAR(64),
    [procedure.identifier.type] NVARCHAR(MAX),
    [procedure.identifier.system] VARCHAR(256),
    [procedure.identifier.value] NVARCHAR(4000),
    [procedure.identifier.period] NVARCHAR(MAX),
    [procedure.identifier.assigner] NVARCHAR(MAX),
    [procedure.type.id] NVARCHAR(100),
    [procedure.type.extension] NVARCHAR(MAX),
    [procedure.type.coding] NVARCHAR(MAX),
    [procedure.type.text] NVARCHAR(4000),
    [procedure.application] VARCHAR(MAX),
    [procedure.date.period.id] NVARCHAR(100),
    [procedure.date.period.extension] NVARCHAR(MAX),
    [procedure.date.period.start] VARCHAR(64),
    [procedure.date.period.end] VARCHAR(64),
    [procedure.date.dateTime] VARCHAR(64),
) WITH (
    LOCATION='/MedicinalProductAuthorization/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicinalProductAuthorizationIdentifier AS
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
        BULK 'MedicinalProductAuthorization/**',
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

CREATE VIEW fhir.MedicinalProductAuthorizationCountry AS
SELECT
    [id],
    [country.JSON],
    [country.id],
    [country.extension],
    [country.coding],
    [country.text]
FROM openrowset (
        BULK 'MedicinalProductAuthorization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [country.JSON]  VARCHAR(MAX) '$.country'
    ) AS rowset
    CROSS APPLY openjson (rowset.[country.JSON]) with (
        [country.id]                   NVARCHAR(100)       '$.id',
        [country.extension]            NVARCHAR(MAX)       '$.extension',
        [country.coding]               NVARCHAR(MAX)       '$.coding' AS JSON,
        [country.text]                 NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.MedicinalProductAuthorizationJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'MedicinalProductAuthorization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [jurisdiction.JSON]  VARCHAR(MAX) '$.jurisdiction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[jurisdiction.JSON]) with (
        [jurisdiction.id]              NVARCHAR(100)       '$.id',
        [jurisdiction.extension]       NVARCHAR(MAX)       '$.extension',
        [jurisdiction.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [jurisdiction.text]            NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.MedicinalProductAuthorizationJurisdictionalAuthorization AS
SELECT
    [id],
    [jurisdictionalAuthorization.JSON],
    [jurisdictionalAuthorization.id],
    [jurisdictionalAuthorization.extension],
    [jurisdictionalAuthorization.modifierExtension],
    [jurisdictionalAuthorization.identifier],
    [jurisdictionalAuthorization.country.id],
    [jurisdictionalAuthorization.country.extension],
    [jurisdictionalAuthorization.country.coding],
    [jurisdictionalAuthorization.country.text],
    [jurisdictionalAuthorization.jurisdiction],
    [jurisdictionalAuthorization.legalStatusOfSupply.id],
    [jurisdictionalAuthorization.legalStatusOfSupply.extension],
    [jurisdictionalAuthorization.legalStatusOfSupply.coding],
    [jurisdictionalAuthorization.legalStatusOfSupply.text],
    [jurisdictionalAuthorization.validityPeriod.id],
    [jurisdictionalAuthorization.validityPeriod.extension],
    [jurisdictionalAuthorization.validityPeriod.start],
    [jurisdictionalAuthorization.validityPeriod.end]
FROM openrowset (
        BULK 'MedicinalProductAuthorization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [jurisdictionalAuthorization.JSON]  VARCHAR(MAX) '$.jurisdictionalAuthorization'
    ) AS rowset
    CROSS APPLY openjson (rowset.[jurisdictionalAuthorization.JSON]) with (
        [jurisdictionalAuthorization.id] NVARCHAR(100)       '$.id',
        [jurisdictionalAuthorization.extension] NVARCHAR(MAX)       '$.extension',
        [jurisdictionalAuthorization.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [jurisdictionalAuthorization.identifier] NVARCHAR(MAX)       '$.identifier' AS JSON,
        [jurisdictionalAuthorization.country.id] NVARCHAR(100)       '$.country.id',
        [jurisdictionalAuthorization.country.extension] NVARCHAR(MAX)       '$.country.extension',
        [jurisdictionalAuthorization.country.coding] NVARCHAR(MAX)       '$.country.coding',
        [jurisdictionalAuthorization.country.text] NVARCHAR(4000)      '$.country.text',
        [jurisdictionalAuthorization.jurisdiction] NVARCHAR(MAX)       '$.jurisdiction' AS JSON,
        [jurisdictionalAuthorization.legalStatusOfSupply.id] NVARCHAR(100)       '$.legalStatusOfSupply.id',
        [jurisdictionalAuthorization.legalStatusOfSupply.extension] NVARCHAR(MAX)       '$.legalStatusOfSupply.extension',
        [jurisdictionalAuthorization.legalStatusOfSupply.coding] NVARCHAR(MAX)       '$.legalStatusOfSupply.coding',
        [jurisdictionalAuthorization.legalStatusOfSupply.text] NVARCHAR(4000)      '$.legalStatusOfSupply.text',
        [jurisdictionalAuthorization.validityPeriod.id] NVARCHAR(100)       '$.validityPeriod.id',
        [jurisdictionalAuthorization.validityPeriod.extension] NVARCHAR(MAX)       '$.validityPeriod.extension',
        [jurisdictionalAuthorization.validityPeriod.start] VARCHAR(64)         '$.validityPeriod.start',
        [jurisdictionalAuthorization.validityPeriod.end] VARCHAR(64)         '$.validityPeriod.end'
    ) j
