CREATE EXTERNAL TABLE [fhir].[Location] (
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
    [operationalStatus.id] NVARCHAR(100),
    [operationalStatus.extension] NVARCHAR(MAX),
    [operationalStatus.system] VARCHAR(256),
    [operationalStatus.version] NVARCHAR(100),
    [operationalStatus.code] NVARCHAR(4000),
    [operationalStatus.display] NVARCHAR(4000),
    [operationalStatus.userSelected] bit,
    [name] NVARCHAR(500),
    [alias] VARCHAR(MAX),
    [description] NVARCHAR(4000),
    [mode] NVARCHAR(64),
    [type] VARCHAR(MAX),
    [telecom] VARCHAR(MAX),
    [address.id] NVARCHAR(100),
    [address.extension] NVARCHAR(MAX),
    [address.use] NVARCHAR(64),
    [address.type] NVARCHAR(64),
    [address.text] NVARCHAR(4000),
    [address.line] VARCHAR(MAX),
    [address.city] NVARCHAR(500),
    [address.district] NVARCHAR(500),
    [address.state] NVARCHAR(500),
    [address.postalCode] NVARCHAR(100),
    [address.country] NVARCHAR(500),
    [address.period.id] NVARCHAR(100),
    [address.period.extension] NVARCHAR(MAX),
    [address.period.start] VARCHAR(64),
    [address.period.end] VARCHAR(64),
    [physicalType.id] NVARCHAR(100),
    [physicalType.extension] NVARCHAR(MAX),
    [physicalType.coding] VARCHAR(MAX),
    [physicalType.text] NVARCHAR(4000),
    [position.id] NVARCHAR(100),
    [position.extension] NVARCHAR(MAX),
    [position.modifierExtension] NVARCHAR(MAX),
    [position.longitude] float,
    [position.latitude] float,
    [position.altitude] float,
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
    [hoursOfOperation] VARCHAR(MAX),
    [availabilityExceptions] NVARCHAR(4000),
    [endpoint] VARCHAR(MAX),
) WITH (
    LOCATION='/Location/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.LocationIdentifier AS
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
        BULK 'Location/**',
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

CREATE VIEW fhir.LocationAlias AS
SELECT
    [id],
    [alias.JSON],
    [alias]
FROM openrowset (
        BULK 'Location/**',
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

CREATE VIEW fhir.LocationType AS
SELECT
    [id],
    [type.JSON],
    [type.id],
    [type.extension],
    [type.coding],
    [type.text]
FROM openrowset (
        BULK 'Location/**',
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

CREATE VIEW fhir.LocationTelecom AS
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
        BULK 'Location/**',
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

CREATE VIEW fhir.LocationHoursOfOperation AS
SELECT
    [id],
    [hoursOfOperation.JSON],
    [hoursOfOperation.id],
    [hoursOfOperation.extension],
    [hoursOfOperation.modifierExtension],
    [hoursOfOperation.daysOfWeek],
    [hoursOfOperation.allDay],
    [hoursOfOperation.openingTime],
    [hoursOfOperation.closingTime]
FROM openrowset (
        BULK 'Location/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [hoursOfOperation.JSON]  VARCHAR(MAX) '$.hoursOfOperation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[hoursOfOperation.JSON]) with (
        [hoursOfOperation.id]          NVARCHAR(100)       '$.id',
        [hoursOfOperation.extension]   NVARCHAR(MAX)       '$.extension',
        [hoursOfOperation.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [hoursOfOperation.daysOfWeek]  NVARCHAR(MAX)       '$.daysOfWeek' AS JSON,
        [hoursOfOperation.allDay]      bit                 '$.allDay',
        [hoursOfOperation.openingTime] NVARCHAR(MAX)       '$.openingTime',
        [hoursOfOperation.closingTime] NVARCHAR(MAX)       '$.closingTime'
    ) j

GO

CREATE VIEW fhir.LocationEndpoint AS
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
        BULK 'Location/**',
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
