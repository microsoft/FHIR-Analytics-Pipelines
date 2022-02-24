CREATE EXTERNAL TABLE [fhir].[OrganizationAffiliation] (
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
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [organization.id] NVARCHAR(100),
    [organization.extension] NVARCHAR(MAX),
    [organization.reference] NVARCHAR(4000),
    [organization.type] VARCHAR(256),
    [organization.identifier.id] NVARCHAR(100),
    [organization.identifier.extension] NVARCHAR(MAX),
    [organization.identifier.use] NVARCHAR(64),
    [organization.identifier.type] NVARCHAR(MAX),
    [organization.identifier.system] VARCHAR(256),
    [organization.identifier.value] NVARCHAR(4000),
    [organization.identifier.period] NVARCHAR(MAX),
    [organization.identifier.assigner] NVARCHAR(MAX),
    [organization.display] NVARCHAR(4000),
    [participatingOrganization.id] NVARCHAR(100),
    [participatingOrganization.extension] NVARCHAR(MAX),
    [participatingOrganization.reference] NVARCHAR(4000),
    [participatingOrganization.type] VARCHAR(256),
    [participatingOrganization.identifier.id] NVARCHAR(100),
    [participatingOrganization.identifier.extension] NVARCHAR(MAX),
    [participatingOrganization.identifier.use] NVARCHAR(64),
    [participatingOrganization.identifier.type] NVARCHAR(MAX),
    [participatingOrganization.identifier.system] VARCHAR(256),
    [participatingOrganization.identifier.value] NVARCHAR(4000),
    [participatingOrganization.identifier.period] NVARCHAR(MAX),
    [participatingOrganization.identifier.assigner] NVARCHAR(MAX),
    [participatingOrganization.display] NVARCHAR(4000),
    [network] VARCHAR(MAX),
    [code] VARCHAR(MAX),
    [specialty] VARCHAR(MAX),
    [location] VARCHAR(MAX),
    [healthcareService] VARCHAR(MAX),
    [telecom] VARCHAR(MAX),
    [endpoint] VARCHAR(MAX),
) WITH (
    LOCATION='/OrganizationAffiliation/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.OrganizationAffiliationIdentifier AS
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
        BULK 'OrganizationAffiliation/**',
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

CREATE VIEW fhir.OrganizationAffiliationNetwork AS
SELECT
    [id],
    [network.JSON],
    [network.id],
    [network.extension],
    [network.reference],
    [network.type],
    [network.identifier.id],
    [network.identifier.extension],
    [network.identifier.use],
    [network.identifier.type],
    [network.identifier.system],
    [network.identifier.value],
    [network.identifier.period],
    [network.identifier.assigner],
    [network.display]
FROM openrowset (
        BULK 'OrganizationAffiliation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [network.JSON]  VARCHAR(MAX) '$.network'
    ) AS rowset
    CROSS APPLY openjson (rowset.[network.JSON]) with (
        [network.id]                   NVARCHAR(100)       '$.id',
        [network.extension]            NVARCHAR(MAX)       '$.extension',
        [network.reference]            NVARCHAR(4000)      '$.reference',
        [network.type]                 VARCHAR(256)        '$.type',
        [network.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [network.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [network.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [network.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [network.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [network.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [network.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [network.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [network.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.OrganizationAffiliationCode AS
SELECT
    [id],
    [code.JSON],
    [code.id],
    [code.extension],
    [code.coding],
    [code.text]
FROM openrowset (
        BULK 'OrganizationAffiliation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [code.JSON]  VARCHAR(MAX) '$.code'
    ) AS rowset
    CROSS APPLY openjson (rowset.[code.JSON]) with (
        [code.id]                      NVARCHAR(100)       '$.id',
        [code.extension]               NVARCHAR(MAX)       '$.extension',
        [code.coding]                  NVARCHAR(MAX)       '$.coding' AS JSON,
        [code.text]                    NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.OrganizationAffiliationSpecialty AS
SELECT
    [id],
    [specialty.JSON],
    [specialty.id],
    [specialty.extension],
    [specialty.coding],
    [specialty.text]
FROM openrowset (
        BULK 'OrganizationAffiliation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specialty.JSON]  VARCHAR(MAX) '$.specialty'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specialty.JSON]) with (
        [specialty.id]                 NVARCHAR(100)       '$.id',
        [specialty.extension]          NVARCHAR(MAX)       '$.extension',
        [specialty.coding]             NVARCHAR(MAX)       '$.coding' AS JSON,
        [specialty.text]               NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.OrganizationAffiliationLocation AS
SELECT
    [id],
    [location.JSON],
    [location.id],
    [location.extension],
    [location.reference],
    [location.type],
    [location.identifier.id],
    [location.identifier.extension],
    [location.identifier.use],
    [location.identifier.type],
    [location.identifier.system],
    [location.identifier.value],
    [location.identifier.period],
    [location.identifier.assigner],
    [location.display]
FROM openrowset (
        BULK 'OrganizationAffiliation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [location.JSON]  VARCHAR(MAX) '$.location'
    ) AS rowset
    CROSS APPLY openjson (rowset.[location.JSON]) with (
        [location.id]                  NVARCHAR(100)       '$.id',
        [location.extension]           NVARCHAR(MAX)       '$.extension',
        [location.reference]           NVARCHAR(4000)      '$.reference',
        [location.type]                VARCHAR(256)        '$.type',
        [location.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [location.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [location.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [location.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [location.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [location.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [location.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [location.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [location.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.OrganizationAffiliationHealthcareService AS
SELECT
    [id],
    [healthcareService.JSON],
    [healthcareService.id],
    [healthcareService.extension],
    [healthcareService.reference],
    [healthcareService.type],
    [healthcareService.identifier.id],
    [healthcareService.identifier.extension],
    [healthcareService.identifier.use],
    [healthcareService.identifier.type],
    [healthcareService.identifier.system],
    [healthcareService.identifier.value],
    [healthcareService.identifier.period],
    [healthcareService.identifier.assigner],
    [healthcareService.display]
FROM openrowset (
        BULK 'OrganizationAffiliation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [healthcareService.JSON]  VARCHAR(MAX) '$.healthcareService'
    ) AS rowset
    CROSS APPLY openjson (rowset.[healthcareService.JSON]) with (
        [healthcareService.id]         NVARCHAR(100)       '$.id',
        [healthcareService.extension]  NVARCHAR(MAX)       '$.extension',
        [healthcareService.reference]  NVARCHAR(4000)      '$.reference',
        [healthcareService.type]       VARCHAR(256)        '$.type',
        [healthcareService.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [healthcareService.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [healthcareService.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [healthcareService.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [healthcareService.identifier.system] VARCHAR(256)        '$.identifier.system',
        [healthcareService.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [healthcareService.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [healthcareService.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [healthcareService.display]    NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.OrganizationAffiliationTelecom AS
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
        BULK 'OrganizationAffiliation/**',
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

CREATE VIEW fhir.OrganizationAffiliationEndpoint AS
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
        BULK 'OrganizationAffiliation/**',
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
