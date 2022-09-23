CREATE EXTERNAL TABLE [fhir].[InsurancePlan] (
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
    [type] VARCHAR(MAX),
    [name] NVARCHAR(500),
    [alias] VARCHAR(MAX),
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [ownedBy.id] NVARCHAR(100),
    [ownedBy.extension] NVARCHAR(MAX),
    [ownedBy.reference] NVARCHAR(4000),
    [ownedBy.type] VARCHAR(256),
    [ownedBy.identifier.id] NVARCHAR(100),
    [ownedBy.identifier.extension] NVARCHAR(MAX),
    [ownedBy.identifier.use] NVARCHAR(64),
    [ownedBy.identifier.type] NVARCHAR(MAX),
    [ownedBy.identifier.system] VARCHAR(256),
    [ownedBy.identifier.value] NVARCHAR(4000),
    [ownedBy.identifier.period] NVARCHAR(MAX),
    [ownedBy.identifier.assigner] NVARCHAR(MAX),
    [ownedBy.display] NVARCHAR(4000),
    [administeredBy.id] NVARCHAR(100),
    [administeredBy.extension] NVARCHAR(MAX),
    [administeredBy.reference] NVARCHAR(4000),
    [administeredBy.type] VARCHAR(256),
    [administeredBy.identifier.id] NVARCHAR(100),
    [administeredBy.identifier.extension] NVARCHAR(MAX),
    [administeredBy.identifier.use] NVARCHAR(64),
    [administeredBy.identifier.type] NVARCHAR(MAX),
    [administeredBy.identifier.system] VARCHAR(256),
    [administeredBy.identifier.value] NVARCHAR(4000),
    [administeredBy.identifier.period] NVARCHAR(MAX),
    [administeredBy.identifier.assigner] NVARCHAR(MAX),
    [administeredBy.display] NVARCHAR(4000),
    [coverageArea] VARCHAR(MAX),
    [contact] VARCHAR(MAX),
    [endpoint] VARCHAR(MAX),
    [network] VARCHAR(MAX),
    [coverage] VARCHAR(MAX),
    [plan] VARCHAR(MAX),
) WITH (
    LOCATION='/InsurancePlan/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.InsurancePlanIdentifier AS
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
        BULK 'InsurancePlan/**',
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

CREATE VIEW fhir.InsurancePlanType AS
SELECT
    [id],
    [type.JSON],
    [type.id],
    [type.extension],
    [type.coding],
    [type.text]
FROM openrowset (
        BULK 'InsurancePlan/**',
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

CREATE VIEW fhir.InsurancePlanAlias AS
SELECT
    [id],
    [alias.JSON],
    [alias]
FROM openrowset (
        BULK 'InsurancePlan/**',
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

CREATE VIEW fhir.InsurancePlanCoverageArea AS
SELECT
    [id],
    [coverageArea.JSON],
    [coverageArea.id],
    [coverageArea.extension],
    [coverageArea.reference],
    [coverageArea.type],
    [coverageArea.identifier.id],
    [coverageArea.identifier.extension],
    [coverageArea.identifier.use],
    [coverageArea.identifier.type],
    [coverageArea.identifier.system],
    [coverageArea.identifier.value],
    [coverageArea.identifier.period],
    [coverageArea.identifier.assigner],
    [coverageArea.display]
FROM openrowset (
        BULK 'InsurancePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [coverageArea.JSON]  VARCHAR(MAX) '$.coverageArea'
    ) AS rowset
    CROSS APPLY openjson (rowset.[coverageArea.JSON]) with (
        [coverageArea.id]              NVARCHAR(100)       '$.id',
        [coverageArea.extension]       NVARCHAR(MAX)       '$.extension',
        [coverageArea.reference]       NVARCHAR(4000)      '$.reference',
        [coverageArea.type]            VARCHAR(256)        '$.type',
        [coverageArea.identifier.id]   NVARCHAR(100)       '$.identifier.id',
        [coverageArea.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [coverageArea.identifier.use]  NVARCHAR(64)        '$.identifier.use',
        [coverageArea.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [coverageArea.identifier.system] VARCHAR(256)        '$.identifier.system',
        [coverageArea.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [coverageArea.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [coverageArea.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [coverageArea.display]         NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.InsurancePlanContact AS
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
        BULK 'InsurancePlan/**',
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

CREATE VIEW fhir.InsurancePlanEndpoint AS
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
        BULK 'InsurancePlan/**',
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

GO

CREATE VIEW fhir.InsurancePlanNetwork AS
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
        BULK 'InsurancePlan/**',
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

CREATE VIEW fhir.InsurancePlanCoverage AS
SELECT
    [id],
    [coverage.JSON],
    [coverage.id],
    [coverage.extension],
    [coverage.modifierExtension],
    [coverage.type.id],
    [coverage.type.extension],
    [coverage.type.coding],
    [coverage.type.text],
    [coverage.network],
    [coverage.benefit]
FROM openrowset (
        BULK 'InsurancePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [coverage.JSON]  VARCHAR(MAX) '$.coverage'
    ) AS rowset
    CROSS APPLY openjson (rowset.[coverage.JSON]) with (
        [coverage.id]                  NVARCHAR(100)       '$.id',
        [coverage.extension]           NVARCHAR(MAX)       '$.extension',
        [coverage.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [coverage.type.id]             NVARCHAR(100)       '$.type.id',
        [coverage.type.extension]      NVARCHAR(MAX)       '$.type.extension',
        [coverage.type.coding]         NVARCHAR(MAX)       '$.type.coding',
        [coverage.type.text]           NVARCHAR(4000)      '$.type.text',
        [coverage.network]             NVARCHAR(MAX)       '$.network' AS JSON,
        [coverage.benefit]             NVARCHAR(MAX)       '$.benefit' AS JSON
    ) j

GO

CREATE VIEW fhir.InsurancePlanPlan AS
SELECT
    [id],
    [plan.JSON],
    [plan.id],
    [plan.extension],
    [plan.modifierExtension],
    [plan.identifier],
    [plan.type.id],
    [plan.type.extension],
    [plan.type.coding],
    [plan.type.text],
    [plan.coverageArea],
    [plan.network],
    [plan.generalCost],
    [plan.specificCost]
FROM openrowset (
        BULK 'InsurancePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [plan.JSON]  VARCHAR(MAX) '$.plan'
    ) AS rowset
    CROSS APPLY openjson (rowset.[plan.JSON]) with (
        [plan.id]                      NVARCHAR(100)       '$.id',
        [plan.extension]               NVARCHAR(MAX)       '$.extension',
        [plan.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [plan.identifier]              NVARCHAR(MAX)       '$.identifier' AS JSON,
        [plan.type.id]                 NVARCHAR(100)       '$.type.id',
        [plan.type.extension]          NVARCHAR(MAX)       '$.type.extension',
        [plan.type.coding]             NVARCHAR(MAX)       '$.type.coding',
        [plan.type.text]               NVARCHAR(4000)      '$.type.text',
        [plan.coverageArea]            NVARCHAR(MAX)       '$.coverageArea' AS JSON,
        [plan.network]                 NVARCHAR(MAX)       '$.network' AS JSON,
        [plan.generalCost]             NVARCHAR(MAX)       '$.generalCost' AS JSON,
        [plan.specificCost]            NVARCHAR(MAX)       '$.specificCost' AS JSON
    ) j
