CREATE EXTERNAL TABLE [fhir].[CoverageEligibilityResponse] (
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
    [status] NVARCHAR(100),
    [purpose] VARCHAR(MAX),
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
    [created] VARCHAR(64),
    [requestor.id] NVARCHAR(100),
    [requestor.extension] NVARCHAR(MAX),
    [requestor.reference] NVARCHAR(4000),
    [requestor.type] VARCHAR(256),
    [requestor.identifier.id] NVARCHAR(100),
    [requestor.identifier.extension] NVARCHAR(MAX),
    [requestor.identifier.use] NVARCHAR(64),
    [requestor.identifier.type] NVARCHAR(MAX),
    [requestor.identifier.system] VARCHAR(256),
    [requestor.identifier.value] NVARCHAR(4000),
    [requestor.identifier.period] NVARCHAR(MAX),
    [requestor.identifier.assigner] NVARCHAR(MAX),
    [requestor.display] NVARCHAR(4000),
    [request.id] NVARCHAR(100),
    [request.extension] NVARCHAR(MAX),
    [request.reference] NVARCHAR(4000),
    [request.type] VARCHAR(256),
    [request.identifier.id] NVARCHAR(100),
    [request.identifier.extension] NVARCHAR(MAX),
    [request.identifier.use] NVARCHAR(64),
    [request.identifier.type] NVARCHAR(MAX),
    [request.identifier.system] VARCHAR(256),
    [request.identifier.value] NVARCHAR(4000),
    [request.identifier.period] NVARCHAR(MAX),
    [request.identifier.assigner] NVARCHAR(MAX),
    [request.display] NVARCHAR(4000),
    [outcome] NVARCHAR(64),
    [disposition] NVARCHAR(4000),
    [insurer.id] NVARCHAR(100),
    [insurer.extension] NVARCHAR(MAX),
    [insurer.reference] NVARCHAR(4000),
    [insurer.type] VARCHAR(256),
    [insurer.identifier.id] NVARCHAR(100),
    [insurer.identifier.extension] NVARCHAR(MAX),
    [insurer.identifier.use] NVARCHAR(64),
    [insurer.identifier.type] NVARCHAR(MAX),
    [insurer.identifier.system] VARCHAR(256),
    [insurer.identifier.value] NVARCHAR(4000),
    [insurer.identifier.period] NVARCHAR(MAX),
    [insurer.identifier.assigner] NVARCHAR(MAX),
    [insurer.display] NVARCHAR(4000),
    [insurance] VARCHAR(MAX),
    [preAuthRef] NVARCHAR(4000),
    [form.id] NVARCHAR(100),
    [form.extension] NVARCHAR(MAX),
    [form.coding] VARCHAR(MAX),
    [form.text] NVARCHAR(4000),
    [error] VARCHAR(MAX),
    [serviced.date] VARCHAR(64),
    [serviced.period.id] NVARCHAR(100),
    [serviced.period.extension] NVARCHAR(MAX),
    [serviced.period.start] VARCHAR(64),
    [serviced.period.end] VARCHAR(64),
) WITH (
    LOCATION='/CoverageEligibilityResponse/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.CoverageEligibilityResponseIdentifier AS
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
        BULK 'CoverageEligibilityResponse/**',
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

CREATE VIEW fhir.CoverageEligibilityResponsePurpose AS
SELECT
    [id],
    [purpose.JSON],
    [purpose]
FROM openrowset (
        BULK 'CoverageEligibilityResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [purpose.JSON]  VARCHAR(MAX) '$.purpose'
    ) AS rowset
    CROSS APPLY openjson (rowset.[purpose.JSON]) with (
        [purpose]                      NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.CoverageEligibilityResponseInsurance AS
SELECT
    [id],
    [insurance.JSON],
    [insurance.id],
    [insurance.extension],
    [insurance.modifierExtension],
    [insurance.coverage.id],
    [insurance.coverage.extension],
    [insurance.coverage.reference],
    [insurance.coverage.type],
    [insurance.coverage.identifier],
    [insurance.coverage.display],
    [insurance.inforce],
    [insurance.benefitPeriod.id],
    [insurance.benefitPeriod.extension],
    [insurance.benefitPeriod.start],
    [insurance.benefitPeriod.end],
    [insurance.item]
FROM openrowset (
        BULK 'CoverageEligibilityResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [insurance.JSON]  VARCHAR(MAX) '$.insurance'
    ) AS rowset
    CROSS APPLY openjson (rowset.[insurance.JSON]) with (
        [insurance.id]                 NVARCHAR(100)       '$.id',
        [insurance.extension]          NVARCHAR(MAX)       '$.extension',
        [insurance.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [insurance.coverage.id]        NVARCHAR(100)       '$.coverage.id',
        [insurance.coverage.extension] NVARCHAR(MAX)       '$.coverage.extension',
        [insurance.coverage.reference] NVARCHAR(4000)      '$.coverage.reference',
        [insurance.coverage.type]      VARCHAR(256)        '$.coverage.type',
        [insurance.coverage.identifier] NVARCHAR(MAX)       '$.coverage.identifier',
        [insurance.coverage.display]   NVARCHAR(4000)      '$.coverage.display',
        [insurance.inforce]            bit                 '$.inforce',
        [insurance.benefitPeriod.id]   NVARCHAR(100)       '$.benefitPeriod.id',
        [insurance.benefitPeriod.extension] NVARCHAR(MAX)       '$.benefitPeriod.extension',
        [insurance.benefitPeriod.start] VARCHAR(64)         '$.benefitPeriod.start',
        [insurance.benefitPeriod.end]  VARCHAR(64)         '$.benefitPeriod.end',
        [insurance.item]               NVARCHAR(MAX)       '$.item' AS JSON
    ) j

GO

CREATE VIEW fhir.CoverageEligibilityResponseError AS
SELECT
    [id],
    [error.JSON],
    [error.id],
    [error.extension],
    [error.modifierExtension],
    [error.code.id],
    [error.code.extension],
    [error.code.coding],
    [error.code.text]
FROM openrowset (
        BULK 'CoverageEligibilityResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [error.JSON]  VARCHAR(MAX) '$.error'
    ) AS rowset
    CROSS APPLY openjson (rowset.[error.JSON]) with (
        [error.id]                     NVARCHAR(100)       '$.id',
        [error.extension]              NVARCHAR(MAX)       '$.extension',
        [error.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [error.code.id]                NVARCHAR(100)       '$.code.id',
        [error.code.extension]         NVARCHAR(MAX)       '$.code.extension',
        [error.code.coding]            NVARCHAR(MAX)       '$.code.coding',
        [error.code.text]              NVARCHAR(4000)      '$.code.text'
    ) j
