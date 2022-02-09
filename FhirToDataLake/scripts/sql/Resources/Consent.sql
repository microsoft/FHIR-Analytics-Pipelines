CREATE EXTERNAL TABLE [fhir].[Consent] (
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
    [scope.id] NVARCHAR(100),
    [scope.extension] NVARCHAR(MAX),
    [scope.coding] VARCHAR(MAX),
    [scope.text] NVARCHAR(4000),
    [category] VARCHAR(MAX),
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
    [dateTime] VARCHAR(64),
    [performer] VARCHAR(MAX),
    [organization] VARCHAR(MAX),
    [policy] VARCHAR(MAX),
    [policyRule.id] NVARCHAR(100),
    [policyRule.extension] NVARCHAR(MAX),
    [policyRule.coding] VARCHAR(MAX),
    [policyRule.text] NVARCHAR(4000),
    [verification] VARCHAR(MAX),
    [provision.id] NVARCHAR(100),
    [provision.extension] NVARCHAR(MAX),
    [provision.modifierExtension] NVARCHAR(MAX),
    [provision.type] NVARCHAR(64),
    [provision.period.id] NVARCHAR(100),
    [provision.period.extension] NVARCHAR(MAX),
    [provision.period.start] VARCHAR(64),
    [provision.period.end] VARCHAR(64),
    [provision.actor] VARCHAR(MAX),
    [provision.action] VARCHAR(MAX),
    [provision.securityLabel] VARCHAR(MAX),
    [provision.purpose] VARCHAR(MAX),
    [provision.class] VARCHAR(MAX),
    [provision.code] VARCHAR(MAX),
    [provision.dataPeriod.id] NVARCHAR(100),
    [provision.dataPeriod.extension] NVARCHAR(MAX),
    [provision.dataPeriod.start] VARCHAR(64),
    [provision.dataPeriod.end] VARCHAR(64),
    [provision.data] VARCHAR(MAX),
    [provision.provision] VARCHAR(MAX),
    [source.attachment.id] NVARCHAR(100),
    [source.attachment.extension] NVARCHAR(MAX),
    [source.attachment.contentType] NVARCHAR(100),
    [source.attachment.language] NVARCHAR(100),
    [source.attachment.data] NVARCHAR(MAX),
    [source.attachment.url] VARCHAR(256),
    [source.attachment.size] bigint,
    [source.attachment.hash] NVARCHAR(MAX),
    [source.attachment.title] NVARCHAR(4000),
    [source.attachment.creation] VARCHAR(64),
    [source.reference.id] NVARCHAR(100),
    [source.reference.extension] NVARCHAR(MAX),
    [source.reference.reference] NVARCHAR(4000),
    [source.reference.type] VARCHAR(256),
    [source.reference.identifier.id] NVARCHAR(100),
    [source.reference.identifier.extension] NVARCHAR(MAX),
    [source.reference.identifier.use] NVARCHAR(64),
    [source.reference.identifier.type] NVARCHAR(MAX),
    [source.reference.identifier.system] VARCHAR(256),
    [source.reference.identifier.value] NVARCHAR(4000),
    [source.reference.identifier.period] NVARCHAR(MAX),
    [source.reference.identifier.assigner] NVARCHAR(MAX),
    [source.reference.display] NVARCHAR(4000),
) WITH (
    LOCATION='/Consent/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ConsentIdentifier AS
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
        BULK 'Consent/**',
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

CREATE VIEW fhir.ConsentCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'Consent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [category.JSON]  VARCHAR(MAX) '$.category'
    ) AS rowset
    CROSS APPLY openjson (rowset.[category.JSON]) with (
        [category.id]                  NVARCHAR(100)       '$.id',
        [category.extension]           NVARCHAR(MAX)       '$.extension',
        [category.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [category.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ConsentPerformer AS
SELECT
    [id],
    [performer.JSON],
    [performer.id],
    [performer.extension],
    [performer.reference],
    [performer.type],
    [performer.identifier.id],
    [performer.identifier.extension],
    [performer.identifier.use],
    [performer.identifier.type],
    [performer.identifier.system],
    [performer.identifier.value],
    [performer.identifier.period],
    [performer.identifier.assigner],
    [performer.display]
FROM openrowset (
        BULK 'Consent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [performer.JSON]  VARCHAR(MAX) '$.performer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[performer.JSON]) with (
        [performer.id]                 NVARCHAR(100)       '$.id',
        [performer.extension]          NVARCHAR(MAX)       '$.extension',
        [performer.reference]          NVARCHAR(4000)      '$.reference',
        [performer.type]               VARCHAR(256)        '$.type',
        [performer.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [performer.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [performer.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [performer.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [performer.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [performer.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [performer.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [performer.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [performer.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ConsentOrganization AS
SELECT
    [id],
    [organization.JSON],
    [organization.id],
    [organization.extension],
    [organization.reference],
    [organization.type],
    [organization.identifier.id],
    [organization.identifier.extension],
    [organization.identifier.use],
    [organization.identifier.type],
    [organization.identifier.system],
    [organization.identifier.value],
    [organization.identifier.period],
    [organization.identifier.assigner],
    [organization.display]
FROM openrowset (
        BULK 'Consent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [organization.JSON]  VARCHAR(MAX) '$.organization'
    ) AS rowset
    CROSS APPLY openjson (rowset.[organization.JSON]) with (
        [organization.id]              NVARCHAR(100)       '$.id',
        [organization.extension]       NVARCHAR(MAX)       '$.extension',
        [organization.reference]       NVARCHAR(4000)      '$.reference',
        [organization.type]            VARCHAR(256)        '$.type',
        [organization.identifier.id]   NVARCHAR(100)       '$.identifier.id',
        [organization.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [organization.identifier.use]  NVARCHAR(64)        '$.identifier.use',
        [organization.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [organization.identifier.system] VARCHAR(256)        '$.identifier.system',
        [organization.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [organization.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [organization.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [organization.display]         NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ConsentPolicy AS
SELECT
    [id],
    [policy.JSON],
    [policy.id],
    [policy.extension],
    [policy.modifierExtension],
    [policy.authority],
    [policy.uri]
FROM openrowset (
        BULK 'Consent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [policy.JSON]  VARCHAR(MAX) '$.policy'
    ) AS rowset
    CROSS APPLY openjson (rowset.[policy.JSON]) with (
        [policy.id]                    NVARCHAR(100)       '$.id',
        [policy.extension]             NVARCHAR(MAX)       '$.extension',
        [policy.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [policy.authority]             VARCHAR(256)        '$.authority',
        [policy.uri]                   VARCHAR(256)        '$.uri'
    ) j

GO

CREATE VIEW fhir.ConsentVerification AS
SELECT
    [id],
    [verification.JSON],
    [verification.id],
    [verification.extension],
    [verification.modifierExtension],
    [verification.verified],
    [verification.verifiedWith.id],
    [verification.verifiedWith.extension],
    [verification.verifiedWith.reference],
    [verification.verifiedWith.type],
    [verification.verifiedWith.identifier],
    [verification.verifiedWith.display],
    [verification.verificationDate]
FROM openrowset (
        BULK 'Consent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [verification.JSON]  VARCHAR(MAX) '$.verification'
    ) AS rowset
    CROSS APPLY openjson (rowset.[verification.JSON]) with (
        [verification.id]              NVARCHAR(100)       '$.id',
        [verification.extension]       NVARCHAR(MAX)       '$.extension',
        [verification.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [verification.verified]        bit                 '$.verified',
        [verification.verifiedWith.id] NVARCHAR(100)       '$.verifiedWith.id',
        [verification.verifiedWith.extension] NVARCHAR(MAX)       '$.verifiedWith.extension',
        [verification.verifiedWith.reference] NVARCHAR(4000)      '$.verifiedWith.reference',
        [verification.verifiedWith.type] VARCHAR(256)        '$.verifiedWith.type',
        [verification.verifiedWith.identifier] NVARCHAR(MAX)       '$.verifiedWith.identifier',
        [verification.verifiedWith.display] NVARCHAR(4000)      '$.verifiedWith.display',
        [verification.verificationDate] VARCHAR(64)         '$.verificationDate'
    ) j
