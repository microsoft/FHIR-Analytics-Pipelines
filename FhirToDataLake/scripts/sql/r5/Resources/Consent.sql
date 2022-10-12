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
    [status] NVARCHAR(100),
    [category] VARCHAR(MAX),
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
    [dateTime] VARCHAR(64),
    [grantor] VARCHAR(MAX),
    [grantee] VARCHAR(MAX),
    [manager] VARCHAR(MAX),
    [controller] VARCHAR(MAX),
    [sourceAttachment] VARCHAR(MAX),
    [sourceReference] VARCHAR(MAX),
    [policy] VARCHAR(MAX),
    [policyRule.id] NVARCHAR(100),
    [policyRule.extension] NVARCHAR(MAX),
    [policyRule.coding] VARCHAR(MAX),
    [policyRule.text] NVARCHAR(4000),
    [verification] VARCHAR(MAX),
    [provision.id] NVARCHAR(100),
    [provision.extension] NVARCHAR(MAX),
    [provision.modifierExtension] NVARCHAR(MAX),
    [provision.type] NVARCHAR(100),
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
    [provision.expression.id] NVARCHAR(100),
    [provision.expression.extension] NVARCHAR(MAX),
    [provision.expression.description] NVARCHAR(4000),
    [provision.expression.name] VARCHAR(64),
    [provision.expression.language] NVARCHAR(100),
    [provision.expression.expression] NVARCHAR(4000),
    [provision.expression.reference] VARCHAR(256),
    [provision.provision] VARCHAR(MAX),
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

CREATE VIEW fhir.ConsentGrantor AS
SELECT
    [id],
    [grantor.JSON],
    [grantor.id],
    [grantor.extension],
    [grantor.reference],
    [grantor.type],
    [grantor.identifier.id],
    [grantor.identifier.extension],
    [grantor.identifier.use],
    [grantor.identifier.type],
    [grantor.identifier.system],
    [grantor.identifier.value],
    [grantor.identifier.period],
    [grantor.identifier.assigner],
    [grantor.display]
FROM openrowset (
        BULK 'Consent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [grantor.JSON]  VARCHAR(MAX) '$.grantor'
    ) AS rowset
    CROSS APPLY openjson (rowset.[grantor.JSON]) with (
        [grantor.id]                   NVARCHAR(100)       '$.id',
        [grantor.extension]            NVARCHAR(MAX)       '$.extension',
        [grantor.reference]            NVARCHAR(4000)      '$.reference',
        [grantor.type]                 VARCHAR(256)        '$.type',
        [grantor.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [grantor.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [grantor.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [grantor.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [grantor.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [grantor.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [grantor.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [grantor.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [grantor.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ConsentGrantee AS
SELECT
    [id],
    [grantee.JSON],
    [grantee.id],
    [grantee.extension],
    [grantee.reference],
    [grantee.type],
    [grantee.identifier.id],
    [grantee.identifier.extension],
    [grantee.identifier.use],
    [grantee.identifier.type],
    [grantee.identifier.system],
    [grantee.identifier.value],
    [grantee.identifier.period],
    [grantee.identifier.assigner],
    [grantee.display]
FROM openrowset (
        BULK 'Consent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [grantee.JSON]  VARCHAR(MAX) '$.grantee'
    ) AS rowset
    CROSS APPLY openjson (rowset.[grantee.JSON]) with (
        [grantee.id]                   NVARCHAR(100)       '$.id',
        [grantee.extension]            NVARCHAR(MAX)       '$.extension',
        [grantee.reference]            NVARCHAR(4000)      '$.reference',
        [grantee.type]                 VARCHAR(256)        '$.type',
        [grantee.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [grantee.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [grantee.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [grantee.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [grantee.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [grantee.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [grantee.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [grantee.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [grantee.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ConsentManager AS
SELECT
    [id],
    [manager.JSON],
    [manager.id],
    [manager.extension],
    [manager.reference],
    [manager.type],
    [manager.identifier.id],
    [manager.identifier.extension],
    [manager.identifier.use],
    [manager.identifier.type],
    [manager.identifier.system],
    [manager.identifier.value],
    [manager.identifier.period],
    [manager.identifier.assigner],
    [manager.display]
FROM openrowset (
        BULK 'Consent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [manager.JSON]  VARCHAR(MAX) '$.manager'
    ) AS rowset
    CROSS APPLY openjson (rowset.[manager.JSON]) with (
        [manager.id]                   NVARCHAR(100)       '$.id',
        [manager.extension]            NVARCHAR(MAX)       '$.extension',
        [manager.reference]            NVARCHAR(4000)      '$.reference',
        [manager.type]                 VARCHAR(256)        '$.type',
        [manager.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [manager.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [manager.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [manager.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [manager.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [manager.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [manager.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [manager.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [manager.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ConsentController AS
SELECT
    [id],
    [controller.JSON],
    [controller.id],
    [controller.extension],
    [controller.reference],
    [controller.type],
    [controller.identifier.id],
    [controller.identifier.extension],
    [controller.identifier.use],
    [controller.identifier.type],
    [controller.identifier.system],
    [controller.identifier.value],
    [controller.identifier.period],
    [controller.identifier.assigner],
    [controller.display]
FROM openrowset (
        BULK 'Consent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [controller.JSON]  VARCHAR(MAX) '$.controller'
    ) AS rowset
    CROSS APPLY openjson (rowset.[controller.JSON]) with (
        [controller.id]                NVARCHAR(100)       '$.id',
        [controller.extension]         NVARCHAR(MAX)       '$.extension',
        [controller.reference]         NVARCHAR(4000)      '$.reference',
        [controller.type]              VARCHAR(256)        '$.type',
        [controller.identifier.id]     NVARCHAR(100)       '$.identifier.id',
        [controller.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [controller.identifier.use]    NVARCHAR(64)        '$.identifier.use',
        [controller.identifier.type]   NVARCHAR(MAX)       '$.identifier.type',
        [controller.identifier.system] VARCHAR(256)        '$.identifier.system',
        [controller.identifier.value]  NVARCHAR(4000)      '$.identifier.value',
        [controller.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [controller.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [controller.display]           NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ConsentSourceAttachment AS
SELECT
    [id],
    [sourceAttachment.JSON],
    [sourceAttachment.id],
    [sourceAttachment.extension],
    [sourceAttachment.contentType],
    [sourceAttachment.language],
    [sourceAttachment.data],
    [sourceAttachment.url],
    [sourceAttachment.size],
    [sourceAttachment.hash],
    [sourceAttachment.title],
    [sourceAttachment.creation],
    [sourceAttachment.height],
    [sourceAttachment.width],
    [sourceAttachment.frames],
    [sourceAttachment.duration],
    [sourceAttachment.pages]
FROM openrowset (
        BULK 'Consent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [sourceAttachment.JSON]  VARCHAR(MAX) '$.sourceAttachment'
    ) AS rowset
    CROSS APPLY openjson (rowset.[sourceAttachment.JSON]) with (
        [sourceAttachment.id]          NVARCHAR(100)       '$.id',
        [sourceAttachment.extension]   NVARCHAR(MAX)       '$.extension',
        [sourceAttachment.contentType] NVARCHAR(100)       '$.contentType',
        [sourceAttachment.language]    NVARCHAR(100)       '$.language',
        [sourceAttachment.data]        NVARCHAR(MAX)       '$.data',
        [sourceAttachment.url]         VARCHAR(256)        '$.url',
        [sourceAttachment.size]        NVARCHAR(MAX)       '$.size',
        [sourceAttachment.hash]        NVARCHAR(MAX)       '$.hash',
        [sourceAttachment.title]       NVARCHAR(4000)      '$.title',
        [sourceAttachment.creation]    VARCHAR(64)         '$.creation',
        [sourceAttachment.height]      bigint              '$.height',
        [sourceAttachment.width]       bigint              '$.width',
        [sourceAttachment.frames]      bigint              '$.frames',
        [sourceAttachment.duration]    float               '$.duration',
        [sourceAttachment.pages]       bigint              '$.pages'
    ) j

GO

CREATE VIEW fhir.ConsentSourceReference AS
SELECT
    [id],
    [sourceReference.JSON],
    [sourceReference.id],
    [sourceReference.extension],
    [sourceReference.reference],
    [sourceReference.type],
    [sourceReference.identifier.id],
    [sourceReference.identifier.extension],
    [sourceReference.identifier.use],
    [sourceReference.identifier.type],
    [sourceReference.identifier.system],
    [sourceReference.identifier.value],
    [sourceReference.identifier.period],
    [sourceReference.identifier.assigner],
    [sourceReference.display]
FROM openrowset (
        BULK 'Consent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [sourceReference.JSON]  VARCHAR(MAX) '$.sourceReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[sourceReference.JSON]) with (
        [sourceReference.id]           NVARCHAR(100)       '$.id',
        [sourceReference.extension]    NVARCHAR(MAX)       '$.extension',
        [sourceReference.reference]    NVARCHAR(4000)      '$.reference',
        [sourceReference.type]         VARCHAR(256)        '$.type',
        [sourceReference.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [sourceReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [sourceReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [sourceReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [sourceReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [sourceReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [sourceReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [sourceReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [sourceReference.display]      NVARCHAR(4000)      '$.display'
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
    [verification.verificationType.id],
    [verification.verificationType.extension],
    [verification.verificationType.coding],
    [verification.verificationType.text],
    [verification.verifiedBy.id],
    [verification.verifiedBy.extension],
    [verification.verifiedBy.reference],
    [verification.verifiedBy.type],
    [verification.verifiedBy.identifier],
    [verification.verifiedBy.display],
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
        [verification.verificationType.id] NVARCHAR(100)       '$.verificationType.id',
        [verification.verificationType.extension] NVARCHAR(MAX)       '$.verificationType.extension',
        [verification.verificationType.coding] NVARCHAR(MAX)       '$.verificationType.coding',
        [verification.verificationType.text] NVARCHAR(4000)      '$.verificationType.text',
        [verification.verifiedBy.id]   NVARCHAR(100)       '$.verifiedBy.id',
        [verification.verifiedBy.extension] NVARCHAR(MAX)       '$.verifiedBy.extension',
        [verification.verifiedBy.reference] NVARCHAR(4000)      '$.verifiedBy.reference',
        [verification.verifiedBy.type] VARCHAR(256)        '$.verifiedBy.type',
        [verification.verifiedBy.identifier] NVARCHAR(MAX)       '$.verifiedBy.identifier',
        [verification.verifiedBy.display] NVARCHAR(4000)      '$.verifiedBy.display',
        [verification.verifiedWith.id] NVARCHAR(100)       '$.verifiedWith.id',
        [verification.verifiedWith.extension] NVARCHAR(MAX)       '$.verifiedWith.extension',
        [verification.verifiedWith.reference] NVARCHAR(4000)      '$.verifiedWith.reference',
        [verification.verifiedWith.type] VARCHAR(256)        '$.verifiedWith.type',
        [verification.verifiedWith.identifier] NVARCHAR(MAX)       '$.verifiedWith.identifier',
        [verification.verifiedWith.display] NVARCHAR(4000)      '$.verifiedWith.display',
        [verification.verificationDate] NVARCHAR(MAX)       '$.verificationDate' AS JSON
    ) j
