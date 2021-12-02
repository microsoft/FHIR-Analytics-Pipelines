CREATE EXTERNAL TABLE [fhir].[Coverage] (
    [resourceType] NVARCHAR(4000),
    [id] VARCHAR(64),
    [meta.id] NVARCHAR(4000),
    [meta.extension] NVARCHAR(MAX),
    [meta.versionId] VARCHAR(64),
    [meta.lastUpdated] VARCHAR(30),
    [meta.source] VARCHAR(256),
    [meta.profile] VARCHAR(MAX),
    [meta.security] VARCHAR(MAX),
    [meta.tag] VARCHAR(MAX),
    [implicitRules] VARCHAR(256),
    [language] NVARCHAR(4000),
    [text.id] NVARCHAR(4000),
    [text.extension] NVARCHAR(MAX),
    [text.status] NVARCHAR(64),
    [text.div] NVARCHAR(MAX),
    [extension] NVARCHAR(MAX),
    [modifierExtension] NVARCHAR(MAX),
    [identifier] VARCHAR(MAX),
    [status] NVARCHAR(4000),
    [type.id] NVARCHAR(4000),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [policyHolder.id] NVARCHAR(4000),
    [policyHolder.extension] NVARCHAR(MAX),
    [policyHolder.reference] NVARCHAR(4000),
    [policyHolder.type] VARCHAR(256),
    [policyHolder.identifier.id] NVARCHAR(4000),
    [policyHolder.identifier.extension] NVARCHAR(MAX),
    [policyHolder.identifier.use] NVARCHAR(64),
    [policyHolder.identifier.type] NVARCHAR(MAX),
    [policyHolder.identifier.system] VARCHAR(256),
    [policyHolder.identifier.value] NVARCHAR(4000),
    [policyHolder.identifier.period] NVARCHAR(MAX),
    [policyHolder.identifier.assigner] NVARCHAR(MAX),
    [policyHolder.display] NVARCHAR(4000),
    [subscriber.id] NVARCHAR(4000),
    [subscriber.extension] NVARCHAR(MAX),
    [subscriber.reference] NVARCHAR(4000),
    [subscriber.type] VARCHAR(256),
    [subscriber.identifier.id] NVARCHAR(4000),
    [subscriber.identifier.extension] NVARCHAR(MAX),
    [subscriber.identifier.use] NVARCHAR(64),
    [subscriber.identifier.type] NVARCHAR(MAX),
    [subscriber.identifier.system] VARCHAR(256),
    [subscriber.identifier.value] NVARCHAR(4000),
    [subscriber.identifier.period] NVARCHAR(MAX),
    [subscriber.identifier.assigner] NVARCHAR(MAX),
    [subscriber.display] NVARCHAR(4000),
    [subscriberId] NVARCHAR(4000),
    [beneficiary.id] NVARCHAR(4000),
    [beneficiary.extension] NVARCHAR(MAX),
    [beneficiary.reference] NVARCHAR(4000),
    [beneficiary.type] VARCHAR(256),
    [beneficiary.identifier.id] NVARCHAR(4000),
    [beneficiary.identifier.extension] NVARCHAR(MAX),
    [beneficiary.identifier.use] NVARCHAR(64),
    [beneficiary.identifier.type] NVARCHAR(MAX),
    [beneficiary.identifier.system] VARCHAR(256),
    [beneficiary.identifier.value] NVARCHAR(4000),
    [beneficiary.identifier.period] NVARCHAR(MAX),
    [beneficiary.identifier.assigner] NVARCHAR(MAX),
    [beneficiary.display] NVARCHAR(4000),
    [dependent] NVARCHAR(4000),
    [relationship.id] NVARCHAR(4000),
    [relationship.extension] NVARCHAR(MAX),
    [relationship.coding] VARCHAR(MAX),
    [relationship.text] NVARCHAR(4000),
    [period.id] NVARCHAR(4000),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(30),
    [period.end] VARCHAR(30),
    [payor] VARCHAR(MAX),
    [class] VARCHAR(MAX),
    [order] bigint,
    [network] NVARCHAR(4000),
    [costToBeneficiary] VARCHAR(MAX),
    [subrogation] bit,
    [contract] VARCHAR(MAX),
) WITH (
    LOCATION='/Coverage/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.CoverageIdentifier AS
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
        BULK 'Coverage/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [identifier.JSON]  VARCHAR(MAX) '$.identifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[identifier.JSON]) with (
        [identifier.id]                NVARCHAR(4000)      '$.id',
        [identifier.extension]         NVARCHAR(MAX)       '$.extension',
        [identifier.use]               NVARCHAR(64)        '$.use',
        [identifier.type.id]           NVARCHAR(4000)      '$.type.id',
        [identifier.type.extension]    NVARCHAR(MAX)       '$.type.extension',
        [identifier.type.coding]       NVARCHAR(MAX)       '$.type.coding',
        [identifier.type.text]         NVARCHAR(4000)      '$.type.text',
        [identifier.system]            VARCHAR(256)        '$.system',
        [identifier.value]             NVARCHAR(4000)      '$.value',
        [identifier.period.id]         NVARCHAR(4000)      '$.period.id',
        [identifier.period.extension]  NVARCHAR(MAX)       '$.period.extension',
        [identifier.period.start]      VARCHAR(30)         '$.period.start',
        [identifier.period.end]        VARCHAR(30)         '$.period.end',
        [identifier.assigner.id]       NVARCHAR(4000)      '$.assigner.id',
        [identifier.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [identifier.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [identifier.assigner.type]     VARCHAR(256)        '$.assigner.type',
        [identifier.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [identifier.assigner.display]  NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.CoveragePayor AS
SELECT
    [id],
    [payor.JSON],
    [payor.id],
    [payor.extension],
    [payor.reference],
    [payor.type],
    [payor.identifier.id],
    [payor.identifier.extension],
    [payor.identifier.use],
    [payor.identifier.type],
    [payor.identifier.system],
    [payor.identifier.value],
    [payor.identifier.period],
    [payor.identifier.assigner],
    [payor.display]
FROM openrowset (
        BULK 'Coverage/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [payor.JSON]  VARCHAR(MAX) '$.payor'
    ) AS rowset
    CROSS APPLY openjson (rowset.[payor.JSON]) with (
        [payor.id]                     NVARCHAR(4000)      '$.id',
        [payor.extension]              NVARCHAR(MAX)       '$.extension',
        [payor.reference]              NVARCHAR(4000)      '$.reference',
        [payor.type]                   VARCHAR(256)        '$.type',
        [payor.identifier.id]          NVARCHAR(4000)      '$.identifier.id',
        [payor.identifier.extension]   NVARCHAR(MAX)       '$.identifier.extension',
        [payor.identifier.use]         NVARCHAR(64)        '$.identifier.use',
        [payor.identifier.type]        NVARCHAR(MAX)       '$.identifier.type',
        [payor.identifier.system]      VARCHAR(256)        '$.identifier.system',
        [payor.identifier.value]       NVARCHAR(4000)      '$.identifier.value',
        [payor.identifier.period]      NVARCHAR(MAX)       '$.identifier.period',
        [payor.identifier.assigner]    NVARCHAR(MAX)       '$.identifier.assigner',
        [payor.display]                NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CoverageClass AS
SELECT
    [id],
    [class.JSON],
    [class.id],
    [class.extension],
    [class.modifierExtension],
    [class.type.id],
    [class.type.extension],
    [class.type.coding],
    [class.type.text],
    [class.value],
    [class.name]
FROM openrowset (
        BULK 'Coverage/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [class.JSON]  VARCHAR(MAX) '$.class'
    ) AS rowset
    CROSS APPLY openjson (rowset.[class.JSON]) with (
        [class.id]                     NVARCHAR(4000)      '$.id',
        [class.extension]              NVARCHAR(MAX)       '$.extension',
        [class.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [class.type.id]                NVARCHAR(4000)      '$.type.id',
        [class.type.extension]         NVARCHAR(MAX)       '$.type.extension',
        [class.type.coding]            NVARCHAR(MAX)       '$.type.coding',
        [class.type.text]              NVARCHAR(4000)      '$.type.text',
        [class.value]                  NVARCHAR(4000)      '$.value',
        [class.name]                   NVARCHAR(4000)      '$.name'
    ) j

GO

CREATE VIEW fhir.CoverageCostToBeneficiary AS
SELECT
    [id],
    [costToBeneficiary.JSON],
    [costToBeneficiary.id],
    [costToBeneficiary.extension],
    [costToBeneficiary.modifierExtension],
    [costToBeneficiary.type.id],
    [costToBeneficiary.type.extension],
    [costToBeneficiary.type.coding],
    [costToBeneficiary.type.text],
    [costToBeneficiary.valueQuantity.id],
    [costToBeneficiary.valueQuantity.extension],
    [costToBeneficiary.valueQuantity.value],
    [costToBeneficiary.valueQuantity.comparator],
    [costToBeneficiary.valueQuantity.unit],
    [costToBeneficiary.valueQuantity.system],
    [costToBeneficiary.valueQuantity.code],
    [costToBeneficiary.exception],
    [costToBeneficiary.value.quantity.id],
    [costToBeneficiary.value.quantity.extension],
    [costToBeneficiary.value.quantity.value],
    [costToBeneficiary.value.quantity.comparator],
    [costToBeneficiary.value.quantity.unit],
    [costToBeneficiary.value.quantity.system],
    [costToBeneficiary.value.quantity.code],
    [costToBeneficiary.value.money.id],
    [costToBeneficiary.value.money.extension],
    [costToBeneficiary.value.money.value],
    [costToBeneficiary.value.money.currency]
FROM openrowset (
        BULK 'Coverage/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [costToBeneficiary.JSON]  VARCHAR(MAX) '$.costToBeneficiary'
    ) AS rowset
    CROSS APPLY openjson (rowset.[costToBeneficiary.JSON]) with (
        [costToBeneficiary.id]         NVARCHAR(4000)      '$.id',
        [costToBeneficiary.extension]  NVARCHAR(MAX)       '$.extension',
        [costToBeneficiary.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [costToBeneficiary.type.id]    NVARCHAR(4000)      '$.type.id',
        [costToBeneficiary.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [costToBeneficiary.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [costToBeneficiary.type.text]  NVARCHAR(4000)      '$.type.text',
        [costToBeneficiary.valueQuantity.id] NVARCHAR(4000)      '$.valueQuantity.id',
        [costToBeneficiary.valueQuantity.extension] NVARCHAR(MAX)       '$.valueQuantity.extension',
        [costToBeneficiary.valueQuantity.value] float               '$.valueQuantity.value',
        [costToBeneficiary.valueQuantity.comparator] NVARCHAR(64)        '$.valueQuantity.comparator',
        [costToBeneficiary.valueQuantity.unit] NVARCHAR(4000)      '$.valueQuantity.unit',
        [costToBeneficiary.valueQuantity.system] VARCHAR(256)        '$.valueQuantity.system',
        [costToBeneficiary.valueQuantity.code] NVARCHAR(4000)      '$.valueQuantity.code',
        [costToBeneficiary.exception]  NVARCHAR(MAX)       '$.exception' AS JSON,
        [costToBeneficiary.value.quantity.id] NVARCHAR(4000)      '$.value.quantity.id',
        [costToBeneficiary.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [costToBeneficiary.value.quantity.value] float               '$.value.quantity.value',
        [costToBeneficiary.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [costToBeneficiary.value.quantity.unit] NVARCHAR(4000)      '$.value.quantity.unit',
        [costToBeneficiary.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [costToBeneficiary.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [costToBeneficiary.value.money.id] NVARCHAR(4000)      '$.value.money.id',
        [costToBeneficiary.value.money.extension] NVARCHAR(MAX)       '$.value.money.extension',
        [costToBeneficiary.value.money.value] float               '$.value.money.value',
        [costToBeneficiary.value.money.currency] NVARCHAR(4000)      '$.value.money.currency'
    ) j

GO

CREATE VIEW fhir.CoverageContract AS
SELECT
    [id],
    [contract.JSON],
    [contract.id],
    [contract.extension],
    [contract.reference],
    [contract.type],
    [contract.identifier.id],
    [contract.identifier.extension],
    [contract.identifier.use],
    [contract.identifier.type],
    [contract.identifier.system],
    [contract.identifier.value],
    [contract.identifier.period],
    [contract.identifier.assigner],
    [contract.display]
FROM openrowset (
        BULK 'Coverage/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contract.JSON]  VARCHAR(MAX) '$.contract'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contract.JSON]) with (
        [contract.id]                  NVARCHAR(4000)      '$.id',
        [contract.extension]           NVARCHAR(MAX)       '$.extension',
        [contract.reference]           NVARCHAR(4000)      '$.reference',
        [contract.type]                VARCHAR(256)        '$.type',
        [contract.identifier.id]       NVARCHAR(4000)      '$.identifier.id',
        [contract.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [contract.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [contract.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [contract.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [contract.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [contract.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [contract.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [contract.display]             NVARCHAR(4000)      '$.display'
    ) j
