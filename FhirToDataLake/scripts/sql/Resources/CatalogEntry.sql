CREATE EXTERNAL TABLE [fhir].[CatalogEntry] (
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
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [orderable] bit,
    [referencedItem.id] NVARCHAR(100),
    [referencedItem.extension] NVARCHAR(MAX),
    [referencedItem.reference] NVARCHAR(4000),
    [referencedItem.type] VARCHAR(256),
    [referencedItem.identifier.id] NVARCHAR(100),
    [referencedItem.identifier.extension] NVARCHAR(MAX),
    [referencedItem.identifier.use] NVARCHAR(64),
    [referencedItem.identifier.type] NVARCHAR(MAX),
    [referencedItem.identifier.system] VARCHAR(256),
    [referencedItem.identifier.value] NVARCHAR(4000),
    [referencedItem.identifier.period] NVARCHAR(MAX),
    [referencedItem.identifier.assigner] NVARCHAR(MAX),
    [referencedItem.display] NVARCHAR(4000),
    [additionalIdentifier] VARCHAR(MAX),
    [classification] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [validityPeriod.id] NVARCHAR(100),
    [validityPeriod.extension] NVARCHAR(MAX),
    [validityPeriod.start] VARCHAR(64),
    [validityPeriod.end] VARCHAR(64),
    [validTo] VARCHAR(64),
    [lastUpdated] VARCHAR(64),
    [additionalCharacteristic] VARCHAR(MAX),
    [additionalClassification] VARCHAR(MAX),
    [relatedEntry] VARCHAR(MAX),
) WITH (
    LOCATION='/CatalogEntry/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.CatalogEntryIdentifier AS
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
        BULK 'CatalogEntry/**',
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

CREATE VIEW fhir.CatalogEntryAdditionalIdentifier AS
SELECT
    [id],
    [additionalIdentifier.JSON],
    [additionalIdentifier.id],
    [additionalIdentifier.extension],
    [additionalIdentifier.use],
    [additionalIdentifier.type.id],
    [additionalIdentifier.type.extension],
    [additionalIdentifier.type.coding],
    [additionalIdentifier.type.text],
    [additionalIdentifier.system],
    [additionalIdentifier.value],
    [additionalIdentifier.period.id],
    [additionalIdentifier.period.extension],
    [additionalIdentifier.period.start],
    [additionalIdentifier.period.end],
    [additionalIdentifier.assigner.id],
    [additionalIdentifier.assigner.extension],
    [additionalIdentifier.assigner.reference],
    [additionalIdentifier.assigner.type],
    [additionalIdentifier.assigner.identifier],
    [additionalIdentifier.assigner.display]
FROM openrowset (
        BULK 'CatalogEntry/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [additionalIdentifier.JSON]  VARCHAR(MAX) '$.additionalIdentifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[additionalIdentifier.JSON]) with (
        [additionalIdentifier.id]      NVARCHAR(100)       '$.id',
        [additionalIdentifier.extension] NVARCHAR(MAX)       '$.extension',
        [additionalIdentifier.use]     NVARCHAR(64)        '$.use',
        [additionalIdentifier.type.id] NVARCHAR(100)       '$.type.id',
        [additionalIdentifier.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [additionalIdentifier.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [additionalIdentifier.type.text] NVARCHAR(4000)      '$.type.text',
        [additionalIdentifier.system]  VARCHAR(256)        '$.system',
        [additionalIdentifier.value]   NVARCHAR(4000)      '$.value',
        [additionalIdentifier.period.id] NVARCHAR(100)       '$.period.id',
        [additionalIdentifier.period.extension] NVARCHAR(MAX)       '$.period.extension',
        [additionalIdentifier.period.start] VARCHAR(64)         '$.period.start',
        [additionalIdentifier.period.end] VARCHAR(64)         '$.period.end',
        [additionalIdentifier.assigner.id] NVARCHAR(100)       '$.assigner.id',
        [additionalIdentifier.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [additionalIdentifier.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [additionalIdentifier.assigner.type] VARCHAR(256)        '$.assigner.type',
        [additionalIdentifier.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [additionalIdentifier.assigner.display] NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.CatalogEntryClassification AS
SELECT
    [id],
    [classification.JSON],
    [classification.id],
    [classification.extension],
    [classification.coding],
    [classification.text]
FROM openrowset (
        BULK 'CatalogEntry/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [classification.JSON]  VARCHAR(MAX) '$.classification'
    ) AS rowset
    CROSS APPLY openjson (rowset.[classification.JSON]) with (
        [classification.id]            NVARCHAR(100)       '$.id',
        [classification.extension]     NVARCHAR(MAX)       '$.extension',
        [classification.coding]        NVARCHAR(MAX)       '$.coding' AS JSON,
        [classification.text]          NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.CatalogEntryAdditionalCharacteristic AS
SELECT
    [id],
    [additionalCharacteristic.JSON],
    [additionalCharacteristic.id],
    [additionalCharacteristic.extension],
    [additionalCharacteristic.coding],
    [additionalCharacteristic.text]
FROM openrowset (
        BULK 'CatalogEntry/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [additionalCharacteristic.JSON]  VARCHAR(MAX) '$.additionalCharacteristic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[additionalCharacteristic.JSON]) with (
        [additionalCharacteristic.id]  NVARCHAR(100)       '$.id',
        [additionalCharacteristic.extension] NVARCHAR(MAX)       '$.extension',
        [additionalCharacteristic.coding] NVARCHAR(MAX)       '$.coding' AS JSON,
        [additionalCharacteristic.text] NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.CatalogEntryAdditionalClassification AS
SELECT
    [id],
    [additionalClassification.JSON],
    [additionalClassification.id],
    [additionalClassification.extension],
    [additionalClassification.coding],
    [additionalClassification.text]
FROM openrowset (
        BULK 'CatalogEntry/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [additionalClassification.JSON]  VARCHAR(MAX) '$.additionalClassification'
    ) AS rowset
    CROSS APPLY openjson (rowset.[additionalClassification.JSON]) with (
        [additionalClassification.id]  NVARCHAR(100)       '$.id',
        [additionalClassification.extension] NVARCHAR(MAX)       '$.extension',
        [additionalClassification.coding] NVARCHAR(MAX)       '$.coding' AS JSON,
        [additionalClassification.text] NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.CatalogEntryRelatedEntry AS
SELECT
    [id],
    [relatedEntry.JSON],
    [relatedEntry.id],
    [relatedEntry.extension],
    [relatedEntry.modifierExtension],
    [relatedEntry.relationtype],
    [relatedEntry.item.id],
    [relatedEntry.item.extension],
    [relatedEntry.item.reference],
    [relatedEntry.item.type],
    [relatedEntry.item.identifier],
    [relatedEntry.item.display]
FROM openrowset (
        BULK 'CatalogEntry/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [relatedEntry.JSON]  VARCHAR(MAX) '$.relatedEntry'
    ) AS rowset
    CROSS APPLY openjson (rowset.[relatedEntry.JSON]) with (
        [relatedEntry.id]              NVARCHAR(100)       '$.id',
        [relatedEntry.extension]       NVARCHAR(MAX)       '$.extension',
        [relatedEntry.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [relatedEntry.relationtype]    NVARCHAR(64)        '$.relationtype',
        [relatedEntry.item.id]         NVARCHAR(100)       '$.item.id',
        [relatedEntry.item.extension]  NVARCHAR(MAX)       '$.item.extension',
        [relatedEntry.item.reference]  NVARCHAR(4000)      '$.item.reference',
        [relatedEntry.item.type]       VARCHAR(256)        '$.item.type',
        [relatedEntry.item.identifier] NVARCHAR(MAX)       '$.item.identifier',
        [relatedEntry.item.display]    NVARCHAR(4000)      '$.item.display'
    ) j
