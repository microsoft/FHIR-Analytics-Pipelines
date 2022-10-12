CREATE EXTERNAL TABLE [fhir].[PackagedProductDefinition] (
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
    [name] NVARCHAR(500),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [packageFor] VARCHAR(MAX),
    [status.id] NVARCHAR(100),
    [status.extension] NVARCHAR(MAX),
    [status.coding] VARCHAR(MAX),
    [status.text] NVARCHAR(4000),
    [statusDate] VARCHAR(64),
    [containedItemQuantity] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [legalStatusOfSupply] VARCHAR(MAX),
    [marketingStatus] VARCHAR(MAX),
    [characteristic] VARCHAR(MAX),
    [copackagedIndicator] bit,
    [manufacturer] VARCHAR(MAX),
    [attachedDocument] VARCHAR(MAX),
    [package.id] NVARCHAR(100),
    [package.extension] NVARCHAR(MAX),
    [package.modifierExtension] NVARCHAR(MAX),
    [package.identifier] VARCHAR(MAX),
    [package.type.id] NVARCHAR(100),
    [package.type.extension] NVARCHAR(MAX),
    [package.type.coding] NVARCHAR(MAX),
    [package.type.text] NVARCHAR(4000),
    [package.quantity] bigint,
    [package.material] VARCHAR(MAX),
    [package.alternateMaterial] VARCHAR(MAX),
    [package.shelfLifeStorage] VARCHAR(MAX),
    [package.manufacturer] VARCHAR(MAX),
    [package.property] VARCHAR(MAX),
    [package.containedItem] VARCHAR(MAX),
    [package.package] VARCHAR(MAX),
) WITH (
    LOCATION='/PackagedProductDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.PackagedProductDefinitionIdentifier AS
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
        BULK 'PackagedProductDefinition/**',
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

CREATE VIEW fhir.PackagedProductDefinitionPackageFor AS
SELECT
    [id],
    [packageFor.JSON],
    [packageFor.id],
    [packageFor.extension],
    [packageFor.reference],
    [packageFor.type],
    [packageFor.identifier.id],
    [packageFor.identifier.extension],
    [packageFor.identifier.use],
    [packageFor.identifier.type],
    [packageFor.identifier.system],
    [packageFor.identifier.value],
    [packageFor.identifier.period],
    [packageFor.identifier.assigner],
    [packageFor.display]
FROM openrowset (
        BULK 'PackagedProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [packageFor.JSON]  VARCHAR(MAX) '$.packageFor'
    ) AS rowset
    CROSS APPLY openjson (rowset.[packageFor.JSON]) with (
        [packageFor.id]                NVARCHAR(100)       '$.id',
        [packageFor.extension]         NVARCHAR(MAX)       '$.extension',
        [packageFor.reference]         NVARCHAR(4000)      '$.reference',
        [packageFor.type]              VARCHAR(256)        '$.type',
        [packageFor.identifier.id]     NVARCHAR(100)       '$.identifier.id',
        [packageFor.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [packageFor.identifier.use]    NVARCHAR(64)        '$.identifier.use',
        [packageFor.identifier.type]   NVARCHAR(MAX)       '$.identifier.type',
        [packageFor.identifier.system] VARCHAR(256)        '$.identifier.system',
        [packageFor.identifier.value]  NVARCHAR(4000)      '$.identifier.value',
        [packageFor.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [packageFor.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [packageFor.display]           NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.PackagedProductDefinitionContainedItemQuantity AS
SELECT
    [id],
    [containedItemQuantity.JSON],
    [containedItemQuantity.id],
    [containedItemQuantity.extension],
    [containedItemQuantity.value],
    [containedItemQuantity.comparator],
    [containedItemQuantity.unit],
    [containedItemQuantity.system],
    [containedItemQuantity.code]
FROM openrowset (
        BULK 'PackagedProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [containedItemQuantity.JSON]  VARCHAR(MAX) '$.containedItemQuantity'
    ) AS rowset
    CROSS APPLY openjson (rowset.[containedItemQuantity.JSON]) with (
        [containedItemQuantity.id]     NVARCHAR(100)       '$.id',
        [containedItemQuantity.extension] NVARCHAR(MAX)       '$.extension',
        [containedItemQuantity.value]  float               '$.value',
        [containedItemQuantity.comparator] NVARCHAR(64)        '$.comparator',
        [containedItemQuantity.unit]   NVARCHAR(100)       '$.unit',
        [containedItemQuantity.system] VARCHAR(256)        '$.system',
        [containedItemQuantity.code]   NVARCHAR(4000)      '$.code'
    ) j

GO

CREATE VIEW fhir.PackagedProductDefinitionLegalStatusOfSupply AS
SELECT
    [id],
    [legalStatusOfSupply.JSON],
    [legalStatusOfSupply.id],
    [legalStatusOfSupply.extension],
    [legalStatusOfSupply.modifierExtension],
    [legalStatusOfSupply.code.id],
    [legalStatusOfSupply.code.extension],
    [legalStatusOfSupply.code.coding],
    [legalStatusOfSupply.code.text],
    [legalStatusOfSupply.jurisdiction.id],
    [legalStatusOfSupply.jurisdiction.extension],
    [legalStatusOfSupply.jurisdiction.coding],
    [legalStatusOfSupply.jurisdiction.text]
FROM openrowset (
        BULK 'PackagedProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [legalStatusOfSupply.JSON]  VARCHAR(MAX) '$.legalStatusOfSupply'
    ) AS rowset
    CROSS APPLY openjson (rowset.[legalStatusOfSupply.JSON]) with (
        [legalStatusOfSupply.id]       NVARCHAR(100)       '$.id',
        [legalStatusOfSupply.extension] NVARCHAR(MAX)       '$.extension',
        [legalStatusOfSupply.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [legalStatusOfSupply.code.id]  NVARCHAR(100)       '$.code.id',
        [legalStatusOfSupply.code.extension] NVARCHAR(MAX)       '$.code.extension',
        [legalStatusOfSupply.code.coding] NVARCHAR(MAX)       '$.code.coding',
        [legalStatusOfSupply.code.text] NVARCHAR(4000)      '$.code.text',
        [legalStatusOfSupply.jurisdiction.id] NVARCHAR(100)       '$.jurisdiction.id',
        [legalStatusOfSupply.jurisdiction.extension] NVARCHAR(MAX)       '$.jurisdiction.extension',
        [legalStatusOfSupply.jurisdiction.coding] NVARCHAR(MAX)       '$.jurisdiction.coding',
        [legalStatusOfSupply.jurisdiction.text] NVARCHAR(4000)      '$.jurisdiction.text'
    ) j

GO

CREATE VIEW fhir.PackagedProductDefinitionMarketingStatus AS
SELECT
    [id],
    [marketingStatus.JSON],
    [marketingStatus.id],
    [marketingStatus.extension],
    [marketingStatus.modifierExtension],
    [marketingStatus.country.id],
    [marketingStatus.country.extension],
    [marketingStatus.country.coding],
    [marketingStatus.country.text],
    [marketingStatus.jurisdiction.id],
    [marketingStatus.jurisdiction.extension],
    [marketingStatus.jurisdiction.coding],
    [marketingStatus.jurisdiction.text],
    [marketingStatus.status.id],
    [marketingStatus.status.extension],
    [marketingStatus.status.coding],
    [marketingStatus.status.text],
    [marketingStatus.dateRange.id],
    [marketingStatus.dateRange.extension],
    [marketingStatus.dateRange.start],
    [marketingStatus.dateRange.end],
    [marketingStatus.restoreDate]
FROM openrowset (
        BULK 'PackagedProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [marketingStatus.JSON]  VARCHAR(MAX) '$.marketingStatus'
    ) AS rowset
    CROSS APPLY openjson (rowset.[marketingStatus.JSON]) with (
        [marketingStatus.id]           NVARCHAR(100)       '$.id',
        [marketingStatus.extension]    NVARCHAR(MAX)       '$.extension',
        [marketingStatus.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [marketingStatus.country.id]   NVARCHAR(100)       '$.country.id',
        [marketingStatus.country.extension] NVARCHAR(MAX)       '$.country.extension',
        [marketingStatus.country.coding] NVARCHAR(MAX)       '$.country.coding',
        [marketingStatus.country.text] NVARCHAR(4000)      '$.country.text',
        [marketingStatus.jurisdiction.id] NVARCHAR(100)       '$.jurisdiction.id',
        [marketingStatus.jurisdiction.extension] NVARCHAR(MAX)       '$.jurisdiction.extension',
        [marketingStatus.jurisdiction.coding] NVARCHAR(MAX)       '$.jurisdiction.coding',
        [marketingStatus.jurisdiction.text] NVARCHAR(4000)      '$.jurisdiction.text',
        [marketingStatus.status.id]    NVARCHAR(100)       '$.status.id',
        [marketingStatus.status.extension] NVARCHAR(MAX)       '$.status.extension',
        [marketingStatus.status.coding] NVARCHAR(MAX)       '$.status.coding',
        [marketingStatus.status.text]  NVARCHAR(4000)      '$.status.text',
        [marketingStatus.dateRange.id] NVARCHAR(100)       '$.dateRange.id',
        [marketingStatus.dateRange.extension] NVARCHAR(MAX)       '$.dateRange.extension',
        [marketingStatus.dateRange.start] VARCHAR(64)         '$.dateRange.start',
        [marketingStatus.dateRange.end] VARCHAR(64)         '$.dateRange.end',
        [marketingStatus.restoreDate]  VARCHAR(64)         '$.restoreDate'
    ) j

GO

CREATE VIEW fhir.PackagedProductDefinitionCharacteristic AS
SELECT
    [id],
    [characteristic.JSON],
    [characteristic.id],
    [characteristic.extension],
    [characteristic.coding],
    [characteristic.text]
FROM openrowset (
        BULK 'PackagedProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [characteristic.JSON]  VARCHAR(MAX) '$.characteristic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[characteristic.JSON]) with (
        [characteristic.id]            NVARCHAR(100)       '$.id',
        [characteristic.extension]     NVARCHAR(MAX)       '$.extension',
        [characteristic.coding]        NVARCHAR(MAX)       '$.coding' AS JSON,
        [characteristic.text]          NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.PackagedProductDefinitionManufacturer AS
SELECT
    [id],
    [manufacturer.JSON],
    [manufacturer.id],
    [manufacturer.extension],
    [manufacturer.reference],
    [manufacturer.type],
    [manufacturer.identifier.id],
    [manufacturer.identifier.extension],
    [manufacturer.identifier.use],
    [manufacturer.identifier.type],
    [manufacturer.identifier.system],
    [manufacturer.identifier.value],
    [manufacturer.identifier.period],
    [manufacturer.identifier.assigner],
    [manufacturer.display]
FROM openrowset (
        BULK 'PackagedProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [manufacturer.JSON]  VARCHAR(MAX) '$.manufacturer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[manufacturer.JSON]) with (
        [manufacturer.id]              NVARCHAR(100)       '$.id',
        [manufacturer.extension]       NVARCHAR(MAX)       '$.extension',
        [manufacturer.reference]       NVARCHAR(4000)      '$.reference',
        [manufacturer.type]            VARCHAR(256)        '$.type',
        [manufacturer.identifier.id]   NVARCHAR(100)       '$.identifier.id',
        [manufacturer.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [manufacturer.identifier.use]  NVARCHAR(64)        '$.identifier.use',
        [manufacturer.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [manufacturer.identifier.system] VARCHAR(256)        '$.identifier.system',
        [manufacturer.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [manufacturer.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [manufacturer.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [manufacturer.display]         NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.PackagedProductDefinitionAttachedDocument AS
SELECT
    [id],
    [attachedDocument.JSON],
    [attachedDocument.id],
    [attachedDocument.extension],
    [attachedDocument.reference],
    [attachedDocument.type],
    [attachedDocument.identifier.id],
    [attachedDocument.identifier.extension],
    [attachedDocument.identifier.use],
    [attachedDocument.identifier.type],
    [attachedDocument.identifier.system],
    [attachedDocument.identifier.value],
    [attachedDocument.identifier.period],
    [attachedDocument.identifier.assigner],
    [attachedDocument.display]
FROM openrowset (
        BULK 'PackagedProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [attachedDocument.JSON]  VARCHAR(MAX) '$.attachedDocument'
    ) AS rowset
    CROSS APPLY openjson (rowset.[attachedDocument.JSON]) with (
        [attachedDocument.id]          NVARCHAR(100)       '$.id',
        [attachedDocument.extension]   NVARCHAR(MAX)       '$.extension',
        [attachedDocument.reference]   NVARCHAR(4000)      '$.reference',
        [attachedDocument.type]        VARCHAR(256)        '$.type',
        [attachedDocument.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [attachedDocument.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [attachedDocument.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [attachedDocument.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [attachedDocument.identifier.system] VARCHAR(256)        '$.identifier.system',
        [attachedDocument.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [attachedDocument.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [attachedDocument.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [attachedDocument.display]     NVARCHAR(4000)      '$.display'
    ) j
