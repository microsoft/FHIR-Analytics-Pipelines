CREATE EXTERNAL TABLE [fhir].[MedicinalProductPackaged] (
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
    [subject] VARCHAR(MAX),
    [description] NVARCHAR(4000),
    [legalStatusOfSupply.id] NVARCHAR(100),
    [legalStatusOfSupply.extension] NVARCHAR(MAX),
    [legalStatusOfSupply.coding] VARCHAR(MAX),
    [legalStatusOfSupply.text] NVARCHAR(4000),
    [marketingStatus] VARCHAR(MAX),
    [marketingAuthorization.id] NVARCHAR(100),
    [marketingAuthorization.extension] NVARCHAR(MAX),
    [marketingAuthorization.reference] NVARCHAR(4000),
    [marketingAuthorization.type] VARCHAR(256),
    [marketingAuthorization.identifier.id] NVARCHAR(100),
    [marketingAuthorization.identifier.extension] NVARCHAR(MAX),
    [marketingAuthorization.identifier.use] NVARCHAR(64),
    [marketingAuthorization.identifier.type] NVARCHAR(MAX),
    [marketingAuthorization.identifier.system] VARCHAR(256),
    [marketingAuthorization.identifier.value] NVARCHAR(4000),
    [marketingAuthorization.identifier.period] NVARCHAR(MAX),
    [marketingAuthorization.identifier.assigner] NVARCHAR(MAX),
    [marketingAuthorization.display] NVARCHAR(4000),
    [manufacturer] VARCHAR(MAX),
    [batchIdentifier] VARCHAR(MAX),
    [packageItem] VARCHAR(MAX),
) WITH (
    LOCATION='/MedicinalProductPackaged/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicinalProductPackagedIdentifier AS
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
        BULK 'MedicinalProductPackaged/**',
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

CREATE VIEW fhir.MedicinalProductPackagedSubject AS
SELECT
    [id],
    [subject.JSON],
    [subject.id],
    [subject.extension],
    [subject.reference],
    [subject.type],
    [subject.identifier.id],
    [subject.identifier.extension],
    [subject.identifier.use],
    [subject.identifier.type],
    [subject.identifier.system],
    [subject.identifier.value],
    [subject.identifier.period],
    [subject.identifier.assigner],
    [subject.display]
FROM openrowset (
        BULK 'MedicinalProductPackaged/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subject.JSON]  VARCHAR(MAX) '$.subject'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subject.JSON]) with (
        [subject.id]                   NVARCHAR(100)       '$.id',
        [subject.extension]            NVARCHAR(MAX)       '$.extension',
        [subject.reference]            NVARCHAR(4000)      '$.reference',
        [subject.type]                 VARCHAR(256)        '$.type',
        [subject.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [subject.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [subject.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [subject.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [subject.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [subject.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [subject.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [subject.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [subject.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductPackagedMarketingStatus AS
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
        BULK 'MedicinalProductPackaged/**',
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

CREATE VIEW fhir.MedicinalProductPackagedManufacturer AS
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
        BULK 'MedicinalProductPackaged/**',
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

CREATE VIEW fhir.MedicinalProductPackagedBatchIdentifier AS
SELECT
    [id],
    [batchIdentifier.JSON],
    [batchIdentifier.id],
    [batchIdentifier.extension],
    [batchIdentifier.modifierExtension],
    [batchIdentifier.outerPackaging.id],
    [batchIdentifier.outerPackaging.extension],
    [batchIdentifier.outerPackaging.use],
    [batchIdentifier.outerPackaging.type],
    [batchIdentifier.outerPackaging.system],
    [batchIdentifier.outerPackaging.value],
    [batchIdentifier.outerPackaging.period],
    [batchIdentifier.outerPackaging.assigner],
    [batchIdentifier.immediatePackaging.id],
    [batchIdentifier.immediatePackaging.extension],
    [batchIdentifier.immediatePackaging.use],
    [batchIdentifier.immediatePackaging.type],
    [batchIdentifier.immediatePackaging.system],
    [batchIdentifier.immediatePackaging.value],
    [batchIdentifier.immediatePackaging.period],
    [batchIdentifier.immediatePackaging.assigner]
FROM openrowset (
        BULK 'MedicinalProductPackaged/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [batchIdentifier.JSON]  VARCHAR(MAX) '$.batchIdentifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[batchIdentifier.JSON]) with (
        [batchIdentifier.id]           NVARCHAR(100)       '$.id',
        [batchIdentifier.extension]    NVARCHAR(MAX)       '$.extension',
        [batchIdentifier.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [batchIdentifier.outerPackaging.id] NVARCHAR(100)       '$.outerPackaging.id',
        [batchIdentifier.outerPackaging.extension] NVARCHAR(MAX)       '$.outerPackaging.extension',
        [batchIdentifier.outerPackaging.use] NVARCHAR(64)        '$.outerPackaging.use',
        [batchIdentifier.outerPackaging.type] NVARCHAR(MAX)       '$.outerPackaging.type',
        [batchIdentifier.outerPackaging.system] VARCHAR(256)        '$.outerPackaging.system',
        [batchIdentifier.outerPackaging.value] NVARCHAR(4000)      '$.outerPackaging.value',
        [batchIdentifier.outerPackaging.period] NVARCHAR(MAX)       '$.outerPackaging.period',
        [batchIdentifier.outerPackaging.assigner] NVARCHAR(MAX)       '$.outerPackaging.assigner',
        [batchIdentifier.immediatePackaging.id] NVARCHAR(100)       '$.immediatePackaging.id',
        [batchIdentifier.immediatePackaging.extension] NVARCHAR(MAX)       '$.immediatePackaging.extension',
        [batchIdentifier.immediatePackaging.use] NVARCHAR(64)        '$.immediatePackaging.use',
        [batchIdentifier.immediatePackaging.type] NVARCHAR(MAX)       '$.immediatePackaging.type',
        [batchIdentifier.immediatePackaging.system] VARCHAR(256)        '$.immediatePackaging.system',
        [batchIdentifier.immediatePackaging.value] NVARCHAR(4000)      '$.immediatePackaging.value',
        [batchIdentifier.immediatePackaging.period] NVARCHAR(MAX)       '$.immediatePackaging.period',
        [batchIdentifier.immediatePackaging.assigner] NVARCHAR(MAX)       '$.immediatePackaging.assigner'
    ) j

GO

CREATE VIEW fhir.MedicinalProductPackagedPackageItem AS
SELECT
    [id],
    [packageItem.JSON],
    [packageItem.id],
    [packageItem.extension],
    [packageItem.modifierExtension],
    [packageItem.identifier],
    [packageItem.type.id],
    [packageItem.type.extension],
    [packageItem.type.coding],
    [packageItem.type.text],
    [packageItem.quantity.id],
    [packageItem.quantity.extension],
    [packageItem.quantity.value],
    [packageItem.quantity.comparator],
    [packageItem.quantity.unit],
    [packageItem.quantity.system],
    [packageItem.quantity.code],
    [packageItem.material],
    [packageItem.alternateMaterial],
    [packageItem.device],
    [packageItem.manufacturedItem],
    [packageItem.packageItem],
    [packageItem.physicalCharacteristics.id],
    [packageItem.physicalCharacteristics.extension],
    [packageItem.physicalCharacteristics.modifierExtension],
    [packageItem.physicalCharacteristics.height],
    [packageItem.physicalCharacteristics.width],
    [packageItem.physicalCharacteristics.depth],
    [packageItem.physicalCharacteristics.weight],
    [packageItem.physicalCharacteristics.nominalVolume],
    [packageItem.physicalCharacteristics.externalDiameter],
    [packageItem.physicalCharacteristics.shape],
    [packageItem.physicalCharacteristics.color],
    [packageItem.physicalCharacteristics.imprint],
    [packageItem.physicalCharacteristics.image],
    [packageItem.physicalCharacteristics.scoring],
    [packageItem.otherCharacteristics],
    [packageItem.shelfLifeStorage],
    [packageItem.manufacturer]
FROM openrowset (
        BULK 'MedicinalProductPackaged/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [packageItem.JSON]  VARCHAR(MAX) '$.packageItem'
    ) AS rowset
    CROSS APPLY openjson (rowset.[packageItem.JSON]) with (
        [packageItem.id]               NVARCHAR(100)       '$.id',
        [packageItem.extension]        NVARCHAR(MAX)       '$.extension',
        [packageItem.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [packageItem.identifier]       NVARCHAR(MAX)       '$.identifier' AS JSON,
        [packageItem.type.id]          NVARCHAR(100)       '$.type.id',
        [packageItem.type.extension]   NVARCHAR(MAX)       '$.type.extension',
        [packageItem.type.coding]      NVARCHAR(MAX)       '$.type.coding',
        [packageItem.type.text]        NVARCHAR(4000)      '$.type.text',
        [packageItem.quantity.id]      NVARCHAR(100)       '$.quantity.id',
        [packageItem.quantity.extension] NVARCHAR(MAX)       '$.quantity.extension',
        [packageItem.quantity.value]   float               '$.quantity.value',
        [packageItem.quantity.comparator] NVARCHAR(64)        '$.quantity.comparator',
        [packageItem.quantity.unit]    NVARCHAR(100)       '$.quantity.unit',
        [packageItem.quantity.system]  VARCHAR(256)        '$.quantity.system',
        [packageItem.quantity.code]    NVARCHAR(4000)      '$.quantity.code',
        [packageItem.material]         NVARCHAR(MAX)       '$.material' AS JSON,
        [packageItem.alternateMaterial] NVARCHAR(MAX)       '$.alternateMaterial' AS JSON,
        [packageItem.device]           NVARCHAR(MAX)       '$.device' AS JSON,
        [packageItem.manufacturedItem] NVARCHAR(MAX)       '$.manufacturedItem' AS JSON,
        [packageItem.packageItem]      NVARCHAR(MAX)       '$.packageItem' AS JSON,
        [packageItem.physicalCharacteristics.id] NVARCHAR(100)       '$.physicalCharacteristics.id',
        [packageItem.physicalCharacteristics.extension] NVARCHAR(MAX)       '$.physicalCharacteristics.extension',
        [packageItem.physicalCharacteristics.modifierExtension] NVARCHAR(MAX)       '$.physicalCharacteristics.modifierExtension',
        [packageItem.physicalCharacteristics.height] NVARCHAR(MAX)       '$.physicalCharacteristics.height',
        [packageItem.physicalCharacteristics.width] NVARCHAR(MAX)       '$.physicalCharacteristics.width',
        [packageItem.physicalCharacteristics.depth] NVARCHAR(MAX)       '$.physicalCharacteristics.depth',
        [packageItem.physicalCharacteristics.weight] NVARCHAR(MAX)       '$.physicalCharacteristics.weight',
        [packageItem.physicalCharacteristics.nominalVolume] NVARCHAR(MAX)       '$.physicalCharacteristics.nominalVolume',
        [packageItem.physicalCharacteristics.externalDiameter] NVARCHAR(MAX)       '$.physicalCharacteristics.externalDiameter',
        [packageItem.physicalCharacteristics.shape] NVARCHAR(100)       '$.physicalCharacteristics.shape',
        [packageItem.physicalCharacteristics.color] NVARCHAR(MAX)       '$.physicalCharacteristics.color',
        [packageItem.physicalCharacteristics.imprint] NVARCHAR(MAX)       '$.physicalCharacteristics.imprint',
        [packageItem.physicalCharacteristics.image] NVARCHAR(MAX)       '$.physicalCharacteristics.image',
        [packageItem.physicalCharacteristics.scoring] NVARCHAR(MAX)       '$.physicalCharacteristics.scoring',
        [packageItem.otherCharacteristics] NVARCHAR(MAX)       '$.otherCharacteristics' AS JSON,
        [packageItem.shelfLifeStorage] NVARCHAR(MAX)       '$.shelfLifeStorage' AS JSON,
        [packageItem.manufacturer]     NVARCHAR(MAX)       '$.manufacturer' AS JSON
    ) j
