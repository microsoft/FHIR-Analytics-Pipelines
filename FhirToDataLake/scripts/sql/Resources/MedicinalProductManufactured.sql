CREATE EXTERNAL TABLE [fhir].[MedicinalProductManufactured] (
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
    [manufacturedDoseForm.id] NVARCHAR(100),
    [manufacturedDoseForm.extension] NVARCHAR(MAX),
    [manufacturedDoseForm.coding] VARCHAR(MAX),
    [manufacturedDoseForm.text] NVARCHAR(4000),
    [unitOfPresentation.id] NVARCHAR(100),
    [unitOfPresentation.extension] NVARCHAR(MAX),
    [unitOfPresentation.coding] VARCHAR(MAX),
    [unitOfPresentation.text] NVARCHAR(4000),
    [quantity.id] NVARCHAR(100),
    [quantity.extension] NVARCHAR(MAX),
    [quantity.value] float,
    [quantity.comparator] NVARCHAR(64),
    [quantity.unit] NVARCHAR(100),
    [quantity.system] VARCHAR(256),
    [quantity.code] NVARCHAR(4000),
    [manufacturer] VARCHAR(MAX),
    [ingredient] VARCHAR(MAX),
    [physicalCharacteristics.id] NVARCHAR(100),
    [physicalCharacteristics.extension] NVARCHAR(MAX),
    [physicalCharacteristics.modifierExtension] NVARCHAR(MAX),
    [physicalCharacteristics.height.id] NVARCHAR(100),
    [physicalCharacteristics.height.extension] NVARCHAR(MAX),
    [physicalCharacteristics.height.value] float,
    [physicalCharacteristics.height.comparator] NVARCHAR(64),
    [physicalCharacteristics.height.unit] NVARCHAR(100),
    [physicalCharacteristics.height.system] VARCHAR(256),
    [physicalCharacteristics.height.code] NVARCHAR(4000),
    [physicalCharacteristics.width.id] NVARCHAR(100),
    [physicalCharacteristics.width.extension] NVARCHAR(MAX),
    [physicalCharacteristics.width.value] float,
    [physicalCharacteristics.width.comparator] NVARCHAR(64),
    [physicalCharacteristics.width.unit] NVARCHAR(100),
    [physicalCharacteristics.width.system] VARCHAR(256),
    [physicalCharacteristics.width.code] NVARCHAR(4000),
    [physicalCharacteristics.depth.id] NVARCHAR(100),
    [physicalCharacteristics.depth.extension] NVARCHAR(MAX),
    [physicalCharacteristics.depth.value] float,
    [physicalCharacteristics.depth.comparator] NVARCHAR(64),
    [physicalCharacteristics.depth.unit] NVARCHAR(100),
    [physicalCharacteristics.depth.system] VARCHAR(256),
    [physicalCharacteristics.depth.code] NVARCHAR(4000),
    [physicalCharacteristics.weight.id] NVARCHAR(100),
    [physicalCharacteristics.weight.extension] NVARCHAR(MAX),
    [physicalCharacteristics.weight.value] float,
    [physicalCharacteristics.weight.comparator] NVARCHAR(64),
    [physicalCharacteristics.weight.unit] NVARCHAR(100),
    [physicalCharacteristics.weight.system] VARCHAR(256),
    [physicalCharacteristics.weight.code] NVARCHAR(4000),
    [physicalCharacteristics.nominalVolume.id] NVARCHAR(100),
    [physicalCharacteristics.nominalVolume.extension] NVARCHAR(MAX),
    [physicalCharacteristics.nominalVolume.value] float,
    [physicalCharacteristics.nominalVolume.comparator] NVARCHAR(64),
    [physicalCharacteristics.nominalVolume.unit] NVARCHAR(100),
    [physicalCharacteristics.nominalVolume.system] VARCHAR(256),
    [physicalCharacteristics.nominalVolume.code] NVARCHAR(4000),
    [physicalCharacteristics.externalDiameter.id] NVARCHAR(100),
    [physicalCharacteristics.externalDiameter.extension] NVARCHAR(MAX),
    [physicalCharacteristics.externalDiameter.value] float,
    [physicalCharacteristics.externalDiameter.comparator] NVARCHAR(64),
    [physicalCharacteristics.externalDiameter.unit] NVARCHAR(100),
    [physicalCharacteristics.externalDiameter.system] VARCHAR(256),
    [physicalCharacteristics.externalDiameter.code] NVARCHAR(4000),
    [physicalCharacteristics.shape] NVARCHAR(100),
    [physicalCharacteristics.color] VARCHAR(MAX),
    [physicalCharacteristics.imprint] VARCHAR(MAX),
    [physicalCharacteristics.image] VARCHAR(MAX),
    [physicalCharacteristics.scoring.id] NVARCHAR(100),
    [physicalCharacteristics.scoring.extension] NVARCHAR(MAX),
    [physicalCharacteristics.scoring.coding] NVARCHAR(MAX),
    [physicalCharacteristics.scoring.text] NVARCHAR(4000),
    [otherCharacteristics] VARCHAR(MAX),
) WITH (
    LOCATION='/MedicinalProductManufactured/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicinalProductManufacturedManufacturer AS
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
        BULK 'MedicinalProductManufactured/**',
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

CREATE VIEW fhir.MedicinalProductManufacturedIngredient AS
SELECT
    [id],
    [ingredient.JSON],
    [ingredient.id],
    [ingredient.extension],
    [ingredient.reference],
    [ingredient.type],
    [ingredient.identifier.id],
    [ingredient.identifier.extension],
    [ingredient.identifier.use],
    [ingredient.identifier.type],
    [ingredient.identifier.system],
    [ingredient.identifier.value],
    [ingredient.identifier.period],
    [ingredient.identifier.assigner],
    [ingredient.display]
FROM openrowset (
        BULK 'MedicinalProductManufactured/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [ingredient.JSON]  VARCHAR(MAX) '$.ingredient'
    ) AS rowset
    CROSS APPLY openjson (rowset.[ingredient.JSON]) with (
        [ingredient.id]                NVARCHAR(100)       '$.id',
        [ingredient.extension]         NVARCHAR(MAX)       '$.extension',
        [ingredient.reference]         NVARCHAR(4000)      '$.reference',
        [ingredient.type]              VARCHAR(256)        '$.type',
        [ingredient.identifier.id]     NVARCHAR(100)       '$.identifier.id',
        [ingredient.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [ingredient.identifier.use]    NVARCHAR(64)        '$.identifier.use',
        [ingredient.identifier.type]   NVARCHAR(MAX)       '$.identifier.type',
        [ingredient.identifier.system] VARCHAR(256)        '$.identifier.system',
        [ingredient.identifier.value]  NVARCHAR(4000)      '$.identifier.value',
        [ingredient.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [ingredient.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [ingredient.display]           NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductManufacturedOtherCharacteristics AS
SELECT
    [id],
    [otherCharacteristics.JSON],
    [otherCharacteristics.id],
    [otherCharacteristics.extension],
    [otherCharacteristics.coding],
    [otherCharacteristics.text]
FROM openrowset (
        BULK 'MedicinalProductManufactured/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [otherCharacteristics.JSON]  VARCHAR(MAX) '$.otherCharacteristics'
    ) AS rowset
    CROSS APPLY openjson (rowset.[otherCharacteristics.JSON]) with (
        [otherCharacteristics.id]      NVARCHAR(100)       '$.id',
        [otherCharacteristics.extension] NVARCHAR(MAX)       '$.extension',
        [otherCharacteristics.coding]  NVARCHAR(MAX)       '$.coding' AS JSON,
        [otherCharacteristics.text]    NVARCHAR(4000)      '$.text'
    ) j
