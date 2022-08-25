CREATE EXTERNAL TABLE [fhir].[NutritionProduct] (
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
    [status] NVARCHAR(100),
    [category] VARCHAR(MAX),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [manufacturer] VARCHAR(MAX),
    [nutrient] VARCHAR(MAX),
    [ingredient] VARCHAR(MAX),
    [knownAllergen] VARCHAR(MAX),
    [productCharacteristic] VARCHAR(MAX),
    [instance.id] NVARCHAR(100),
    [instance.extension] NVARCHAR(MAX),
    [instance.modifierExtension] NVARCHAR(MAX),
    [instance.quantity.id] NVARCHAR(100),
    [instance.quantity.extension] NVARCHAR(MAX),
    [instance.quantity.value] float,
    [instance.quantity.comparator] NVARCHAR(64),
    [instance.quantity.unit] NVARCHAR(100),
    [instance.quantity.system] VARCHAR(256),
    [instance.quantity.code] NVARCHAR(4000),
    [instance.identifier] VARCHAR(MAX),
    [instance.lotNumber] NVARCHAR(100),
    [instance.expiry] VARCHAR(64),
    [instance.useBy] VARCHAR(64),
    [instance.biologicalSource.id] NVARCHAR(100),
    [instance.biologicalSource.extension] NVARCHAR(MAX),
    [instance.biologicalSource.use] NVARCHAR(64),
    [instance.biologicalSource.type] NVARCHAR(MAX),
    [instance.biologicalSource.system] VARCHAR(256),
    [instance.biologicalSource.value] NVARCHAR(4000),
    [instance.biologicalSource.period] NVARCHAR(MAX),
    [instance.biologicalSource.assigner] NVARCHAR(MAX),
    [note] VARCHAR(MAX),
) WITH (
    LOCATION='/NutritionProduct/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.NutritionProductCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'NutritionProduct/**',
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

CREATE VIEW fhir.NutritionProductManufacturer AS
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
        BULK 'NutritionProduct/**',
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

CREATE VIEW fhir.NutritionProductNutrient AS
SELECT
    [id],
    [nutrient.JSON],
    [nutrient.id],
    [nutrient.extension],
    [nutrient.modifierExtension],
    [nutrient.item.id],
    [nutrient.item.extension],
    [nutrient.item.concept],
    [nutrient.item.reference],
    [nutrient.amount]
FROM openrowset (
        BULK 'NutritionProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [nutrient.JSON]  VARCHAR(MAX) '$.nutrient'
    ) AS rowset
    CROSS APPLY openjson (rowset.[nutrient.JSON]) with (
        [nutrient.id]                  NVARCHAR(100)       '$.id',
        [nutrient.extension]           NVARCHAR(MAX)       '$.extension',
        [nutrient.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [nutrient.item.id]             NVARCHAR(100)       '$.item.id',
        [nutrient.item.extension]      NVARCHAR(MAX)       '$.item.extension',
        [nutrient.item.concept]        NVARCHAR(MAX)       '$.item.concept',
        [nutrient.item.reference]      NVARCHAR(MAX)       '$.item.reference',
        [nutrient.amount]              NVARCHAR(MAX)       '$.amount' AS JSON
    ) j

GO

CREATE VIEW fhir.NutritionProductIngredient AS
SELECT
    [id],
    [ingredient.JSON],
    [ingredient.id],
    [ingredient.extension],
    [ingredient.modifierExtension],
    [ingredient.item.id],
    [ingredient.item.extension],
    [ingredient.item.concept],
    [ingredient.item.reference],
    [ingredient.amount]
FROM openrowset (
        BULK 'NutritionProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [ingredient.JSON]  VARCHAR(MAX) '$.ingredient'
    ) AS rowset
    CROSS APPLY openjson (rowset.[ingredient.JSON]) with (
        [ingredient.id]                NVARCHAR(100)       '$.id',
        [ingredient.extension]         NVARCHAR(MAX)       '$.extension',
        [ingredient.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [ingredient.item.id]           NVARCHAR(100)       '$.item.id',
        [ingredient.item.extension]    NVARCHAR(MAX)       '$.item.extension',
        [ingredient.item.concept]      NVARCHAR(MAX)       '$.item.concept',
        [ingredient.item.reference]    NVARCHAR(MAX)       '$.item.reference',
        [ingredient.amount]            NVARCHAR(MAX)       '$.amount' AS JSON
    ) j

GO

CREATE VIEW fhir.NutritionProductKnownAllergen AS
SELECT
    [id],
    [knownAllergen.JSON],
    [knownAllergen.id],
    [knownAllergen.extension],
    [knownAllergen.concept.id],
    [knownAllergen.concept.extension],
    [knownAllergen.concept.coding],
    [knownAllergen.concept.text],
    [knownAllergen.reference.id],
    [knownAllergen.reference.extension],
    [knownAllergen.reference.reference],
    [knownAllergen.reference.type],
    [knownAllergen.reference.identifier],
    [knownAllergen.reference.display]
FROM openrowset (
        BULK 'NutritionProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [knownAllergen.JSON]  VARCHAR(MAX) '$.knownAllergen'
    ) AS rowset
    CROSS APPLY openjson (rowset.[knownAllergen.JSON]) with (
        [knownAllergen.id]             NVARCHAR(100)       '$.id',
        [knownAllergen.extension]      NVARCHAR(MAX)       '$.extension',
        [knownAllergen.concept.id]     NVARCHAR(100)       '$.concept.id',
        [knownAllergen.concept.extension] NVARCHAR(MAX)       '$.concept.extension',
        [knownAllergen.concept.coding] NVARCHAR(MAX)       '$.concept.coding',
        [knownAllergen.concept.text]   NVARCHAR(4000)      '$.concept.text',
        [knownAllergen.reference.id]   NVARCHAR(100)       '$.reference.id',
        [knownAllergen.reference.extension] NVARCHAR(MAX)       '$.reference.extension',
        [knownAllergen.reference.reference] NVARCHAR(4000)      '$.reference.reference',
        [knownAllergen.reference.type] VARCHAR(256)        '$.reference.type',
        [knownAllergen.reference.identifier] NVARCHAR(MAX)       '$.reference.identifier',
        [knownAllergen.reference.display] NVARCHAR(4000)      '$.reference.display'
    ) j

GO

CREATE VIEW fhir.NutritionProductProductCharacteristic AS
SELECT
    [id],
    [productCharacteristic.JSON],
    [productCharacteristic.id],
    [productCharacteristic.extension],
    [productCharacteristic.modifierExtension],
    [productCharacteristic.type.id],
    [productCharacteristic.type.extension],
    [productCharacteristic.type.coding],
    [productCharacteristic.type.text],
    [productCharacteristic.valueQuantity.id],
    [productCharacteristic.valueQuantity.extension],
    [productCharacteristic.valueQuantity.value],
    [productCharacteristic.valueQuantity.comparator],
    [productCharacteristic.valueQuantity.unit],
    [productCharacteristic.valueQuantity.system],
    [productCharacteristic.valueQuantity.code],
    [productCharacteristic.value.codeableConcept.id],
    [productCharacteristic.value.codeableConcept.extension],
    [productCharacteristic.value.codeableConcept.coding],
    [productCharacteristic.value.codeableConcept.text],
    [productCharacteristic.value.string],
    [productCharacteristic.value.quantity.id],
    [productCharacteristic.value.quantity.extension],
    [productCharacteristic.value.quantity.value],
    [productCharacteristic.value.quantity.comparator],
    [productCharacteristic.value.quantity.unit],
    [productCharacteristic.value.quantity.system],
    [productCharacteristic.value.quantity.code],
    [productCharacteristic.value.base64Binary],
    [productCharacteristic.value.attachment.id],
    [productCharacteristic.value.attachment.extension],
    [productCharacteristic.value.attachment.contentType],
    [productCharacteristic.value.attachment.language],
    [productCharacteristic.value.attachment.data],
    [productCharacteristic.value.attachment.url],
    [productCharacteristic.value.attachment.size],
    [productCharacteristic.value.attachment.hash],
    [productCharacteristic.value.attachment.title],
    [productCharacteristic.value.attachment.creation],
    [productCharacteristic.value.attachment.height],
    [productCharacteristic.value.attachment.width],
    [productCharacteristic.value.attachment.frames],
    [productCharacteristic.value.attachment.duration],
    [productCharacteristic.value.attachment.pages],
    [productCharacteristic.value.boolean]
FROM openrowset (
        BULK 'NutritionProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [productCharacteristic.JSON]  VARCHAR(MAX) '$.productCharacteristic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[productCharacteristic.JSON]) with (
        [productCharacteristic.id]     NVARCHAR(100)       '$.id',
        [productCharacteristic.extension] NVARCHAR(MAX)       '$.extension',
        [productCharacteristic.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [productCharacteristic.type.id] NVARCHAR(100)       '$.type.id',
        [productCharacteristic.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [productCharacteristic.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [productCharacteristic.type.text] NVARCHAR(4000)      '$.type.text',
        [productCharacteristic.valueQuantity.id] NVARCHAR(100)       '$.valueQuantity.id',
        [productCharacteristic.valueQuantity.extension] NVARCHAR(MAX)       '$.valueQuantity.extension',
        [productCharacteristic.valueQuantity.value] float               '$.valueQuantity.value',
        [productCharacteristic.valueQuantity.comparator] NVARCHAR(64)        '$.valueQuantity.comparator',
        [productCharacteristic.valueQuantity.unit] NVARCHAR(100)       '$.valueQuantity.unit',
        [productCharacteristic.valueQuantity.system] VARCHAR(256)        '$.valueQuantity.system',
        [productCharacteristic.valueQuantity.code] NVARCHAR(4000)      '$.valueQuantity.code',
        [productCharacteristic.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [productCharacteristic.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [productCharacteristic.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [productCharacteristic.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [productCharacteristic.value.string] NVARCHAR(4000)      '$.value.string',
        [productCharacteristic.value.quantity.id] NVARCHAR(100)       '$.value.quantity.id',
        [productCharacteristic.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [productCharacteristic.value.quantity.value] float               '$.value.quantity.value',
        [productCharacteristic.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [productCharacteristic.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [productCharacteristic.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [productCharacteristic.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [productCharacteristic.value.base64Binary] NVARCHAR(MAX)       '$.value.base64Binary',
        [productCharacteristic.value.attachment.id] NVARCHAR(100)       '$.value.attachment.id',
        [productCharacteristic.value.attachment.extension] NVARCHAR(MAX)       '$.value.attachment.extension',
        [productCharacteristic.value.attachment.contentType] NVARCHAR(100)       '$.value.attachment.contentType',
        [productCharacteristic.value.attachment.language] NVARCHAR(100)       '$.value.attachment.language',
        [productCharacteristic.value.attachment.data] NVARCHAR(MAX)       '$.value.attachment.data',
        [productCharacteristic.value.attachment.url] VARCHAR(256)        '$.value.attachment.url',
        [productCharacteristic.value.attachment.size] NVARCHAR(MAX)       '$.value.attachment.size',
        [productCharacteristic.value.attachment.hash] NVARCHAR(MAX)       '$.value.attachment.hash',
        [productCharacteristic.value.attachment.title] NVARCHAR(4000)      '$.value.attachment.title',
        [productCharacteristic.value.attachment.creation] VARCHAR(64)         '$.value.attachment.creation',
        [productCharacteristic.value.attachment.height] bigint              '$.value.attachment.height',
        [productCharacteristic.value.attachment.width] bigint              '$.value.attachment.width',
        [productCharacteristic.value.attachment.frames] bigint              '$.value.attachment.frames',
        [productCharacteristic.value.attachment.duration] float               '$.value.attachment.duration',
        [productCharacteristic.value.attachment.pages] bigint              '$.value.attachment.pages',
        [productCharacteristic.value.boolean] bit                 '$.value.boolean'
    ) j

GO

CREATE VIEW fhir.NutritionProductNote AS
SELECT
    [id],
    [note.JSON],
    [note.id],
    [note.extension],
    [note.time],
    [note.text],
    [note.author.reference.id],
    [note.author.reference.extension],
    [note.author.reference.reference],
    [note.author.reference.type],
    [note.author.reference.identifier],
    [note.author.reference.display],
    [note.author.string]
FROM openrowset (
        BULK 'NutritionProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [note.JSON]  VARCHAR(MAX) '$.note'
    ) AS rowset
    CROSS APPLY openjson (rowset.[note.JSON]) with (
        [note.id]                      NVARCHAR(100)       '$.id',
        [note.extension]               NVARCHAR(MAX)       '$.extension',
        [note.time]                    VARCHAR(64)         '$.time',
        [note.text]                    NVARCHAR(MAX)       '$.text',
        [note.author.reference.id]     NVARCHAR(100)       '$.author.reference.id',
        [note.author.reference.extension] NVARCHAR(MAX)       '$.author.reference.extension',
        [note.author.reference.reference] NVARCHAR(4000)      '$.author.reference.reference',
        [note.author.reference.type]   VARCHAR(256)        '$.author.reference.type',
        [note.author.reference.identifier] NVARCHAR(MAX)       '$.author.reference.identifier',
        [note.author.reference.display] NVARCHAR(4000)      '$.author.reference.display',
        [note.author.string]           NVARCHAR(4000)      '$.author.string'
    ) j
