CREATE EXTERNAL TABLE [fhir].[ManufacturedItemDefinition] (
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
    [manufacturedDoseForm.id] NVARCHAR(100),
    [manufacturedDoseForm.extension] NVARCHAR(MAX),
    [manufacturedDoseForm.coding] VARCHAR(MAX),
    [manufacturedDoseForm.text] NVARCHAR(4000),
    [unitOfPresentation.id] NVARCHAR(100),
    [unitOfPresentation.extension] NVARCHAR(MAX),
    [unitOfPresentation.coding] VARCHAR(MAX),
    [unitOfPresentation.text] NVARCHAR(4000),
    [manufacturer] VARCHAR(MAX),
    [ingredient] VARCHAR(MAX),
    [property] VARCHAR(MAX),
) WITH (
    LOCATION='/ManufacturedItemDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ManufacturedItemDefinitionIdentifier AS
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
        BULK 'ManufacturedItemDefinition/**',
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

CREATE VIEW fhir.ManufacturedItemDefinitionManufacturer AS
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
        BULK 'ManufacturedItemDefinition/**',
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

CREATE VIEW fhir.ManufacturedItemDefinitionIngredient AS
SELECT
    [id],
    [ingredient.JSON],
    [ingredient.id],
    [ingredient.extension],
    [ingredient.coding],
    [ingredient.text]
FROM openrowset (
        BULK 'ManufacturedItemDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [ingredient.JSON]  VARCHAR(MAX) '$.ingredient'
    ) AS rowset
    CROSS APPLY openjson (rowset.[ingredient.JSON]) with (
        [ingredient.id]                NVARCHAR(100)       '$.id',
        [ingredient.extension]         NVARCHAR(MAX)       '$.extension',
        [ingredient.coding]            NVARCHAR(MAX)       '$.coding' AS JSON,
        [ingredient.text]              NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ManufacturedItemDefinitionProperty AS
SELECT
    [id],
    [property.JSON],
    [property.id],
    [property.extension],
    [property.modifierExtension],
    [property.type.id],
    [property.type.extension],
    [property.type.coding],
    [property.type.text],
    [property.value.codeableConcept.id],
    [property.value.codeableConcept.extension],
    [property.value.codeableConcept.coding],
    [property.value.codeableConcept.text],
    [property.value.quantity.id],
    [property.value.quantity.extension],
    [property.value.quantity.value],
    [property.value.quantity.comparator],
    [property.value.quantity.unit],
    [property.value.quantity.system],
    [property.value.quantity.code],
    [property.value.date],
    [property.value.boolean],
    [property.value.attachment.id],
    [property.value.attachment.extension],
    [property.value.attachment.contentType],
    [property.value.attachment.language],
    [property.value.attachment.data],
    [property.value.attachment.url],
    [property.value.attachment.size],
    [property.value.attachment.hash],
    [property.value.attachment.title],
    [property.value.attachment.creation],
    [property.value.attachment.height],
    [property.value.attachment.width],
    [property.value.attachment.frames],
    [property.value.attachment.duration],
    [property.value.attachment.pages]
FROM openrowset (
        BULK 'ManufacturedItemDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [property.JSON]  VARCHAR(MAX) '$.property'
    ) AS rowset
    CROSS APPLY openjson (rowset.[property.JSON]) with (
        [property.id]                  NVARCHAR(100)       '$.id',
        [property.extension]           NVARCHAR(MAX)       '$.extension',
        [property.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [property.type.id]             NVARCHAR(100)       '$.type.id',
        [property.type.extension]      NVARCHAR(MAX)       '$.type.extension',
        [property.type.coding]         NVARCHAR(MAX)       '$.type.coding',
        [property.type.text]           NVARCHAR(4000)      '$.type.text',
        [property.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [property.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [property.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [property.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [property.value.quantity.id]   NVARCHAR(100)       '$.value.quantity.id',
        [property.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [property.value.quantity.value] float               '$.value.quantity.value',
        [property.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [property.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [property.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [property.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [property.value.date]          VARCHAR(64)         '$.value.date',
        [property.value.boolean]       bit                 '$.value.boolean',
        [property.value.attachment.id] NVARCHAR(100)       '$.value.attachment.id',
        [property.value.attachment.extension] NVARCHAR(MAX)       '$.value.attachment.extension',
        [property.value.attachment.contentType] NVARCHAR(100)       '$.value.attachment.contentType',
        [property.value.attachment.language] NVARCHAR(100)       '$.value.attachment.language',
        [property.value.attachment.data] NVARCHAR(MAX)       '$.value.attachment.data',
        [property.value.attachment.url] VARCHAR(256)        '$.value.attachment.url',
        [property.value.attachment.size] NVARCHAR(MAX)       '$.value.attachment.size',
        [property.value.attachment.hash] NVARCHAR(MAX)       '$.value.attachment.hash',
        [property.value.attachment.title] NVARCHAR(4000)      '$.value.attachment.title',
        [property.value.attachment.creation] VARCHAR(64)         '$.value.attachment.creation',
        [property.value.attachment.height] bigint              '$.value.attachment.height',
        [property.value.attachment.width] bigint              '$.value.attachment.width',
        [property.value.attachment.frames] bigint              '$.value.attachment.frames',
        [property.value.attachment.duration] float               '$.value.attachment.duration',
        [property.value.attachment.pages] bigint              '$.value.attachment.pages'
    ) j
