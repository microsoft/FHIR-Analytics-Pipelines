CREATE EXTERNAL TABLE [fhir].[Substance] (
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
    [instance] bit,
    [status] NVARCHAR(100),
    [category] VARCHAR(MAX),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.concept.id] NVARCHAR(100),
    [code.concept.extension] NVARCHAR(MAX),
    [code.concept.coding] NVARCHAR(MAX),
    [code.concept.text] NVARCHAR(4000),
    [code.reference.id] NVARCHAR(100),
    [code.reference.extension] NVARCHAR(MAX),
    [code.reference.reference] NVARCHAR(4000),
    [code.reference.type] VARCHAR(256),
    [code.reference.identifier] NVARCHAR(MAX),
    [code.reference.display] NVARCHAR(4000),
    [description] NVARCHAR(4000),
    [expiry] VARCHAR(64),
    [quantity.id] NVARCHAR(100),
    [quantity.extension] NVARCHAR(MAX),
    [quantity.value] float,
    [quantity.comparator] NVARCHAR(64),
    [quantity.unit] NVARCHAR(100),
    [quantity.system] VARCHAR(256),
    [quantity.code] NVARCHAR(4000),
    [ingredient] VARCHAR(MAX),
) WITH (
    LOCATION='/Substance/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SubstanceIdentifier AS
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
        BULK 'Substance/**',
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

CREATE VIEW fhir.SubstanceCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'Substance/**',
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

CREATE VIEW fhir.SubstanceIngredient AS
SELECT
    [id],
    [ingredient.JSON],
    [ingredient.id],
    [ingredient.extension],
    [ingredient.modifierExtension],
    [ingredient.quantity.id],
    [ingredient.quantity.extension],
    [ingredient.quantity.numerator],
    [ingredient.quantity.denominator],
    [ingredient.substance.codeableConcept.id],
    [ingredient.substance.codeableConcept.extension],
    [ingredient.substance.codeableConcept.coding],
    [ingredient.substance.codeableConcept.text],
    [ingredient.substance.reference.id],
    [ingredient.substance.reference.extension],
    [ingredient.substance.reference.reference],
    [ingredient.substance.reference.type],
    [ingredient.substance.reference.identifier],
    [ingredient.substance.reference.display]
FROM openrowset (
        BULK 'Substance/**',
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
        [ingredient.quantity.id]       NVARCHAR(100)       '$.quantity.id',
        [ingredient.quantity.extension] NVARCHAR(MAX)       '$.quantity.extension',
        [ingredient.quantity.numerator] NVARCHAR(MAX)       '$.quantity.numerator',
        [ingredient.quantity.denominator] NVARCHAR(MAX)       '$.quantity.denominator',
        [ingredient.substance.codeableConcept.id] NVARCHAR(100)       '$.substance.codeableConcept.id',
        [ingredient.substance.codeableConcept.extension] NVARCHAR(MAX)       '$.substance.codeableConcept.extension',
        [ingredient.substance.codeableConcept.coding] NVARCHAR(MAX)       '$.substance.codeableConcept.coding',
        [ingredient.substance.codeableConcept.text] NVARCHAR(4000)      '$.substance.codeableConcept.text',
        [ingredient.substance.reference.id] NVARCHAR(100)       '$.substance.reference.id',
        [ingredient.substance.reference.extension] NVARCHAR(MAX)       '$.substance.reference.extension',
        [ingredient.substance.reference.reference] NVARCHAR(4000)      '$.substance.reference.reference',
        [ingredient.substance.reference.type] VARCHAR(256)        '$.substance.reference.type',
        [ingredient.substance.reference.identifier] NVARCHAR(MAX)       '$.substance.reference.identifier',
        [ingredient.substance.reference.display] NVARCHAR(4000)      '$.substance.reference.display'
    ) j
