CREATE EXTERNAL TABLE [fhir].[Medication] (
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
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [status] NVARCHAR(100),
    [manufacturer.id] NVARCHAR(100),
    [manufacturer.extension] NVARCHAR(MAX),
    [manufacturer.reference] NVARCHAR(4000),
    [manufacturer.type] VARCHAR(256),
    [manufacturer.identifier.id] NVARCHAR(100),
    [manufacturer.identifier.extension] NVARCHAR(MAX),
    [manufacturer.identifier.use] NVARCHAR(64),
    [manufacturer.identifier.type] NVARCHAR(MAX),
    [manufacturer.identifier.system] VARCHAR(256),
    [manufacturer.identifier.value] NVARCHAR(4000),
    [manufacturer.identifier.period] NVARCHAR(MAX),
    [manufacturer.identifier.assigner] NVARCHAR(MAX),
    [manufacturer.display] NVARCHAR(4000),
    [form.id] NVARCHAR(100),
    [form.extension] NVARCHAR(MAX),
    [form.coding] VARCHAR(MAX),
    [form.text] NVARCHAR(4000),
    [amount.id] NVARCHAR(100),
    [amount.extension] NVARCHAR(MAX),
    [amount.numerator.id] NVARCHAR(100),
    [amount.numerator.extension] NVARCHAR(MAX),
    [amount.numerator.value] float,
    [amount.numerator.comparator] NVARCHAR(64),
    [amount.numerator.unit] NVARCHAR(100),
    [amount.numerator.system] VARCHAR(256),
    [amount.numerator.code] NVARCHAR(4000),
    [amount.denominator.id] NVARCHAR(100),
    [amount.denominator.extension] NVARCHAR(MAX),
    [amount.denominator.value] float,
    [amount.denominator.comparator] NVARCHAR(64),
    [amount.denominator.unit] NVARCHAR(100),
    [amount.denominator.system] VARCHAR(256),
    [amount.denominator.code] NVARCHAR(4000),
    [ingredient] VARCHAR(MAX),
    [batch.id] NVARCHAR(100),
    [batch.extension] NVARCHAR(MAX),
    [batch.modifierExtension] NVARCHAR(MAX),
<<<<<<< HEAD
    [batch.lotNumber] NVARCHAR(100),
=======
    [batch.lotNumber] NVARCHAR(4000),
>>>>>>> origin/main
    [batch.expirationDate] VARCHAR(64),
) WITH (
    LOCATION='/Medication/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicationIdentifier AS
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
        BULK 'Medication/**',
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
<<<<<<< HEAD
        [identifier.assigner.id]       NVARCHAR(100)       '$.assigner.id',
=======
        [identifier.assigner.id]       NVARCHAR(4000)      '$.assigner.id',
>>>>>>> origin/main
        [identifier.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [identifier.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [identifier.assigner.type]     VARCHAR(256)        '$.assigner.type',
        [identifier.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [identifier.assigner.display]  NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.MedicationIngredient AS
SELECT
    [id],
    [ingredient.JSON],
    [ingredient.id],
    [ingredient.extension],
    [ingredient.modifierExtension],
    [ingredient.isActive],
    [ingredient.strength.id],
    [ingredient.strength.extension],
    [ingredient.strength.numerator],
    [ingredient.strength.denominator],
    [ingredient.item.codeableConcept.id],
    [ingredient.item.codeableConcept.extension],
    [ingredient.item.codeableConcept.coding],
    [ingredient.item.codeableConcept.text],
    [ingredient.item.reference.id],
    [ingredient.item.reference.extension],
    [ingredient.item.reference.reference],
    [ingredient.item.reference.type],
    [ingredient.item.reference.identifier],
    [ingredient.item.reference.display]
FROM openrowset (
        BULK 'Medication/**',
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
        [ingredient.isActive]          bit                 '$.isActive',
        [ingredient.strength.id]       NVARCHAR(100)       '$.strength.id',
        [ingredient.strength.extension] NVARCHAR(MAX)       '$.strength.extension',
        [ingredient.strength.numerator] NVARCHAR(MAX)       '$.strength.numerator',
        [ingredient.strength.denominator] NVARCHAR(MAX)       '$.strength.denominator',
        [ingredient.item.codeableConcept.id] NVARCHAR(100)       '$.item.codeableConcept.id',
        [ingredient.item.codeableConcept.extension] NVARCHAR(MAX)       '$.item.codeableConcept.extension',
        [ingredient.item.codeableConcept.coding] NVARCHAR(MAX)       '$.item.codeableConcept.coding',
        [ingredient.item.codeableConcept.text] NVARCHAR(4000)      '$.item.codeableConcept.text',
        [ingredient.item.reference.id] NVARCHAR(100)       '$.item.reference.id',
        [ingredient.item.reference.extension] NVARCHAR(MAX)       '$.item.reference.extension',
        [ingredient.item.reference.reference] NVARCHAR(4000)      '$.item.reference.reference',
        [ingredient.item.reference.type] VARCHAR(256)        '$.item.reference.type',
        [ingredient.item.reference.identifier] NVARCHAR(MAX)       '$.item.reference.identifier',
        [ingredient.item.reference.display] NVARCHAR(4000)      '$.item.reference.display'
    ) j
