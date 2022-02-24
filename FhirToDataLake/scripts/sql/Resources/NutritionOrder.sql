CREATE EXTERNAL TABLE [fhir].[NutritionOrder] (
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
    [instantiatesCanonical] VARCHAR(MAX),
    [instantiatesUri] VARCHAR(MAX),
    [instantiates] VARCHAR(MAX),
    [status] NVARCHAR(100),
    [intent] NVARCHAR(100),
    [patient.id] NVARCHAR(100),
    [patient.extension] NVARCHAR(MAX),
    [patient.reference] NVARCHAR(4000),
    [patient.type] VARCHAR(256),
    [patient.identifier.id] NVARCHAR(100),
    [patient.identifier.extension] NVARCHAR(MAX),
    [patient.identifier.use] NVARCHAR(64),
    [patient.identifier.type] NVARCHAR(MAX),
    [patient.identifier.system] VARCHAR(256),
    [patient.identifier.value] NVARCHAR(4000),
    [patient.identifier.period] NVARCHAR(MAX),
    [patient.identifier.assigner] NVARCHAR(MAX),
    [patient.display] NVARCHAR(4000),
    [encounter.id] NVARCHAR(100),
    [encounter.extension] NVARCHAR(MAX),
    [encounter.reference] NVARCHAR(4000),
    [encounter.type] VARCHAR(256),
    [encounter.identifier.id] NVARCHAR(100),
    [encounter.identifier.extension] NVARCHAR(MAX),
    [encounter.identifier.use] NVARCHAR(64),
    [encounter.identifier.type] NVARCHAR(MAX),
    [encounter.identifier.system] VARCHAR(256),
    [encounter.identifier.value] NVARCHAR(4000),
    [encounter.identifier.period] NVARCHAR(MAX),
    [encounter.identifier.assigner] NVARCHAR(MAX),
    [encounter.display] NVARCHAR(4000),
    [dateTime] VARCHAR(64),
    [orderer.id] NVARCHAR(100),
    [orderer.extension] NVARCHAR(MAX),
    [orderer.reference] NVARCHAR(4000),
    [orderer.type] VARCHAR(256),
    [orderer.identifier.id] NVARCHAR(100),
    [orderer.identifier.extension] NVARCHAR(MAX),
    [orderer.identifier.use] NVARCHAR(64),
    [orderer.identifier.type] NVARCHAR(MAX),
    [orderer.identifier.system] VARCHAR(256),
    [orderer.identifier.value] NVARCHAR(4000),
    [orderer.identifier.period] NVARCHAR(MAX),
    [orderer.identifier.assigner] NVARCHAR(MAX),
    [orderer.display] NVARCHAR(4000),
    [allergyIntolerance] VARCHAR(MAX),
    [foodPreferenceModifier] VARCHAR(MAX),
    [excludeFoodModifier] VARCHAR(MAX),
    [oralDiet.id] NVARCHAR(100),
    [oralDiet.extension] NVARCHAR(MAX),
    [oralDiet.modifierExtension] NVARCHAR(MAX),
    [oralDiet.type] VARCHAR(MAX),
    [oralDiet.schedule] VARCHAR(MAX),
    [oralDiet.nutrient] VARCHAR(MAX),
    [oralDiet.texture] VARCHAR(MAX),
    [oralDiet.fluidConsistencyType] VARCHAR(MAX),
    [oralDiet.instruction] NVARCHAR(4000),
    [supplement] VARCHAR(MAX),
    [enteralFormula.id] NVARCHAR(100),
    [enteralFormula.extension] NVARCHAR(MAX),
    [enteralFormula.modifierExtension] NVARCHAR(MAX),
    [enteralFormula.baseFormulaType.id] NVARCHAR(100),
    [enteralFormula.baseFormulaType.extension] NVARCHAR(MAX),
    [enteralFormula.baseFormulaType.coding] NVARCHAR(MAX),
    [enteralFormula.baseFormulaType.text] NVARCHAR(4000),
    [enteralFormula.baseFormulaProductName] NVARCHAR(500),
    [enteralFormula.additiveType.id] NVARCHAR(100),
    [enteralFormula.additiveType.extension] NVARCHAR(MAX),
    [enteralFormula.additiveType.coding] NVARCHAR(MAX),
    [enteralFormula.additiveType.text] NVARCHAR(4000),
    [enteralFormula.additiveProductName] NVARCHAR(500),
    [enteralFormula.caloricDensity.id] NVARCHAR(100),
    [enteralFormula.caloricDensity.extension] NVARCHAR(MAX),
    [enteralFormula.caloricDensity.value] float,
    [enteralFormula.caloricDensity.comparator] NVARCHAR(64),
    [enteralFormula.caloricDensity.unit] NVARCHAR(100),
    [enteralFormula.caloricDensity.system] VARCHAR(256),
    [enteralFormula.caloricDensity.code] NVARCHAR(4000),
    [enteralFormula.routeofAdministration.id] NVARCHAR(100),
    [enteralFormula.routeofAdministration.extension] NVARCHAR(MAX),
    [enteralFormula.routeofAdministration.coding] NVARCHAR(MAX),
    [enteralFormula.routeofAdministration.text] NVARCHAR(4000),
    [enteralFormula.administration] VARCHAR(MAX),
    [enteralFormula.maxVolumeToDeliver.id] NVARCHAR(100),
    [enteralFormula.maxVolumeToDeliver.extension] NVARCHAR(MAX),
    [enteralFormula.maxVolumeToDeliver.value] float,
    [enteralFormula.maxVolumeToDeliver.comparator] NVARCHAR(64),
    [enteralFormula.maxVolumeToDeliver.unit] NVARCHAR(100),
    [enteralFormula.maxVolumeToDeliver.system] VARCHAR(256),
    [enteralFormula.maxVolumeToDeliver.code] NVARCHAR(4000),
    [enteralFormula.administrationInstruction] NVARCHAR(4000),
    [note] VARCHAR(MAX),
) WITH (
    LOCATION='/NutritionOrder/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.NutritionOrderIdentifier AS
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
        BULK 'NutritionOrder/**',
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

CREATE VIEW fhir.NutritionOrderInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'NutritionOrder/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesCanonical.JSON]  VARCHAR(MAX) '$.instantiatesCanonical'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesCanonical.JSON]) with (
        [instantiatesCanonical]        NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.NutritionOrderInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'NutritionOrder/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesUri.JSON]  VARCHAR(MAX) '$.instantiatesUri'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesUri.JSON]) with (
        [instantiatesUri]              NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.NutritionOrderInstantiates AS
SELECT
    [id],
    [instantiates.JSON],
    [instantiates]
FROM openrowset (
        BULK 'NutritionOrder/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiates.JSON]  VARCHAR(MAX) '$.instantiates'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiates.JSON]) with (
        [instantiates]                 NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.NutritionOrderAllergyIntolerance AS
SELECT
    [id],
    [allergyIntolerance.JSON],
    [allergyIntolerance.id],
    [allergyIntolerance.extension],
    [allergyIntolerance.reference],
    [allergyIntolerance.type],
    [allergyIntolerance.identifier.id],
    [allergyIntolerance.identifier.extension],
    [allergyIntolerance.identifier.use],
    [allergyIntolerance.identifier.type],
    [allergyIntolerance.identifier.system],
    [allergyIntolerance.identifier.value],
    [allergyIntolerance.identifier.period],
    [allergyIntolerance.identifier.assigner],
    [allergyIntolerance.display]
FROM openrowset (
        BULK 'NutritionOrder/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [allergyIntolerance.JSON]  VARCHAR(MAX) '$.allergyIntolerance'
    ) AS rowset
    CROSS APPLY openjson (rowset.[allergyIntolerance.JSON]) with (
        [allergyIntolerance.id]        NVARCHAR(100)       '$.id',
        [allergyIntolerance.extension] NVARCHAR(MAX)       '$.extension',
        [allergyIntolerance.reference] NVARCHAR(4000)      '$.reference',
        [allergyIntolerance.type]      VARCHAR(256)        '$.type',
        [allergyIntolerance.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [allergyIntolerance.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [allergyIntolerance.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [allergyIntolerance.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [allergyIntolerance.identifier.system] VARCHAR(256)        '$.identifier.system',
        [allergyIntolerance.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [allergyIntolerance.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [allergyIntolerance.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [allergyIntolerance.display]   NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.NutritionOrderFoodPreferenceModifier AS
SELECT
    [id],
    [foodPreferenceModifier.JSON],
    [foodPreferenceModifier.id],
    [foodPreferenceModifier.extension],
    [foodPreferenceModifier.coding],
    [foodPreferenceModifier.text]
FROM openrowset (
        BULK 'NutritionOrder/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [foodPreferenceModifier.JSON]  VARCHAR(MAX) '$.foodPreferenceModifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[foodPreferenceModifier.JSON]) with (
        [foodPreferenceModifier.id]    NVARCHAR(100)       '$.id',
        [foodPreferenceModifier.extension] NVARCHAR(MAX)       '$.extension',
        [foodPreferenceModifier.coding] NVARCHAR(MAX)       '$.coding' AS JSON,
        [foodPreferenceModifier.text]  NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.NutritionOrderExcludeFoodModifier AS
SELECT
    [id],
    [excludeFoodModifier.JSON],
    [excludeFoodModifier.id],
    [excludeFoodModifier.extension],
    [excludeFoodModifier.coding],
    [excludeFoodModifier.text]
FROM openrowset (
        BULK 'NutritionOrder/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [excludeFoodModifier.JSON]  VARCHAR(MAX) '$.excludeFoodModifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[excludeFoodModifier.JSON]) with (
        [excludeFoodModifier.id]       NVARCHAR(100)       '$.id',
        [excludeFoodModifier.extension] NVARCHAR(MAX)       '$.extension',
        [excludeFoodModifier.coding]   NVARCHAR(MAX)       '$.coding' AS JSON,
        [excludeFoodModifier.text]     NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.NutritionOrderSupplement AS
SELECT
    [id],
    [supplement.JSON],
    [supplement.id],
    [supplement.extension],
    [supplement.modifierExtension],
    [supplement.type.id],
    [supplement.type.extension],
    [supplement.type.coding],
    [supplement.type.text],
    [supplement.productName],
    [supplement.schedule],
    [supplement.quantity.id],
    [supplement.quantity.extension],
    [supplement.quantity.value],
    [supplement.quantity.comparator],
    [supplement.quantity.unit],
    [supplement.quantity.system],
    [supplement.quantity.code],
    [supplement.instruction]
FROM openrowset (
        BULK 'NutritionOrder/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [supplement.JSON]  VARCHAR(MAX) '$.supplement'
    ) AS rowset
    CROSS APPLY openjson (rowset.[supplement.JSON]) with (
        [supplement.id]                NVARCHAR(100)       '$.id',
        [supplement.extension]         NVARCHAR(MAX)       '$.extension',
        [supplement.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [supplement.type.id]           NVARCHAR(100)       '$.type.id',
        [supplement.type.extension]    NVARCHAR(MAX)       '$.type.extension',
        [supplement.type.coding]       NVARCHAR(MAX)       '$.type.coding',
        [supplement.type.text]         NVARCHAR(4000)      '$.type.text',
        [supplement.productName]       NVARCHAR(500)       '$.productName',
        [supplement.schedule]          NVARCHAR(MAX)       '$.schedule' AS JSON,
        [supplement.quantity.id]       NVARCHAR(100)       '$.quantity.id',
        [supplement.quantity.extension] NVARCHAR(MAX)       '$.quantity.extension',
        [supplement.quantity.value]    float               '$.quantity.value',
        [supplement.quantity.comparator] NVARCHAR(64)        '$.quantity.comparator',
        [supplement.quantity.unit]     NVARCHAR(100)       '$.quantity.unit',
        [supplement.quantity.system]   VARCHAR(256)        '$.quantity.system',
        [supplement.quantity.code]     NVARCHAR(4000)      '$.quantity.code',
        [supplement.instruction]       NVARCHAR(4000)      '$.instruction'
    ) j

GO

CREATE VIEW fhir.NutritionOrderNote AS
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
        BULK 'NutritionOrder/**',
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
