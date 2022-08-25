CREATE EXTERNAL TABLE [fhir].[NutritionIntake] (
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
    [basedOn] VARCHAR(MAX),
    [partOf] VARCHAR(MAX),
    [status] NVARCHAR(100),
    [statusReason] VARCHAR(MAX),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [subject.id] NVARCHAR(100),
    [subject.extension] NVARCHAR(MAX),
    [subject.reference] NVARCHAR(4000),
    [subject.type] VARCHAR(256),
    [subject.identifier.id] NVARCHAR(100),
    [subject.identifier.extension] NVARCHAR(MAX),
    [subject.identifier.use] NVARCHAR(64),
    [subject.identifier.type] NVARCHAR(MAX),
    [subject.identifier.system] VARCHAR(256),
    [subject.identifier.value] NVARCHAR(4000),
    [subject.identifier.period] NVARCHAR(MAX),
    [subject.identifier.assigner] NVARCHAR(MAX),
    [subject.display] NVARCHAR(4000),
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
    [recorded] VARCHAR(64),
    [consumedItem] VARCHAR(MAX),
    [ingredientLabel] VARCHAR(MAX),
    [performer] VARCHAR(MAX),
    [location.id] NVARCHAR(100),
    [location.extension] NVARCHAR(MAX),
    [location.reference] NVARCHAR(4000),
    [location.type] VARCHAR(256),
    [location.identifier.id] NVARCHAR(100),
    [location.identifier.extension] NVARCHAR(MAX),
    [location.identifier.use] NVARCHAR(64),
    [location.identifier.type] NVARCHAR(MAX),
    [location.identifier.system] VARCHAR(256),
    [location.identifier.value] NVARCHAR(4000),
    [location.identifier.period] NVARCHAR(MAX),
    [location.identifier.assigner] NVARCHAR(MAX),
    [location.display] NVARCHAR(4000),
    [derivedFrom] VARCHAR(MAX),
    [reason] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [occurrence.dateTime] VARCHAR(64),
    [occurrence.period.id] NVARCHAR(100),
    [occurrence.period.extension] NVARCHAR(MAX),
    [occurrence.period.start] VARCHAR(64),
    [occurrence.period.end] VARCHAR(64),
    [reported.boolean] bit,
    [reported.reference.id] NVARCHAR(100),
    [reported.reference.extension] NVARCHAR(MAX),
    [reported.reference.reference] NVARCHAR(4000),
    [reported.reference.type] VARCHAR(256),
    [reported.reference.identifier.id] NVARCHAR(100),
    [reported.reference.identifier.extension] NVARCHAR(MAX),
    [reported.reference.identifier.use] NVARCHAR(64),
    [reported.reference.identifier.type] NVARCHAR(MAX),
    [reported.reference.identifier.system] VARCHAR(256),
    [reported.reference.identifier.value] NVARCHAR(4000),
    [reported.reference.identifier.period] NVARCHAR(MAX),
    [reported.reference.identifier.assigner] NVARCHAR(MAX),
    [reported.reference.display] NVARCHAR(4000),
) WITH (
    LOCATION='/NutritionIntake/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.NutritionIntakeIdentifier AS
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
        BULK 'NutritionIntake/**',
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

CREATE VIEW fhir.NutritionIntakeInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'NutritionIntake/**',
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

CREATE VIEW fhir.NutritionIntakeInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'NutritionIntake/**',
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

CREATE VIEW fhir.NutritionIntakeBasedOn AS
SELECT
    [id],
    [basedOn.JSON],
    [basedOn.id],
    [basedOn.extension],
    [basedOn.reference],
    [basedOn.type],
    [basedOn.identifier.id],
    [basedOn.identifier.extension],
    [basedOn.identifier.use],
    [basedOn.identifier.type],
    [basedOn.identifier.system],
    [basedOn.identifier.value],
    [basedOn.identifier.period],
    [basedOn.identifier.assigner],
    [basedOn.display]
FROM openrowset (
        BULK 'NutritionIntake/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [basedOn.JSON]  VARCHAR(MAX) '$.basedOn'
    ) AS rowset
    CROSS APPLY openjson (rowset.[basedOn.JSON]) with (
        [basedOn.id]                   NVARCHAR(100)       '$.id',
        [basedOn.extension]            NVARCHAR(MAX)       '$.extension',
        [basedOn.reference]            NVARCHAR(4000)      '$.reference',
        [basedOn.type]                 VARCHAR(256)        '$.type',
        [basedOn.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [basedOn.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [basedOn.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [basedOn.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [basedOn.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [basedOn.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [basedOn.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [basedOn.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [basedOn.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.NutritionIntakePartOf AS
SELECT
    [id],
    [partOf.JSON],
    [partOf.id],
    [partOf.extension],
    [partOf.reference],
    [partOf.type],
    [partOf.identifier.id],
    [partOf.identifier.extension],
    [partOf.identifier.use],
    [partOf.identifier.type],
    [partOf.identifier.system],
    [partOf.identifier.value],
    [partOf.identifier.period],
    [partOf.identifier.assigner],
    [partOf.display]
FROM openrowset (
        BULK 'NutritionIntake/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [partOf.JSON]  VARCHAR(MAX) '$.partOf'
    ) AS rowset
    CROSS APPLY openjson (rowset.[partOf.JSON]) with (
        [partOf.id]                    NVARCHAR(100)       '$.id',
        [partOf.extension]             NVARCHAR(MAX)       '$.extension',
        [partOf.reference]             NVARCHAR(4000)      '$.reference',
        [partOf.type]                  VARCHAR(256)        '$.type',
        [partOf.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [partOf.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [partOf.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [partOf.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [partOf.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [partOf.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [partOf.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [partOf.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [partOf.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.NutritionIntakeStatusReason AS
SELECT
    [id],
    [statusReason.JSON],
    [statusReason.id],
    [statusReason.extension],
    [statusReason.coding],
    [statusReason.text]
FROM openrowset (
        BULK 'NutritionIntake/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [statusReason.JSON]  VARCHAR(MAX) '$.statusReason'
    ) AS rowset
    CROSS APPLY openjson (rowset.[statusReason.JSON]) with (
        [statusReason.id]              NVARCHAR(100)       '$.id',
        [statusReason.extension]       NVARCHAR(MAX)       '$.extension',
        [statusReason.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [statusReason.text]            NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.NutritionIntakeConsumedItem AS
SELECT
    [id],
    [consumedItem.JSON],
    [consumedItem.id],
    [consumedItem.extension],
    [consumedItem.modifierExtension],
    [consumedItem.type.id],
    [consumedItem.type.extension],
    [consumedItem.type.coding],
    [consumedItem.type.text],
    [consumedItem.nutritionProduct.id],
    [consumedItem.nutritionProduct.extension],
    [consumedItem.nutritionProduct.concept],
    [consumedItem.nutritionProduct.reference],
    [consumedItem.schedule.id],
    [consumedItem.schedule.extension],
    [consumedItem.schedule.modifierExtension],
    [consumedItem.schedule.event],
    [consumedItem.schedule.repeat],
    [consumedItem.schedule.code],
    [consumedItem.amount.id],
    [consumedItem.amount.extension],
    [consumedItem.amount.value],
    [consumedItem.amount.comparator],
    [consumedItem.amount.unit],
    [consumedItem.amount.system],
    [consumedItem.amount.code],
    [consumedItem.rate.id],
    [consumedItem.rate.extension],
    [consumedItem.rate.value],
    [consumedItem.rate.comparator],
    [consumedItem.rate.unit],
    [consumedItem.rate.system],
    [consumedItem.rate.code],
    [consumedItem.notConsumed],
    [consumedItem.notConsumedReason.id],
    [consumedItem.notConsumedReason.extension],
    [consumedItem.notConsumedReason.coding],
    [consumedItem.notConsumedReason.text]
FROM openrowset (
        BULK 'NutritionIntake/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [consumedItem.JSON]  VARCHAR(MAX) '$.consumedItem'
    ) AS rowset
    CROSS APPLY openjson (rowset.[consumedItem.JSON]) with (
        [consumedItem.id]              NVARCHAR(100)       '$.id',
        [consumedItem.extension]       NVARCHAR(MAX)       '$.extension',
        [consumedItem.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [consumedItem.type.id]         NVARCHAR(100)       '$.type.id',
        [consumedItem.type.extension]  NVARCHAR(MAX)       '$.type.extension',
        [consumedItem.type.coding]     NVARCHAR(MAX)       '$.type.coding',
        [consumedItem.type.text]       NVARCHAR(4000)      '$.type.text',
        [consumedItem.nutritionProduct.id] NVARCHAR(100)       '$.nutritionProduct.id',
        [consumedItem.nutritionProduct.extension] NVARCHAR(MAX)       '$.nutritionProduct.extension',
        [consumedItem.nutritionProduct.concept] NVARCHAR(MAX)       '$.nutritionProduct.concept',
        [consumedItem.nutritionProduct.reference] NVARCHAR(MAX)       '$.nutritionProduct.reference',
        [consumedItem.schedule.id]     NVARCHAR(100)       '$.schedule.id',
        [consumedItem.schedule.extension] NVARCHAR(MAX)       '$.schedule.extension',
        [consumedItem.schedule.modifierExtension] NVARCHAR(MAX)       '$.schedule.modifierExtension',
        [consumedItem.schedule.event]  NVARCHAR(MAX)       '$.schedule.event',
        [consumedItem.schedule.repeat] NVARCHAR(MAX)       '$.schedule.repeat',
        [consumedItem.schedule.code]   NVARCHAR(MAX)       '$.schedule.code',
        [consumedItem.amount.id]       NVARCHAR(100)       '$.amount.id',
        [consumedItem.amount.extension] NVARCHAR(MAX)       '$.amount.extension',
        [consumedItem.amount.value]    float               '$.amount.value',
        [consumedItem.amount.comparator] NVARCHAR(64)        '$.amount.comparator',
        [consumedItem.amount.unit]     NVARCHAR(100)       '$.amount.unit',
        [consumedItem.amount.system]   VARCHAR(256)        '$.amount.system',
        [consumedItem.amount.code]     NVARCHAR(4000)      '$.amount.code',
        [consumedItem.rate.id]         NVARCHAR(100)       '$.rate.id',
        [consumedItem.rate.extension]  NVARCHAR(MAX)       '$.rate.extension',
        [consumedItem.rate.value]      float               '$.rate.value',
        [consumedItem.rate.comparator] NVARCHAR(64)        '$.rate.comparator',
        [consumedItem.rate.unit]       NVARCHAR(100)       '$.rate.unit',
        [consumedItem.rate.system]     VARCHAR(256)        '$.rate.system',
        [consumedItem.rate.code]       NVARCHAR(4000)      '$.rate.code',
        [consumedItem.notConsumed]     bit                 '$.notConsumed',
        [consumedItem.notConsumedReason.id] NVARCHAR(100)       '$.notConsumedReason.id',
        [consumedItem.notConsumedReason.extension] NVARCHAR(MAX)       '$.notConsumedReason.extension',
        [consumedItem.notConsumedReason.coding] NVARCHAR(MAX)       '$.notConsumedReason.coding',
        [consumedItem.notConsumedReason.text] NVARCHAR(4000)      '$.notConsumedReason.text'
    ) j

GO

CREATE VIEW fhir.NutritionIntakeIngredientLabel AS
SELECT
    [id],
    [ingredientLabel.JSON],
    [ingredientLabel.id],
    [ingredientLabel.extension],
    [ingredientLabel.modifierExtension],
    [ingredientLabel.nutrient.id],
    [ingredientLabel.nutrient.extension],
    [ingredientLabel.nutrient.concept],
    [ingredientLabel.nutrient.reference],
    [ingredientLabel.amount.id],
    [ingredientLabel.amount.extension],
    [ingredientLabel.amount.value],
    [ingredientLabel.amount.comparator],
    [ingredientLabel.amount.unit],
    [ingredientLabel.amount.system],
    [ingredientLabel.amount.code]
FROM openrowset (
        BULK 'NutritionIntake/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [ingredientLabel.JSON]  VARCHAR(MAX) '$.ingredientLabel'
    ) AS rowset
    CROSS APPLY openjson (rowset.[ingredientLabel.JSON]) with (
        [ingredientLabel.id]           NVARCHAR(100)       '$.id',
        [ingredientLabel.extension]    NVARCHAR(MAX)       '$.extension',
        [ingredientLabel.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [ingredientLabel.nutrient.id]  NVARCHAR(100)       '$.nutrient.id',
        [ingredientLabel.nutrient.extension] NVARCHAR(MAX)       '$.nutrient.extension',
        [ingredientLabel.nutrient.concept] NVARCHAR(MAX)       '$.nutrient.concept',
        [ingredientLabel.nutrient.reference] NVARCHAR(MAX)       '$.nutrient.reference',
        [ingredientLabel.amount.id]    NVARCHAR(100)       '$.amount.id',
        [ingredientLabel.amount.extension] NVARCHAR(MAX)       '$.amount.extension',
        [ingredientLabel.amount.value] float               '$.amount.value',
        [ingredientLabel.amount.comparator] NVARCHAR(64)        '$.amount.comparator',
        [ingredientLabel.amount.unit]  NVARCHAR(100)       '$.amount.unit',
        [ingredientLabel.amount.system] VARCHAR(256)        '$.amount.system',
        [ingredientLabel.amount.code]  NVARCHAR(4000)      '$.amount.code'
    ) j

GO

CREATE VIEW fhir.NutritionIntakePerformer AS
SELECT
    [id],
    [performer.JSON],
    [performer.id],
    [performer.extension],
    [performer.modifierExtension],
    [performer.function.id],
    [performer.function.extension],
    [performer.function.coding],
    [performer.function.text],
    [performer.actor.id],
    [performer.actor.extension],
    [performer.actor.reference],
    [performer.actor.type],
    [performer.actor.identifier],
    [performer.actor.display]
FROM openrowset (
        BULK 'NutritionIntake/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [performer.JSON]  VARCHAR(MAX) '$.performer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[performer.JSON]) with (
        [performer.id]                 NVARCHAR(100)       '$.id',
        [performer.extension]          NVARCHAR(MAX)       '$.extension',
        [performer.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [performer.function.id]        NVARCHAR(100)       '$.function.id',
        [performer.function.extension] NVARCHAR(MAX)       '$.function.extension',
        [performer.function.coding]    NVARCHAR(MAX)       '$.function.coding',
        [performer.function.text]      NVARCHAR(4000)      '$.function.text',
        [performer.actor.id]           NVARCHAR(100)       '$.actor.id',
        [performer.actor.extension]    NVARCHAR(MAX)       '$.actor.extension',
        [performer.actor.reference]    NVARCHAR(4000)      '$.actor.reference',
        [performer.actor.type]         VARCHAR(256)        '$.actor.type',
        [performer.actor.identifier]   NVARCHAR(MAX)       '$.actor.identifier',
        [performer.actor.display]      NVARCHAR(4000)      '$.actor.display'
    ) j

GO

CREATE VIEW fhir.NutritionIntakeDerivedFrom AS
SELECT
    [id],
    [derivedFrom.JSON],
    [derivedFrom.id],
    [derivedFrom.extension],
    [derivedFrom.reference],
    [derivedFrom.type],
    [derivedFrom.identifier.id],
    [derivedFrom.identifier.extension],
    [derivedFrom.identifier.use],
    [derivedFrom.identifier.type],
    [derivedFrom.identifier.system],
    [derivedFrom.identifier.value],
    [derivedFrom.identifier.period],
    [derivedFrom.identifier.assigner],
    [derivedFrom.display]
FROM openrowset (
        BULK 'NutritionIntake/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [derivedFrom.JSON]  VARCHAR(MAX) '$.derivedFrom'
    ) AS rowset
    CROSS APPLY openjson (rowset.[derivedFrom.JSON]) with (
        [derivedFrom.id]               NVARCHAR(100)       '$.id',
        [derivedFrom.extension]        NVARCHAR(MAX)       '$.extension',
        [derivedFrom.reference]        NVARCHAR(4000)      '$.reference',
        [derivedFrom.type]             VARCHAR(256)        '$.type',
        [derivedFrom.identifier.id]    NVARCHAR(100)       '$.identifier.id',
        [derivedFrom.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [derivedFrom.identifier.use]   NVARCHAR(64)        '$.identifier.use',
        [derivedFrom.identifier.type]  NVARCHAR(MAX)       '$.identifier.type',
        [derivedFrom.identifier.system] VARCHAR(256)        '$.identifier.system',
        [derivedFrom.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [derivedFrom.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [derivedFrom.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [derivedFrom.display]          NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.NutritionIntakeReason AS
SELECT
    [id],
    [reason.JSON],
    [reason.id],
    [reason.extension],
    [reason.concept.id],
    [reason.concept.extension],
    [reason.concept.coding],
    [reason.concept.text],
    [reason.reference.id],
    [reason.reference.extension],
    [reason.reference.reference],
    [reason.reference.type],
    [reason.reference.identifier],
    [reason.reference.display]
FROM openrowset (
        BULK 'NutritionIntake/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reason.JSON]  VARCHAR(MAX) '$.reason'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reason.JSON]) with (
        [reason.id]                    NVARCHAR(100)       '$.id',
        [reason.extension]             NVARCHAR(MAX)       '$.extension',
        [reason.concept.id]            NVARCHAR(100)       '$.concept.id',
        [reason.concept.extension]     NVARCHAR(MAX)       '$.concept.extension',
        [reason.concept.coding]        NVARCHAR(MAX)       '$.concept.coding',
        [reason.concept.text]          NVARCHAR(4000)      '$.concept.text',
        [reason.reference.id]          NVARCHAR(100)       '$.reference.id',
        [reason.reference.extension]   NVARCHAR(MAX)       '$.reference.extension',
        [reason.reference.reference]   NVARCHAR(4000)      '$.reference.reference',
        [reason.reference.type]        VARCHAR(256)        '$.reference.type',
        [reason.reference.identifier]  NVARCHAR(MAX)       '$.reference.identifier',
        [reason.reference.display]     NVARCHAR(4000)      '$.reference.display'
    ) j

GO

CREATE VIEW fhir.NutritionIntakeNote AS
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
        BULK 'NutritionIntake/**',
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
