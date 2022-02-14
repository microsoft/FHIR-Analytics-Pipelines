CREATE EXTERNAL TABLE [fhir].[MedicationKnowledge] (
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
    [doseForm.id] NVARCHAR(100),
    [doseForm.extension] NVARCHAR(MAX),
    [doseForm.coding] VARCHAR(MAX),
    [doseForm.text] NVARCHAR(4000),
    [amount.id] NVARCHAR(100),
    [amount.extension] NVARCHAR(MAX),
    [amount.value] float,
    [amount.comparator] NVARCHAR(64),
    [amount.unit] NVARCHAR(100),
    [amount.system] VARCHAR(256),
    [amount.code] NVARCHAR(4000),
    [synonym] VARCHAR(MAX),
    [relatedMedicationKnowledge] VARCHAR(MAX),
    [associatedMedication] VARCHAR(MAX),
    [productType] VARCHAR(MAX),
    [monograph] VARCHAR(MAX),
    [ingredient] VARCHAR(MAX),
    [preparationInstruction] NVARCHAR(MAX),
    [intendedRoute] VARCHAR(MAX),
    [cost] VARCHAR(MAX),
    [monitoringProgram] VARCHAR(MAX),
    [administrationGuidelines] VARCHAR(MAX),
    [medicineClassification] VARCHAR(MAX),
    [packaging.id] NVARCHAR(100),
    [packaging.extension] NVARCHAR(MAX),
    [packaging.modifierExtension] NVARCHAR(MAX),
    [packaging.type.id] NVARCHAR(100),
    [packaging.type.extension] NVARCHAR(MAX),
    [packaging.type.coding] NVARCHAR(MAX),
    [packaging.type.text] NVARCHAR(4000),
    [packaging.quantity.id] NVARCHAR(100),
    [packaging.quantity.extension] NVARCHAR(MAX),
    [packaging.quantity.value] float,
    [packaging.quantity.comparator] NVARCHAR(64),
    [packaging.quantity.unit] NVARCHAR(100),
    [packaging.quantity.system] VARCHAR(256),
    [packaging.quantity.code] NVARCHAR(4000),
    [drugCharacteristic] VARCHAR(MAX),
    [contraindication] VARCHAR(MAX),
    [regulatory] VARCHAR(MAX),
    [kinetics] VARCHAR(MAX),
) WITH (
    LOCATION='/MedicationKnowledge/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicationKnowledgeSynonym AS
SELECT
    [id],
    [synonym.JSON],
    [synonym]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [synonym.JSON]  VARCHAR(MAX) '$.synonym'
    ) AS rowset
    CROSS APPLY openjson (rowset.[synonym.JSON]) with (
        [synonym]                      NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeRelatedMedicationKnowledge AS
SELECT
    [id],
    [relatedMedicationKnowledge.JSON],
    [relatedMedicationKnowledge.id],
    [relatedMedicationKnowledge.extension],
    [relatedMedicationKnowledge.modifierExtension],
    [relatedMedicationKnowledge.type.id],
    [relatedMedicationKnowledge.type.extension],
    [relatedMedicationKnowledge.type.coding],
    [relatedMedicationKnowledge.type.text],
    [relatedMedicationKnowledge.reference]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [relatedMedicationKnowledge.JSON]  VARCHAR(MAX) '$.relatedMedicationKnowledge'
    ) AS rowset
    CROSS APPLY openjson (rowset.[relatedMedicationKnowledge.JSON]) with (
        [relatedMedicationKnowledge.id] NVARCHAR(100)       '$.id',
        [relatedMedicationKnowledge.extension] NVARCHAR(MAX)       '$.extension',
        [relatedMedicationKnowledge.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [relatedMedicationKnowledge.type.id] NVARCHAR(100)       '$.type.id',
        [relatedMedicationKnowledge.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [relatedMedicationKnowledge.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [relatedMedicationKnowledge.type.text] NVARCHAR(4000)      '$.type.text',
        [relatedMedicationKnowledge.reference] NVARCHAR(MAX)       '$.reference' AS JSON
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeAssociatedMedication AS
SELECT
    [id],
    [associatedMedication.JSON],
    [associatedMedication.id],
    [associatedMedication.extension],
    [associatedMedication.reference],
    [associatedMedication.type],
    [associatedMedication.identifier.id],
    [associatedMedication.identifier.extension],
    [associatedMedication.identifier.use],
    [associatedMedication.identifier.type],
    [associatedMedication.identifier.system],
    [associatedMedication.identifier.value],
    [associatedMedication.identifier.period],
    [associatedMedication.identifier.assigner],
    [associatedMedication.display]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [associatedMedication.JSON]  VARCHAR(MAX) '$.associatedMedication'
    ) AS rowset
    CROSS APPLY openjson (rowset.[associatedMedication.JSON]) with (
        [associatedMedication.id]      NVARCHAR(100)       '$.id',
        [associatedMedication.extension] NVARCHAR(MAX)       '$.extension',
        [associatedMedication.reference] NVARCHAR(4000)      '$.reference',
        [associatedMedication.type]    VARCHAR(256)        '$.type',
        [associatedMedication.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [associatedMedication.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [associatedMedication.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [associatedMedication.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [associatedMedication.identifier.system] VARCHAR(256)        '$.identifier.system',
        [associatedMedication.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [associatedMedication.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [associatedMedication.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [associatedMedication.display] NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeProductType AS
SELECT
    [id],
    [productType.JSON],
    [productType.id],
    [productType.extension],
    [productType.coding],
    [productType.text]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [productType.JSON]  VARCHAR(MAX) '$.productType'
    ) AS rowset
    CROSS APPLY openjson (rowset.[productType.JSON]) with (
        [productType.id]               NVARCHAR(100)       '$.id',
        [productType.extension]        NVARCHAR(MAX)       '$.extension',
        [productType.coding]           NVARCHAR(MAX)       '$.coding' AS JSON,
        [productType.text]             NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeMonograph AS
SELECT
    [id],
    [monograph.JSON],
    [monograph.id],
    [monograph.extension],
    [monograph.modifierExtension],
    [monograph.type.id],
    [monograph.type.extension],
    [monograph.type.coding],
    [monograph.type.text],
    [monograph.source.id],
    [monograph.source.extension],
    [monograph.source.reference],
    [monograph.source.type],
    [monograph.source.identifier],
    [monograph.source.display]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [monograph.JSON]  VARCHAR(MAX) '$.monograph'
    ) AS rowset
    CROSS APPLY openjson (rowset.[monograph.JSON]) with (
        [monograph.id]                 NVARCHAR(100)       '$.id',
        [monograph.extension]          NVARCHAR(MAX)       '$.extension',
        [monograph.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [monograph.type.id]            NVARCHAR(100)       '$.type.id',
        [monograph.type.extension]     NVARCHAR(MAX)       '$.type.extension',
        [monograph.type.coding]        NVARCHAR(MAX)       '$.type.coding',
        [monograph.type.text]          NVARCHAR(4000)      '$.type.text',
        [monograph.source.id]          NVARCHAR(100)       '$.source.id',
        [monograph.source.extension]   NVARCHAR(MAX)       '$.source.extension',
        [monograph.source.reference]   NVARCHAR(4000)      '$.source.reference',
        [monograph.source.type]        VARCHAR(256)        '$.source.type',
        [monograph.source.identifier]  NVARCHAR(MAX)       '$.source.identifier',
        [monograph.source.display]     NVARCHAR(4000)      '$.source.display'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeIngredient AS
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
        BULK 'MedicationKnowledge/**',
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

GO

CREATE VIEW fhir.MedicationKnowledgeIntendedRoute AS
SELECT
    [id],
    [intendedRoute.JSON],
    [intendedRoute.id],
    [intendedRoute.extension],
    [intendedRoute.coding],
    [intendedRoute.text]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [intendedRoute.JSON]  VARCHAR(MAX) '$.intendedRoute'
    ) AS rowset
    CROSS APPLY openjson (rowset.[intendedRoute.JSON]) with (
        [intendedRoute.id]             NVARCHAR(100)       '$.id',
        [intendedRoute.extension]      NVARCHAR(MAX)       '$.extension',
        [intendedRoute.coding]         NVARCHAR(MAX)       '$.coding' AS JSON,
        [intendedRoute.text]           NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeCost AS
SELECT
    [id],
    [cost.JSON],
    [cost.id],
    [cost.extension],
    [cost.modifierExtension],
    [cost.type.id],
    [cost.type.extension],
    [cost.type.coding],
    [cost.type.text],
    [cost.source],
    [cost.cost.id],
    [cost.cost.extension],
    [cost.cost.value],
    [cost.cost.currency]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [cost.JSON]  VARCHAR(MAX) '$.cost'
    ) AS rowset
    CROSS APPLY openjson (rowset.[cost.JSON]) with (
        [cost.id]                      NVARCHAR(100)       '$.id',
        [cost.extension]               NVARCHAR(MAX)       '$.extension',
        [cost.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [cost.type.id]                 NVARCHAR(100)       '$.type.id',
        [cost.type.extension]          NVARCHAR(MAX)       '$.type.extension',
        [cost.type.coding]             NVARCHAR(MAX)       '$.type.coding',
        [cost.type.text]               NVARCHAR(4000)      '$.type.text',
        [cost.source]                  NVARCHAR(4000)      '$.source',
        [cost.cost.id]                 NVARCHAR(100)       '$.cost.id',
        [cost.cost.extension]          NVARCHAR(MAX)       '$.cost.extension',
        [cost.cost.value]              float               '$.cost.value',
        [cost.cost.currency]           NVARCHAR(100)       '$.cost.currency'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeMonitoringProgram AS
SELECT
    [id],
    [monitoringProgram.JSON],
    [monitoringProgram.id],
    [monitoringProgram.extension],
    [monitoringProgram.modifierExtension],
    [monitoringProgram.type.id],
    [monitoringProgram.type.extension],
    [monitoringProgram.type.coding],
    [monitoringProgram.type.text],
    [monitoringProgram.name]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [monitoringProgram.JSON]  VARCHAR(MAX) '$.monitoringProgram'
    ) AS rowset
    CROSS APPLY openjson (rowset.[monitoringProgram.JSON]) with (
        [monitoringProgram.id]         NVARCHAR(100)       '$.id',
        [monitoringProgram.extension]  NVARCHAR(MAX)       '$.extension',
        [monitoringProgram.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [monitoringProgram.type.id]    NVARCHAR(100)       '$.type.id',
        [monitoringProgram.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [monitoringProgram.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [monitoringProgram.type.text]  NVARCHAR(4000)      '$.type.text',
        [monitoringProgram.name]       NVARCHAR(500)       '$.name'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeAdministrationGuidelines AS
SELECT
    [id],
    [administrationGuidelines.JSON],
    [administrationGuidelines.id],
    [administrationGuidelines.extension],
    [administrationGuidelines.modifierExtension],
    [administrationGuidelines.dosage],
    [administrationGuidelines.patientCharacteristics],
    [administrationGuidelines.indication.codeableConcept.id],
    [administrationGuidelines.indication.codeableConcept.extension],
    [administrationGuidelines.indication.codeableConcept.coding],
    [administrationGuidelines.indication.codeableConcept.text],
    [administrationGuidelines.indication.reference.id],
    [administrationGuidelines.indication.reference.extension],
    [administrationGuidelines.indication.reference.reference],
    [administrationGuidelines.indication.reference.type],
    [administrationGuidelines.indication.reference.identifier],
    [administrationGuidelines.indication.reference.display]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [administrationGuidelines.JSON]  VARCHAR(MAX) '$.administrationGuidelines'
    ) AS rowset
    CROSS APPLY openjson (rowset.[administrationGuidelines.JSON]) with (
        [administrationGuidelines.id]  NVARCHAR(100)       '$.id',
        [administrationGuidelines.extension] NVARCHAR(MAX)       '$.extension',
        [administrationGuidelines.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [administrationGuidelines.dosage] NVARCHAR(MAX)       '$.dosage' AS JSON,
        [administrationGuidelines.patientCharacteristics] NVARCHAR(MAX)       '$.patientCharacteristics' AS JSON,
        [administrationGuidelines.indication.codeableConcept.id] NVARCHAR(100)       '$.indication.codeableConcept.id',
        [administrationGuidelines.indication.codeableConcept.extension] NVARCHAR(MAX)       '$.indication.codeableConcept.extension',
        [administrationGuidelines.indication.codeableConcept.coding] NVARCHAR(MAX)       '$.indication.codeableConcept.coding',
        [administrationGuidelines.indication.codeableConcept.text] NVARCHAR(4000)      '$.indication.codeableConcept.text',
        [administrationGuidelines.indication.reference.id] NVARCHAR(100)       '$.indication.reference.id',
        [administrationGuidelines.indication.reference.extension] NVARCHAR(MAX)       '$.indication.reference.extension',
        [administrationGuidelines.indication.reference.reference] NVARCHAR(4000)      '$.indication.reference.reference',
        [administrationGuidelines.indication.reference.type] VARCHAR(256)        '$.indication.reference.type',
        [administrationGuidelines.indication.reference.identifier] NVARCHAR(MAX)       '$.indication.reference.identifier',
        [administrationGuidelines.indication.reference.display] NVARCHAR(4000)      '$.indication.reference.display'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeMedicineClassification AS
SELECT
    [id],
    [medicineClassification.JSON],
    [medicineClassification.id],
    [medicineClassification.extension],
    [medicineClassification.modifierExtension],
    [medicineClassification.type.id],
    [medicineClassification.type.extension],
    [medicineClassification.type.coding],
    [medicineClassification.type.text],
    [medicineClassification.classification]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [medicineClassification.JSON]  VARCHAR(MAX) '$.medicineClassification'
    ) AS rowset
    CROSS APPLY openjson (rowset.[medicineClassification.JSON]) with (
        [medicineClassification.id]    NVARCHAR(100)       '$.id',
        [medicineClassification.extension] NVARCHAR(MAX)       '$.extension',
        [medicineClassification.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [medicineClassification.type.id] NVARCHAR(100)       '$.type.id',
        [medicineClassification.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [medicineClassification.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [medicineClassification.type.text] NVARCHAR(4000)      '$.type.text',
        [medicineClassification.classification] NVARCHAR(MAX)       '$.classification' AS JSON
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeDrugCharacteristic AS
SELECT
    [id],
    [drugCharacteristic.JSON],
    [drugCharacteristic.id],
    [drugCharacteristic.extension],
    [drugCharacteristic.modifierExtension],
    [drugCharacteristic.type.id],
    [drugCharacteristic.type.extension],
    [drugCharacteristic.type.coding],
    [drugCharacteristic.type.text],
    [drugCharacteristic.valueQuantity.id],
    [drugCharacteristic.valueQuantity.extension],
    [drugCharacteristic.valueQuantity.value],
    [drugCharacteristic.valueQuantity.comparator],
    [drugCharacteristic.valueQuantity.unit],
    [drugCharacteristic.valueQuantity.system],
    [drugCharacteristic.valueQuantity.code],
    [drugCharacteristic.value.codeableConcept.id],
    [drugCharacteristic.value.codeableConcept.extension],
    [drugCharacteristic.value.codeableConcept.coding],
    [drugCharacteristic.value.codeableConcept.text],
    [drugCharacteristic.value.string],
    [drugCharacteristic.value.quantity.id],
    [drugCharacteristic.value.quantity.extension],
    [drugCharacteristic.value.quantity.value],
    [drugCharacteristic.value.quantity.comparator],
    [drugCharacteristic.value.quantity.unit],
    [drugCharacteristic.value.quantity.system],
    [drugCharacteristic.value.quantity.code],
    [drugCharacteristic.value.base64Binary]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [drugCharacteristic.JSON]  VARCHAR(MAX) '$.drugCharacteristic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[drugCharacteristic.JSON]) with (
        [drugCharacteristic.id]        NVARCHAR(100)       '$.id',
        [drugCharacteristic.extension] NVARCHAR(MAX)       '$.extension',
        [drugCharacteristic.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [drugCharacteristic.type.id]   NVARCHAR(100)       '$.type.id',
        [drugCharacteristic.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [drugCharacteristic.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [drugCharacteristic.type.text] NVARCHAR(4000)      '$.type.text',
        [drugCharacteristic.valueQuantity.id] NVARCHAR(100)       '$.valueQuantity.id',
        [drugCharacteristic.valueQuantity.extension] NVARCHAR(MAX)       '$.valueQuantity.extension',
        [drugCharacteristic.valueQuantity.value] float               '$.valueQuantity.value',
        [drugCharacteristic.valueQuantity.comparator] NVARCHAR(64)        '$.valueQuantity.comparator',
        [drugCharacteristic.valueQuantity.unit] NVARCHAR(100)       '$.valueQuantity.unit',
        [drugCharacteristic.valueQuantity.system] VARCHAR(256)        '$.valueQuantity.system',
        [drugCharacteristic.valueQuantity.code] NVARCHAR(4000)      '$.valueQuantity.code',
        [drugCharacteristic.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [drugCharacteristic.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [drugCharacteristic.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [drugCharacteristic.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [drugCharacteristic.value.string] NVARCHAR(4000)      '$.value.string',
        [drugCharacteristic.value.quantity.id] NVARCHAR(100)       '$.value.quantity.id',
        [drugCharacteristic.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [drugCharacteristic.value.quantity.value] float               '$.value.quantity.value',
        [drugCharacteristic.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [drugCharacteristic.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [drugCharacteristic.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [drugCharacteristic.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [drugCharacteristic.value.base64Binary] NVARCHAR(MAX)       '$.value.base64Binary'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeContraindication AS
SELECT
    [id],
    [contraindication.JSON],
    [contraindication.id],
    [contraindication.extension],
    [contraindication.reference],
    [contraindication.type],
    [contraindication.identifier.id],
    [contraindication.identifier.extension],
    [contraindication.identifier.use],
    [contraindication.identifier.type],
    [contraindication.identifier.system],
    [contraindication.identifier.value],
    [contraindication.identifier.period],
    [contraindication.identifier.assigner],
    [contraindication.display]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contraindication.JSON]  VARCHAR(MAX) '$.contraindication'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contraindication.JSON]) with (
        [contraindication.id]          NVARCHAR(100)       '$.id',
        [contraindication.extension]   NVARCHAR(MAX)       '$.extension',
        [contraindication.reference]   NVARCHAR(4000)      '$.reference',
        [contraindication.type]        VARCHAR(256)        '$.type',
        [contraindication.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [contraindication.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [contraindication.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [contraindication.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [contraindication.identifier.system] VARCHAR(256)        '$.identifier.system',
        [contraindication.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [contraindication.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [contraindication.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [contraindication.display]     NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeRegulatory AS
SELECT
    [id],
    [regulatory.JSON],
    [regulatory.id],
    [regulatory.extension],
    [regulatory.modifierExtension],
    [regulatory.regulatoryAuthority.id],
    [regulatory.regulatoryAuthority.extension],
    [regulatory.regulatoryAuthority.reference],
    [regulatory.regulatoryAuthority.type],
    [regulatory.regulatoryAuthority.identifier],
    [regulatory.regulatoryAuthority.display],
    [regulatory.substitution],
    [regulatory.schedule],
    [regulatory.maxDispense.id],
    [regulatory.maxDispense.extension],
    [regulatory.maxDispense.modifierExtension],
    [regulatory.maxDispense.quantity],
    [regulatory.maxDispense.period]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [regulatory.JSON]  VARCHAR(MAX) '$.regulatory'
    ) AS rowset
    CROSS APPLY openjson (rowset.[regulatory.JSON]) with (
        [regulatory.id]                NVARCHAR(100)       '$.id',
        [regulatory.extension]         NVARCHAR(MAX)       '$.extension',
        [regulatory.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [regulatory.regulatoryAuthority.id] NVARCHAR(100)       '$.regulatoryAuthority.id',
        [regulatory.regulatoryAuthority.extension] NVARCHAR(MAX)       '$.regulatoryAuthority.extension',
        [regulatory.regulatoryAuthority.reference] NVARCHAR(4000)      '$.regulatoryAuthority.reference',
        [regulatory.regulatoryAuthority.type] VARCHAR(256)        '$.regulatoryAuthority.type',
        [regulatory.regulatoryAuthority.identifier] NVARCHAR(MAX)       '$.regulatoryAuthority.identifier',
        [regulatory.regulatoryAuthority.display] NVARCHAR(4000)      '$.regulatoryAuthority.display',
        [regulatory.substitution]      NVARCHAR(MAX)       '$.substitution' AS JSON,
        [regulatory.schedule]          NVARCHAR(MAX)       '$.schedule' AS JSON,
        [regulatory.maxDispense.id]    NVARCHAR(100)       '$.maxDispense.id',
        [regulatory.maxDispense.extension] NVARCHAR(MAX)       '$.maxDispense.extension',
        [regulatory.maxDispense.modifierExtension] NVARCHAR(MAX)       '$.maxDispense.modifierExtension',
        [regulatory.maxDispense.quantity] NVARCHAR(MAX)       '$.maxDispense.quantity',
        [regulatory.maxDispense.period] NVARCHAR(MAX)       '$.maxDispense.period'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeKinetics AS
SELECT
    [id],
    [kinetics.JSON],
    [kinetics.id],
    [kinetics.extension],
    [kinetics.modifierExtension],
    [kinetics.areaUnderCurve],
    [kinetics.lethalDose50],
    [kinetics.halfLifePeriod.id],
    [kinetics.halfLifePeriod.extension],
    [kinetics.halfLifePeriod.value],
    [kinetics.halfLifePeriod.comparator],
    [kinetics.halfLifePeriod.unit],
    [kinetics.halfLifePeriod.system],
    [kinetics.halfLifePeriod.code]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [kinetics.JSON]  VARCHAR(MAX) '$.kinetics'
    ) AS rowset
    CROSS APPLY openjson (rowset.[kinetics.JSON]) with (
        [kinetics.id]                  NVARCHAR(100)       '$.id',
        [kinetics.extension]           NVARCHAR(MAX)       '$.extension',
        [kinetics.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [kinetics.areaUnderCurve]      NVARCHAR(MAX)       '$.areaUnderCurve' AS JSON,
        [kinetics.lethalDose50]        NVARCHAR(MAX)       '$.lethalDose50' AS JSON,
        [kinetics.halfLifePeriod.id]   NVARCHAR(100)       '$.halfLifePeriod.id',
        [kinetics.halfLifePeriod.extension] NVARCHAR(MAX)       '$.halfLifePeriod.extension',
        [kinetics.halfLifePeriod.value] float               '$.halfLifePeriod.value',
        [kinetics.halfLifePeriod.comparator] NVARCHAR(64)        '$.halfLifePeriod.comparator',
        [kinetics.halfLifePeriod.unit] NVARCHAR(100)       '$.halfLifePeriod.unit',
        [kinetics.halfLifePeriod.system] VARCHAR(256)        '$.halfLifePeriod.system',
        [kinetics.halfLifePeriod.code] NVARCHAR(4000)      '$.halfLifePeriod.code'
    ) j
