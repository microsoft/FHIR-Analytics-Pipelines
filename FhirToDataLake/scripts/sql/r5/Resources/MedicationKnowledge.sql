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
    [identifier] VARCHAR(MAX),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [status] NVARCHAR(100),
    [author.id] NVARCHAR(100),
    [author.extension] NVARCHAR(MAX),
    [author.reference] NVARCHAR(4000),
    [author.type] VARCHAR(256),
    [author.identifier.id] NVARCHAR(100),
    [author.identifier.extension] NVARCHAR(MAX),
    [author.identifier.use] NVARCHAR(64),
    [author.identifier.type] NVARCHAR(MAX),
    [author.identifier.system] VARCHAR(256),
    [author.identifier.value] NVARCHAR(4000),
    [author.identifier.period] NVARCHAR(MAX),
    [author.identifier.assigner] NVARCHAR(MAX),
    [author.display] NVARCHAR(4000),
    [intendedJurisdiction] VARCHAR(MAX),
    [name] VARCHAR(MAX),
    [relatedMedicationKnowledge] VARCHAR(MAX),
    [associatedMedication] VARCHAR(MAX),
    [productType] VARCHAR(MAX),
    [monograph] VARCHAR(MAX),
    [preparationInstruction] NVARCHAR(MAX),
    [cost] VARCHAR(MAX),
    [monitoringProgram] VARCHAR(MAX),
    [indicationGuideline] VARCHAR(MAX),
    [medicineClassification] VARCHAR(MAX),
    [packaging] VARCHAR(MAX),
    [clinicalUseIssue] VARCHAR(MAX),
    [regulatory] VARCHAR(MAX),
    [definitional.id] NVARCHAR(100),
    [definitional.extension] NVARCHAR(MAX),
    [definitional.modifierExtension] NVARCHAR(MAX),
    [definitional.definition] VARCHAR(MAX),
    [definitional.doseForm.id] NVARCHAR(100),
    [definitional.doseForm.extension] NVARCHAR(MAX),
    [definitional.doseForm.coding] NVARCHAR(MAX),
    [definitional.doseForm.text] NVARCHAR(4000),
    [definitional.intendedRoute] VARCHAR(MAX),
    [definitional.ingredient] VARCHAR(MAX),
    [definitional.drugCharacteristic] VARCHAR(MAX),
) WITH (
    LOCATION='/MedicationKnowledge/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicationKnowledgeIdentifier AS
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
        BULK 'MedicationKnowledge/**',
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

CREATE VIEW fhir.MedicationKnowledgeIntendedJurisdiction AS
SELECT
    [id],
    [intendedJurisdiction.JSON],
    [intendedJurisdiction.id],
    [intendedJurisdiction.extension],
    [intendedJurisdiction.coding],
    [intendedJurisdiction.text]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [intendedJurisdiction.JSON]  VARCHAR(MAX) '$.intendedJurisdiction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[intendedJurisdiction.JSON]) with (
        [intendedJurisdiction.id]      NVARCHAR(100)       '$.id',
        [intendedJurisdiction.extension] NVARCHAR(MAX)       '$.extension',
        [intendedJurisdiction.coding]  NVARCHAR(MAX)       '$.coding' AS JSON,
        [intendedJurisdiction.text]    NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeName AS
SELECT
    [id],
    [name.JSON],
    [name]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [name.JSON]  VARCHAR(MAX) '$.name'
    ) AS rowset
    CROSS APPLY openjson (rowset.[name.JSON]) with (
        [name]                         NVARCHAR(MAX)       '$'
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

CREATE VIEW fhir.MedicationKnowledgeCost AS
SELECT
    [id],
    [cost.JSON],
    [cost.id],
    [cost.extension],
    [cost.modifierExtension],
    [cost.effectiveDate],
    [cost.type.id],
    [cost.type.extension],
    [cost.type.coding],
    [cost.type.text],
    [cost.source],
    [cost.cost.money.id],
    [cost.cost.money.extension],
    [cost.cost.money.value],
    [cost.cost.money.currency],
    [cost.cost.codeableConcept.id],
    [cost.cost.codeableConcept.extension],
    [cost.cost.codeableConcept.coding],
    [cost.cost.codeableConcept.text]
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
        [cost.effectiveDate]           NVARCHAR(MAX)       '$.effectiveDate' AS JSON,
        [cost.type.id]                 NVARCHAR(100)       '$.type.id',
        [cost.type.extension]          NVARCHAR(MAX)       '$.type.extension',
        [cost.type.coding]             NVARCHAR(MAX)       '$.type.coding',
        [cost.type.text]               NVARCHAR(4000)      '$.type.text',
        [cost.source]                  NVARCHAR(4000)      '$.source',
        [cost.cost.money.id]           NVARCHAR(100)       '$.cost.money.id',
        [cost.cost.money.extension]    NVARCHAR(MAX)       '$.cost.money.extension',
        [cost.cost.money.value]        float               '$.cost.money.value',
        [cost.cost.money.currency]     NVARCHAR(100)       '$.cost.money.currency',
        [cost.cost.codeableConcept.id] NVARCHAR(100)       '$.cost.codeableConcept.id',
        [cost.cost.codeableConcept.extension] NVARCHAR(MAX)       '$.cost.codeableConcept.extension',
        [cost.cost.codeableConcept.coding] NVARCHAR(MAX)       '$.cost.codeableConcept.coding',
        [cost.cost.codeableConcept.text] NVARCHAR(4000)      '$.cost.codeableConcept.text'
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

CREATE VIEW fhir.MedicationKnowledgeIndicationGuideline AS
SELECT
    [id],
    [indicationGuideline.JSON],
    [indicationGuideline.id],
    [indicationGuideline.extension],
    [indicationGuideline.modifierExtension],
    [indicationGuideline.indication],
    [indicationGuideline.dosingGuideline]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [indicationGuideline.JSON]  VARCHAR(MAX) '$.indicationGuideline'
    ) AS rowset
    CROSS APPLY openjson (rowset.[indicationGuideline.JSON]) with (
        [indicationGuideline.id]       NVARCHAR(100)       '$.id',
        [indicationGuideline.extension] NVARCHAR(MAX)       '$.extension',
        [indicationGuideline.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [indicationGuideline.indication] NVARCHAR(MAX)       '$.indication' AS JSON,
        [indicationGuideline.dosingGuideline] NVARCHAR(MAX)       '$.dosingGuideline' AS JSON
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
    [medicineClassification.classification],
    [medicineClassification.source.string],
    [medicineClassification.source.uri]
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
        [medicineClassification.classification] NVARCHAR(MAX)       '$.classification' AS JSON,
        [medicineClassification.source.string] NVARCHAR(4000)      '$.source.string',
        [medicineClassification.source.uri] VARCHAR(256)        '$.source.uri'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgePackaging AS
SELECT
    [id],
    [packaging.JSON],
    [packaging.id],
    [packaging.extension],
    [packaging.modifierExtension],
    [packaging.cost],
    [packaging.packagedProduct.id],
    [packaging.packagedProduct.extension],
    [packaging.packagedProduct.reference],
    [packaging.packagedProduct.type],
    [packaging.packagedProduct.identifier],
    [packaging.packagedProduct.display]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [packaging.JSON]  VARCHAR(MAX) '$.packaging'
    ) AS rowset
    CROSS APPLY openjson (rowset.[packaging.JSON]) with (
        [packaging.id]                 NVARCHAR(100)       '$.id',
        [packaging.extension]          NVARCHAR(MAX)       '$.extension',
        [packaging.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [packaging.cost]               NVARCHAR(MAX)       '$.cost' AS JSON,
        [packaging.packagedProduct.id] NVARCHAR(100)       '$.packagedProduct.id',
        [packaging.packagedProduct.extension] NVARCHAR(MAX)       '$.packagedProduct.extension',
        [packaging.packagedProduct.reference] NVARCHAR(4000)      '$.packagedProduct.reference',
        [packaging.packagedProduct.type] VARCHAR(256)        '$.packagedProduct.type',
        [packaging.packagedProduct.identifier] NVARCHAR(MAX)       '$.packagedProduct.identifier',
        [packaging.packagedProduct.display] NVARCHAR(4000)      '$.packagedProduct.display'
    ) j

GO

CREATE VIEW fhir.MedicationKnowledgeClinicalUseIssue AS
SELECT
    [id],
    [clinicalUseIssue.JSON],
    [clinicalUseIssue.id],
    [clinicalUseIssue.extension],
    [clinicalUseIssue.reference],
    [clinicalUseIssue.type],
    [clinicalUseIssue.identifier.id],
    [clinicalUseIssue.identifier.extension],
    [clinicalUseIssue.identifier.use],
    [clinicalUseIssue.identifier.type],
    [clinicalUseIssue.identifier.system],
    [clinicalUseIssue.identifier.value],
    [clinicalUseIssue.identifier.period],
    [clinicalUseIssue.identifier.assigner],
    [clinicalUseIssue.display]
FROM openrowset (
        BULK 'MedicationKnowledge/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [clinicalUseIssue.JSON]  VARCHAR(MAX) '$.clinicalUseIssue'
    ) AS rowset
    CROSS APPLY openjson (rowset.[clinicalUseIssue.JSON]) with (
        [clinicalUseIssue.id]          NVARCHAR(100)       '$.id',
        [clinicalUseIssue.extension]   NVARCHAR(MAX)       '$.extension',
        [clinicalUseIssue.reference]   NVARCHAR(4000)      '$.reference',
        [clinicalUseIssue.type]        VARCHAR(256)        '$.type',
        [clinicalUseIssue.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [clinicalUseIssue.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [clinicalUseIssue.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [clinicalUseIssue.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [clinicalUseIssue.identifier.system] VARCHAR(256)        '$.identifier.system',
        [clinicalUseIssue.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [clinicalUseIssue.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [clinicalUseIssue.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [clinicalUseIssue.display]     NVARCHAR(4000)      '$.display'
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
