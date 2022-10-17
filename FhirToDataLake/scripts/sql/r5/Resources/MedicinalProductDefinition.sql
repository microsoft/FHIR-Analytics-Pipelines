CREATE EXTERNAL TABLE [fhir].[MedicinalProductDefinition] (
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
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [domain.id] NVARCHAR(100),
    [domain.extension] NVARCHAR(MAX),
    [domain.coding] VARCHAR(MAX),
    [domain.text] NVARCHAR(4000),
    [version] NVARCHAR(100),
    [status.id] NVARCHAR(100),
    [status.extension] NVARCHAR(MAX),
    [status.coding] VARCHAR(MAX),
    [status.text] NVARCHAR(4000),
    [statusDate] VARCHAR(64),
    [description] NVARCHAR(MAX),
    [combinedPharmaceuticalDoseForm.id] NVARCHAR(100),
    [combinedPharmaceuticalDoseForm.extension] NVARCHAR(MAX),
    [combinedPharmaceuticalDoseForm.coding] VARCHAR(MAX),
    [combinedPharmaceuticalDoseForm.text] NVARCHAR(4000),
    [route] VARCHAR(MAX),
    [indication] NVARCHAR(MAX),
    [legalStatusOfSupply.id] NVARCHAR(100),
    [legalStatusOfSupply.extension] NVARCHAR(MAX),
    [legalStatusOfSupply.coding] VARCHAR(MAX),
    [legalStatusOfSupply.text] NVARCHAR(4000),
    [additionalMonitoringIndicator.id] NVARCHAR(100),
    [additionalMonitoringIndicator.extension] NVARCHAR(MAX),
    [additionalMonitoringIndicator.coding] VARCHAR(MAX),
    [additionalMonitoringIndicator.text] NVARCHAR(4000),
    [specialMeasures] VARCHAR(MAX),
    [pediatricUseIndicator.id] NVARCHAR(100),
    [pediatricUseIndicator.extension] NVARCHAR(MAX),
    [pediatricUseIndicator.coding] VARCHAR(MAX),
    [pediatricUseIndicator.text] NVARCHAR(4000),
    [classification] VARCHAR(MAX),
    [marketingStatus] VARCHAR(MAX),
    [packagedMedicinalProduct] VARCHAR(MAX),
    [ingredient] VARCHAR(MAX),
    [impurity] VARCHAR(MAX),
    [attachedDocument] VARCHAR(MAX),
    [masterFile] VARCHAR(MAX),
    [contact] VARCHAR(MAX),
    [clinicalTrial] VARCHAR(MAX),
    [code] VARCHAR(MAX),
    [name] VARCHAR(MAX),
    [crossReference] VARCHAR(MAX),
    [operation] VARCHAR(MAX),
    [characteristic] VARCHAR(MAX),
) WITH (
    LOCATION='/MedicinalProductDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicinalProductDefinitionIdentifier AS
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
        BULK 'MedicinalProductDefinition/**',
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

CREATE VIEW fhir.MedicinalProductDefinitionRoute AS
SELECT
    [id],
    [route.JSON],
    [route.id],
    [route.extension],
    [route.coding],
    [route.text]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [route.JSON]  VARCHAR(MAX) '$.route'
    ) AS rowset
    CROSS APPLY openjson (rowset.[route.JSON]) with (
        [route.id]                     NVARCHAR(100)       '$.id',
        [route.extension]              NVARCHAR(MAX)       '$.extension',
        [route.coding]                 NVARCHAR(MAX)       '$.coding' AS JSON,
        [route.text]                   NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionSpecialMeasures AS
SELECT
    [id],
    [specialMeasures.JSON],
    [specialMeasures.id],
    [specialMeasures.extension],
    [specialMeasures.coding],
    [specialMeasures.text]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specialMeasures.JSON]  VARCHAR(MAX) '$.specialMeasures'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specialMeasures.JSON]) with (
        [specialMeasures.id]           NVARCHAR(100)       '$.id',
        [specialMeasures.extension]    NVARCHAR(MAX)       '$.extension',
        [specialMeasures.coding]       NVARCHAR(MAX)       '$.coding' AS JSON,
        [specialMeasures.text]         NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionClassification AS
SELECT
    [id],
    [classification.JSON],
    [classification.id],
    [classification.extension],
    [classification.coding],
    [classification.text]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [classification.JSON]  VARCHAR(MAX) '$.classification'
    ) AS rowset
    CROSS APPLY openjson (rowset.[classification.JSON]) with (
        [classification.id]            NVARCHAR(100)       '$.id',
        [classification.extension]     NVARCHAR(MAX)       '$.extension',
        [classification.coding]        NVARCHAR(MAX)       '$.coding' AS JSON,
        [classification.text]          NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionMarketingStatus AS
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
        BULK 'MedicinalProductDefinition/**',
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

CREATE VIEW fhir.MedicinalProductDefinitionPackagedMedicinalProduct AS
SELECT
    [id],
    [packagedMedicinalProduct.JSON],
    [packagedMedicinalProduct.id],
    [packagedMedicinalProduct.extension],
    [packagedMedicinalProduct.coding],
    [packagedMedicinalProduct.text]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [packagedMedicinalProduct.JSON]  VARCHAR(MAX) '$.packagedMedicinalProduct'
    ) AS rowset
    CROSS APPLY openjson (rowset.[packagedMedicinalProduct.JSON]) with (
        [packagedMedicinalProduct.id]  NVARCHAR(100)       '$.id',
        [packagedMedicinalProduct.extension] NVARCHAR(MAX)       '$.extension',
        [packagedMedicinalProduct.coding] NVARCHAR(MAX)       '$.coding' AS JSON,
        [packagedMedicinalProduct.text] NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionIngredient AS
SELECT
    [id],
    [ingredient.JSON],
    [ingredient.id],
    [ingredient.extension],
    [ingredient.coding],
    [ingredient.text]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
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

CREATE VIEW fhir.MedicinalProductDefinitionImpurity AS
SELECT
    [id],
    [impurity.JSON],
    [impurity.id],
    [impurity.extension],
    [impurity.concept.id],
    [impurity.concept.extension],
    [impurity.concept.coding],
    [impurity.concept.text],
    [impurity.reference.id],
    [impurity.reference.extension],
    [impurity.reference.reference],
    [impurity.reference.type],
    [impurity.reference.identifier],
    [impurity.reference.display]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [impurity.JSON]  VARCHAR(MAX) '$.impurity'
    ) AS rowset
    CROSS APPLY openjson (rowset.[impurity.JSON]) with (
        [impurity.id]                  NVARCHAR(100)       '$.id',
        [impurity.extension]           NVARCHAR(MAX)       '$.extension',
        [impurity.concept.id]          NVARCHAR(100)       '$.concept.id',
        [impurity.concept.extension]   NVARCHAR(MAX)       '$.concept.extension',
        [impurity.concept.coding]      NVARCHAR(MAX)       '$.concept.coding',
        [impurity.concept.text]        NVARCHAR(4000)      '$.concept.text',
        [impurity.reference.id]        NVARCHAR(100)       '$.reference.id',
        [impurity.reference.extension] NVARCHAR(MAX)       '$.reference.extension',
        [impurity.reference.reference] NVARCHAR(4000)      '$.reference.reference',
        [impurity.reference.type]      VARCHAR(256)        '$.reference.type',
        [impurity.reference.identifier] NVARCHAR(MAX)       '$.reference.identifier',
        [impurity.reference.display]   NVARCHAR(4000)      '$.reference.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionAttachedDocument AS
SELECT
    [id],
    [attachedDocument.JSON],
    [attachedDocument.id],
    [attachedDocument.extension],
    [attachedDocument.reference],
    [attachedDocument.type],
    [attachedDocument.identifier.id],
    [attachedDocument.identifier.extension],
    [attachedDocument.identifier.use],
    [attachedDocument.identifier.type],
    [attachedDocument.identifier.system],
    [attachedDocument.identifier.value],
    [attachedDocument.identifier.period],
    [attachedDocument.identifier.assigner],
    [attachedDocument.display]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [attachedDocument.JSON]  VARCHAR(MAX) '$.attachedDocument'
    ) AS rowset
    CROSS APPLY openjson (rowset.[attachedDocument.JSON]) with (
        [attachedDocument.id]          NVARCHAR(100)       '$.id',
        [attachedDocument.extension]   NVARCHAR(MAX)       '$.extension',
        [attachedDocument.reference]   NVARCHAR(4000)      '$.reference',
        [attachedDocument.type]        VARCHAR(256)        '$.type',
        [attachedDocument.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [attachedDocument.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [attachedDocument.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [attachedDocument.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [attachedDocument.identifier.system] VARCHAR(256)        '$.identifier.system',
        [attachedDocument.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [attachedDocument.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [attachedDocument.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [attachedDocument.display]     NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionMasterFile AS
SELECT
    [id],
    [masterFile.JSON],
    [masterFile.id],
    [masterFile.extension],
    [masterFile.reference],
    [masterFile.type],
    [masterFile.identifier.id],
    [masterFile.identifier.extension],
    [masterFile.identifier.use],
    [masterFile.identifier.type],
    [masterFile.identifier.system],
    [masterFile.identifier.value],
    [masterFile.identifier.period],
    [masterFile.identifier.assigner],
    [masterFile.display]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [masterFile.JSON]  VARCHAR(MAX) '$.masterFile'
    ) AS rowset
    CROSS APPLY openjson (rowset.[masterFile.JSON]) with (
        [masterFile.id]                NVARCHAR(100)       '$.id',
        [masterFile.extension]         NVARCHAR(MAX)       '$.extension',
        [masterFile.reference]         NVARCHAR(4000)      '$.reference',
        [masterFile.type]              VARCHAR(256)        '$.type',
        [masterFile.identifier.id]     NVARCHAR(100)       '$.identifier.id',
        [masterFile.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [masterFile.identifier.use]    NVARCHAR(64)        '$.identifier.use',
        [masterFile.identifier.type]   NVARCHAR(MAX)       '$.identifier.type',
        [masterFile.identifier.system] VARCHAR(256)        '$.identifier.system',
        [masterFile.identifier.value]  NVARCHAR(4000)      '$.identifier.value',
        [masterFile.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [masterFile.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [masterFile.display]           NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.modifierExtension],
    [contact.type.id],
    [contact.type.extension],
    [contact.type.coding],
    [contact.type.text],
    [contact.contact.id],
    [contact.contact.extension],
    [contact.contact.reference],
    [contact.contact.type],
    [contact.contact.identifier],
    [contact.contact.display]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contact.JSON]  VARCHAR(MAX) '$.contact'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contact.JSON]) with (
        [contact.id]                   NVARCHAR(100)       '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [contact.type.id]              NVARCHAR(100)       '$.type.id',
        [contact.type.extension]       NVARCHAR(MAX)       '$.type.extension',
        [contact.type.coding]          NVARCHAR(MAX)       '$.type.coding',
        [contact.type.text]            NVARCHAR(4000)      '$.type.text',
        [contact.contact.id]           NVARCHAR(100)       '$.contact.id',
        [contact.contact.extension]    NVARCHAR(MAX)       '$.contact.extension',
        [contact.contact.reference]    NVARCHAR(4000)      '$.contact.reference',
        [contact.contact.type]         VARCHAR(256)        '$.contact.type',
        [contact.contact.identifier]   NVARCHAR(MAX)       '$.contact.identifier',
        [contact.contact.display]      NVARCHAR(4000)      '$.contact.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionClinicalTrial AS
SELECT
    [id],
    [clinicalTrial.JSON],
    [clinicalTrial.id],
    [clinicalTrial.extension],
    [clinicalTrial.reference],
    [clinicalTrial.type],
    [clinicalTrial.identifier.id],
    [clinicalTrial.identifier.extension],
    [clinicalTrial.identifier.use],
    [clinicalTrial.identifier.type],
    [clinicalTrial.identifier.system],
    [clinicalTrial.identifier.value],
    [clinicalTrial.identifier.period],
    [clinicalTrial.identifier.assigner],
    [clinicalTrial.display]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [clinicalTrial.JSON]  VARCHAR(MAX) '$.clinicalTrial'
    ) AS rowset
    CROSS APPLY openjson (rowset.[clinicalTrial.JSON]) with (
        [clinicalTrial.id]             NVARCHAR(100)       '$.id',
        [clinicalTrial.extension]      NVARCHAR(MAX)       '$.extension',
        [clinicalTrial.reference]      NVARCHAR(4000)      '$.reference',
        [clinicalTrial.type]           VARCHAR(256)        '$.type',
        [clinicalTrial.identifier.id]  NVARCHAR(100)       '$.identifier.id',
        [clinicalTrial.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [clinicalTrial.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [clinicalTrial.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [clinicalTrial.identifier.system] VARCHAR(256)        '$.identifier.system',
        [clinicalTrial.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [clinicalTrial.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [clinicalTrial.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [clinicalTrial.display]        NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionCode AS
SELECT
    [id],
    [code.JSON],
    [code.id],
    [code.extension],
    [code.system],
    [code.version],
    [code.code],
    [code.display],
    [code.userSelected]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [code.JSON]  VARCHAR(MAX) '$.code'
    ) AS rowset
    CROSS APPLY openjson (rowset.[code.JSON]) with (
        [code.id]                      NVARCHAR(100)       '$.id',
        [code.extension]               NVARCHAR(MAX)       '$.extension',
        [code.system]                  VARCHAR(256)        '$.system',
        [code.version]                 NVARCHAR(100)       '$.version',
        [code.code]                    NVARCHAR(4000)      '$.code',
        [code.display]                 NVARCHAR(4000)      '$.display',
        [code.userSelected]            bit                 '$.userSelected'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionName AS
SELECT
    [id],
    [name.JSON],
    [name.id],
    [name.extension],
    [name.modifierExtension],
    [name.productName],
    [name.type.id],
    [name.type.extension],
    [name.type.coding],
    [name.type.text],
    [name.namePart],
    [name.countryLanguage]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [name.JSON]  VARCHAR(MAX) '$.name'
    ) AS rowset
    CROSS APPLY openjson (rowset.[name.JSON]) with (
        [name.id]                      NVARCHAR(100)       '$.id',
        [name.extension]               NVARCHAR(MAX)       '$.extension',
        [name.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [name.productName]             NVARCHAR(500)       '$.productName',
        [name.type.id]                 NVARCHAR(100)       '$.type.id',
        [name.type.extension]          NVARCHAR(MAX)       '$.type.extension',
        [name.type.coding]             NVARCHAR(MAX)       '$.type.coding',
        [name.type.text]               NVARCHAR(4000)      '$.type.text',
        [name.namePart]                NVARCHAR(MAX)       '$.namePart' AS JSON,
        [name.countryLanguage]         NVARCHAR(MAX)       '$.countryLanguage' AS JSON
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionCrossReference AS
SELECT
    [id],
    [crossReference.JSON],
    [crossReference.id],
    [crossReference.extension],
    [crossReference.modifierExtension],
    [crossReference.product.id],
    [crossReference.product.extension],
    [crossReference.product.concept],
    [crossReference.product.reference],
    [crossReference.type.id],
    [crossReference.type.extension],
    [crossReference.type.coding],
    [crossReference.type.text]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [crossReference.JSON]  VARCHAR(MAX) '$.crossReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[crossReference.JSON]) with (
        [crossReference.id]            NVARCHAR(100)       '$.id',
        [crossReference.extension]     NVARCHAR(MAX)       '$.extension',
        [crossReference.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [crossReference.product.id]    NVARCHAR(100)       '$.product.id',
        [crossReference.product.extension] NVARCHAR(MAX)       '$.product.extension',
        [crossReference.product.concept] NVARCHAR(MAX)       '$.product.concept',
        [crossReference.product.reference] NVARCHAR(MAX)       '$.product.reference',
        [crossReference.type.id]       NVARCHAR(100)       '$.type.id',
        [crossReference.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [crossReference.type.coding]   NVARCHAR(MAX)       '$.type.coding',
        [crossReference.type.text]     NVARCHAR(4000)      '$.type.text'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionOperation AS
SELECT
    [id],
    [operation.JSON],
    [operation.id],
    [operation.extension],
    [operation.modifierExtension],
    [operation.type.id],
    [operation.type.extension],
    [operation.type.concept],
    [operation.type.reference],
    [operation.effectiveDate.id],
    [operation.effectiveDate.extension],
    [operation.effectiveDate.start],
    [operation.effectiveDate.end],
    [operation.organization],
    [operation.confidentialityIndicator.id],
    [operation.confidentialityIndicator.extension],
    [operation.confidentialityIndicator.coding],
    [operation.confidentialityIndicator.text]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [operation.JSON]  VARCHAR(MAX) '$.operation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[operation.JSON]) with (
        [operation.id]                 NVARCHAR(100)       '$.id',
        [operation.extension]          NVARCHAR(MAX)       '$.extension',
        [operation.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [operation.type.id]            NVARCHAR(100)       '$.type.id',
        [operation.type.extension]     NVARCHAR(MAX)       '$.type.extension',
        [operation.type.concept]       NVARCHAR(MAX)       '$.type.concept',
        [operation.type.reference]     NVARCHAR(MAX)       '$.type.reference',
        [operation.effectiveDate.id]   NVARCHAR(100)       '$.effectiveDate.id',
        [operation.effectiveDate.extension] NVARCHAR(MAX)       '$.effectiveDate.extension',
        [operation.effectiveDate.start] VARCHAR(64)         '$.effectiveDate.start',
        [operation.effectiveDate.end]  VARCHAR(64)         '$.effectiveDate.end',
        [operation.organization]       NVARCHAR(MAX)       '$.organization' AS JSON,
        [operation.confidentialityIndicator.id] NVARCHAR(100)       '$.confidentialityIndicator.id',
        [operation.confidentialityIndicator.extension] NVARCHAR(MAX)       '$.confidentialityIndicator.extension',
        [operation.confidentialityIndicator.coding] NVARCHAR(MAX)       '$.confidentialityIndicator.coding',
        [operation.confidentialityIndicator.text] NVARCHAR(4000)      '$.confidentialityIndicator.text'
    ) j

GO

CREATE VIEW fhir.MedicinalProductDefinitionCharacteristic AS
SELECT
    [id],
    [characteristic.JSON],
    [characteristic.id],
    [characteristic.extension],
    [characteristic.modifierExtension],
    [characteristic.type.id],
    [characteristic.type.extension],
    [characteristic.type.coding],
    [characteristic.type.text],
    [characteristic.value.codeableConcept.id],
    [characteristic.value.codeableConcept.extension],
    [characteristic.value.codeableConcept.coding],
    [characteristic.value.codeableConcept.text],
    [characteristic.value.quantity.id],
    [characteristic.value.quantity.extension],
    [characteristic.value.quantity.value],
    [characteristic.value.quantity.comparator],
    [characteristic.value.quantity.unit],
    [characteristic.value.quantity.system],
    [characteristic.value.quantity.code],
    [characteristic.value.date],
    [characteristic.value.boolean],
    [characteristic.value.attachment.id],
    [characteristic.value.attachment.extension],
    [characteristic.value.attachment.contentType],
    [characteristic.value.attachment.language],
    [characteristic.value.attachment.data],
    [characteristic.value.attachment.url],
    [characteristic.value.attachment.size],
    [characteristic.value.attachment.hash],
    [characteristic.value.attachment.title],
    [characteristic.value.attachment.creation],
    [characteristic.value.attachment.height],
    [characteristic.value.attachment.width],
    [characteristic.value.attachment.frames],
    [characteristic.value.attachment.duration],
    [characteristic.value.attachment.pages]
FROM openrowset (
        BULK 'MedicinalProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [characteristic.JSON]  VARCHAR(MAX) '$.characteristic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[characteristic.JSON]) with (
        [characteristic.id]            NVARCHAR(100)       '$.id',
        [characteristic.extension]     NVARCHAR(MAX)       '$.extension',
        [characteristic.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [characteristic.type.id]       NVARCHAR(100)       '$.type.id',
        [characteristic.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [characteristic.type.coding]   NVARCHAR(MAX)       '$.type.coding',
        [characteristic.type.text]     NVARCHAR(4000)      '$.type.text',
        [characteristic.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [characteristic.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [characteristic.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [characteristic.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [characteristic.value.quantity.id] NVARCHAR(100)       '$.value.quantity.id',
        [characteristic.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [characteristic.value.quantity.value] float               '$.value.quantity.value',
        [characteristic.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [characteristic.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [characteristic.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [characteristic.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [characteristic.value.date]    VARCHAR(64)         '$.value.date',
        [characteristic.value.boolean] bit                 '$.value.boolean',
        [characteristic.value.attachment.id] NVARCHAR(100)       '$.value.attachment.id',
        [characteristic.value.attachment.extension] NVARCHAR(MAX)       '$.value.attachment.extension',
        [characteristic.value.attachment.contentType] NVARCHAR(100)       '$.value.attachment.contentType',
        [characteristic.value.attachment.language] NVARCHAR(100)       '$.value.attachment.language',
        [characteristic.value.attachment.data] NVARCHAR(MAX)       '$.value.attachment.data',
        [characteristic.value.attachment.url] VARCHAR(256)        '$.value.attachment.url',
        [characteristic.value.attachment.size] NVARCHAR(MAX)       '$.value.attachment.size',
        [characteristic.value.attachment.hash] NVARCHAR(MAX)       '$.value.attachment.hash',
        [characteristic.value.attachment.title] NVARCHAR(4000)      '$.value.attachment.title',
        [characteristic.value.attachment.creation] VARCHAR(64)         '$.value.attachment.creation',
        [characteristic.value.attachment.height] bigint              '$.value.attachment.height',
        [characteristic.value.attachment.width] bigint              '$.value.attachment.width',
        [characteristic.value.attachment.frames] bigint              '$.value.attachment.frames',
        [characteristic.value.attachment.duration] float               '$.value.attachment.duration',
        [characteristic.value.attachment.pages] bigint              '$.value.attachment.pages'
    ) j
