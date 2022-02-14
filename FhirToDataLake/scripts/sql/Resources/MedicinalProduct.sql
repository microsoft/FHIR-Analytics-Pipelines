CREATE EXTERNAL TABLE [fhir].[MedicinalProduct] (
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
    [domain.system] VARCHAR(256),
    [domain.version] NVARCHAR(100),
    [domain.code] NVARCHAR(4000),
    [domain.display] NVARCHAR(4000),
    [domain.userSelected] bit,
    [combinedPharmaceuticalDoseForm.id] NVARCHAR(100),
    [combinedPharmaceuticalDoseForm.extension] NVARCHAR(MAX),
    [combinedPharmaceuticalDoseForm.coding] VARCHAR(MAX),
    [combinedPharmaceuticalDoseForm.text] NVARCHAR(4000),
    [legalStatusOfSupply.id] NVARCHAR(100),
    [legalStatusOfSupply.extension] NVARCHAR(MAX),
    [legalStatusOfSupply.coding] VARCHAR(MAX),
    [legalStatusOfSupply.text] NVARCHAR(4000),
    [additionalMonitoringIndicator.id] NVARCHAR(100),
    [additionalMonitoringIndicator.extension] NVARCHAR(MAX),
    [additionalMonitoringIndicator.coding] VARCHAR(MAX),
    [additionalMonitoringIndicator.text] NVARCHAR(4000),
    [specialMeasures] VARCHAR(MAX),
    [paediatricUseIndicator.id] NVARCHAR(100),
    [paediatricUseIndicator.extension] NVARCHAR(MAX),
    [paediatricUseIndicator.coding] VARCHAR(MAX),
    [paediatricUseIndicator.text] NVARCHAR(4000),
    [productClassification] VARCHAR(MAX),
    [marketingStatus] VARCHAR(MAX),
    [pharmaceuticalProduct] VARCHAR(MAX),
    [packagedMedicinalProduct] VARCHAR(MAX),
    [attachedDocument] VARCHAR(MAX),
    [masterFile] VARCHAR(MAX),
    [contact] VARCHAR(MAX),
    [clinicalTrial] VARCHAR(MAX),
    [name] VARCHAR(MAX),
    [crossReference] VARCHAR(MAX),
    [manufacturingBusinessOperation] VARCHAR(MAX),
    [specialDesignation] VARCHAR(MAX),
) WITH (
    LOCATION='/MedicinalProduct/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicinalProductIdentifier AS
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
        BULK 'MedicinalProduct/**',
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

CREATE VIEW fhir.MedicinalProductSpecialMeasures AS
SELECT
    [id],
    [specialMeasures.JSON],
    [specialMeasures]
FROM openrowset (
        BULK 'MedicinalProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specialMeasures.JSON]  VARCHAR(MAX) '$.specialMeasures'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specialMeasures.JSON]) with (
        [specialMeasures]              NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.MedicinalProductProductClassification AS
SELECT
    [id],
    [productClassification.JSON],
    [productClassification.id],
    [productClassification.extension],
    [productClassification.coding],
    [productClassification.text]
FROM openrowset (
        BULK 'MedicinalProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [productClassification.JSON]  VARCHAR(MAX) '$.productClassification'
    ) AS rowset
    CROSS APPLY openjson (rowset.[productClassification.JSON]) with (
        [productClassification.id]     NVARCHAR(100)       '$.id',
        [productClassification.extension] NVARCHAR(MAX)       '$.extension',
        [productClassification.coding] NVARCHAR(MAX)       '$.coding' AS JSON,
        [productClassification.text]   NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.MedicinalProductMarketingStatus AS
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
        BULK 'MedicinalProduct/**',
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

CREATE VIEW fhir.MedicinalProductPharmaceuticalProduct AS
SELECT
    [id],
    [pharmaceuticalProduct.JSON],
    [pharmaceuticalProduct.id],
    [pharmaceuticalProduct.extension],
    [pharmaceuticalProduct.reference],
    [pharmaceuticalProduct.type],
    [pharmaceuticalProduct.identifier.id],
    [pharmaceuticalProduct.identifier.extension],
    [pharmaceuticalProduct.identifier.use],
    [pharmaceuticalProduct.identifier.type],
    [pharmaceuticalProduct.identifier.system],
    [pharmaceuticalProduct.identifier.value],
    [pharmaceuticalProduct.identifier.period],
    [pharmaceuticalProduct.identifier.assigner],
    [pharmaceuticalProduct.display]
FROM openrowset (
        BULK 'MedicinalProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [pharmaceuticalProduct.JSON]  VARCHAR(MAX) '$.pharmaceuticalProduct'
    ) AS rowset
    CROSS APPLY openjson (rowset.[pharmaceuticalProduct.JSON]) with (
        [pharmaceuticalProduct.id]     NVARCHAR(100)       '$.id',
        [pharmaceuticalProduct.extension] NVARCHAR(MAX)       '$.extension',
        [pharmaceuticalProduct.reference] NVARCHAR(4000)      '$.reference',
        [pharmaceuticalProduct.type]   VARCHAR(256)        '$.type',
        [pharmaceuticalProduct.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [pharmaceuticalProduct.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [pharmaceuticalProduct.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [pharmaceuticalProduct.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [pharmaceuticalProduct.identifier.system] VARCHAR(256)        '$.identifier.system',
        [pharmaceuticalProduct.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [pharmaceuticalProduct.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [pharmaceuticalProduct.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [pharmaceuticalProduct.display] NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductPackagedMedicinalProduct AS
SELECT
    [id],
    [packagedMedicinalProduct.JSON],
    [packagedMedicinalProduct.id],
    [packagedMedicinalProduct.extension],
    [packagedMedicinalProduct.reference],
    [packagedMedicinalProduct.type],
    [packagedMedicinalProduct.identifier.id],
    [packagedMedicinalProduct.identifier.extension],
    [packagedMedicinalProduct.identifier.use],
    [packagedMedicinalProduct.identifier.type],
    [packagedMedicinalProduct.identifier.system],
    [packagedMedicinalProduct.identifier.value],
    [packagedMedicinalProduct.identifier.period],
    [packagedMedicinalProduct.identifier.assigner],
    [packagedMedicinalProduct.display]
FROM openrowset (
        BULK 'MedicinalProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [packagedMedicinalProduct.JSON]  VARCHAR(MAX) '$.packagedMedicinalProduct'
    ) AS rowset
    CROSS APPLY openjson (rowset.[packagedMedicinalProduct.JSON]) with (
        [packagedMedicinalProduct.id]  NVARCHAR(100)       '$.id',
        [packagedMedicinalProduct.extension] NVARCHAR(MAX)       '$.extension',
        [packagedMedicinalProduct.reference] NVARCHAR(4000)      '$.reference',
        [packagedMedicinalProduct.type] VARCHAR(256)        '$.type',
        [packagedMedicinalProduct.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [packagedMedicinalProduct.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [packagedMedicinalProduct.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [packagedMedicinalProduct.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [packagedMedicinalProduct.identifier.system] VARCHAR(256)        '$.identifier.system',
        [packagedMedicinalProduct.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [packagedMedicinalProduct.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [packagedMedicinalProduct.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [packagedMedicinalProduct.display] NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductAttachedDocument AS
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
        BULK 'MedicinalProduct/**',
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

CREATE VIEW fhir.MedicinalProductMasterFile AS
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
        BULK 'MedicinalProduct/**',
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

CREATE VIEW fhir.MedicinalProductContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.reference],
    [contact.type],
    [contact.identifier.id],
    [contact.identifier.extension],
    [contact.identifier.use],
    [contact.identifier.type],
    [contact.identifier.system],
    [contact.identifier.value],
    [contact.identifier.period],
    [contact.identifier.assigner],
    [contact.display]
FROM openrowset (
        BULK 'MedicinalProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contact.JSON]  VARCHAR(MAX) '$.contact'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contact.JSON]) with (
        [contact.id]                   NVARCHAR(100)       '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.reference]            NVARCHAR(4000)      '$.reference',
        [contact.type]                 VARCHAR(256)        '$.type',
        [contact.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [contact.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [contact.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [contact.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [contact.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [contact.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [contact.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [contact.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [contact.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductClinicalTrial AS
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
        BULK 'MedicinalProduct/**',
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

CREATE VIEW fhir.MedicinalProductName AS
SELECT
    [id],
    [name.JSON],
    [name.id],
    [name.extension],
    [name.modifierExtension],
    [name.productName],
    [name.namePart],
    [name.countryLanguage]
FROM openrowset (
        BULK 'MedicinalProduct/**',
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
        [name.namePart]                NVARCHAR(MAX)       '$.namePart' AS JSON,
        [name.countryLanguage]         NVARCHAR(MAX)       '$.countryLanguage' AS JSON
    ) j

GO

CREATE VIEW fhir.MedicinalProductCrossReference AS
SELECT
    [id],
    [crossReference.JSON],
    [crossReference.id],
    [crossReference.extension],
    [crossReference.use],
    [crossReference.type.id],
    [crossReference.type.extension],
    [crossReference.type.coding],
    [crossReference.type.text],
    [crossReference.system],
    [crossReference.value],
    [crossReference.period.id],
    [crossReference.period.extension],
    [crossReference.period.start],
    [crossReference.period.end],
    [crossReference.assigner.id],
    [crossReference.assigner.extension],
    [crossReference.assigner.reference],
    [crossReference.assigner.type],
    [crossReference.assigner.identifier],
    [crossReference.assigner.display]
FROM openrowset (
        BULK 'MedicinalProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [crossReference.JSON]  VARCHAR(MAX) '$.crossReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[crossReference.JSON]) with (
        [crossReference.id]            NVARCHAR(100)       '$.id',
        [crossReference.extension]     NVARCHAR(MAX)       '$.extension',
        [crossReference.use]           NVARCHAR(64)        '$.use',
        [crossReference.type.id]       NVARCHAR(100)       '$.type.id',
        [crossReference.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [crossReference.type.coding]   NVARCHAR(MAX)       '$.type.coding',
        [crossReference.type.text]     NVARCHAR(4000)      '$.type.text',
        [crossReference.system]        VARCHAR(256)        '$.system',
        [crossReference.value]         NVARCHAR(4000)      '$.value',
        [crossReference.period.id]     NVARCHAR(100)       '$.period.id',
        [crossReference.period.extension] NVARCHAR(MAX)       '$.period.extension',
        [crossReference.period.start]  VARCHAR(64)         '$.period.start',
        [crossReference.period.end]    VARCHAR(64)         '$.period.end',
        [crossReference.assigner.id]   NVARCHAR(100)       '$.assigner.id',
        [crossReference.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [crossReference.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [crossReference.assigner.type] VARCHAR(256)        '$.assigner.type',
        [crossReference.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [crossReference.assigner.display] NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductManufacturingBusinessOperation AS
SELECT
    [id],
    [manufacturingBusinessOperation.JSON],
    [manufacturingBusinessOperation.id],
    [manufacturingBusinessOperation.extension],
    [manufacturingBusinessOperation.modifierExtension],
    [manufacturingBusinessOperation.operationType.id],
    [manufacturingBusinessOperation.operationType.extension],
    [manufacturingBusinessOperation.operationType.coding],
    [manufacturingBusinessOperation.operationType.text],
    [manufacturingBusinessOperation.authorisationReferenceNumber.id],
    [manufacturingBusinessOperation.authorisationReferenceNumber.extension],
    [manufacturingBusinessOperation.authorisationReferenceNumber.use],
    [manufacturingBusinessOperation.authorisationReferenceNumber.type],
    [manufacturingBusinessOperation.authorisationReferenceNumber.system],
    [manufacturingBusinessOperation.authorisationReferenceNumber.value],
    [manufacturingBusinessOperation.authorisationReferenceNumber.period],
    [manufacturingBusinessOperation.authorisationReferenceNumber.assigner],
    [manufacturingBusinessOperation.effectiveDate],
    [manufacturingBusinessOperation.confidentialityIndicator.id],
    [manufacturingBusinessOperation.confidentialityIndicator.extension],
    [manufacturingBusinessOperation.confidentialityIndicator.coding],
    [manufacturingBusinessOperation.confidentialityIndicator.text],
    [manufacturingBusinessOperation.manufacturer],
    [manufacturingBusinessOperation.regulator.id],
    [manufacturingBusinessOperation.regulator.extension],
    [manufacturingBusinessOperation.regulator.reference],
    [manufacturingBusinessOperation.regulator.type],
    [manufacturingBusinessOperation.regulator.identifier],
    [manufacturingBusinessOperation.regulator.display]
FROM openrowset (
        BULK 'MedicinalProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [manufacturingBusinessOperation.JSON]  VARCHAR(MAX) '$.manufacturingBusinessOperation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[manufacturingBusinessOperation.JSON]) with (
        [manufacturingBusinessOperation.id] NVARCHAR(100)       '$.id',
        [manufacturingBusinessOperation.extension] NVARCHAR(MAX)       '$.extension',
        [manufacturingBusinessOperation.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [manufacturingBusinessOperation.operationType.id] NVARCHAR(100)       '$.operationType.id',
        [manufacturingBusinessOperation.operationType.extension] NVARCHAR(MAX)       '$.operationType.extension',
        [manufacturingBusinessOperation.operationType.coding] NVARCHAR(MAX)       '$.operationType.coding',
        [manufacturingBusinessOperation.operationType.text] NVARCHAR(4000)      '$.operationType.text',
        [manufacturingBusinessOperation.authorisationReferenceNumber.id] NVARCHAR(100)       '$.authorisationReferenceNumber.id',
        [manufacturingBusinessOperation.authorisationReferenceNumber.extension] NVARCHAR(MAX)       '$.authorisationReferenceNumber.extension',
        [manufacturingBusinessOperation.authorisationReferenceNumber.use] NVARCHAR(64)        '$.authorisationReferenceNumber.use',
        [manufacturingBusinessOperation.authorisationReferenceNumber.type] NVARCHAR(MAX)       '$.authorisationReferenceNumber.type',
        [manufacturingBusinessOperation.authorisationReferenceNumber.system] VARCHAR(256)        '$.authorisationReferenceNumber.system',
        [manufacturingBusinessOperation.authorisationReferenceNumber.value] NVARCHAR(4000)      '$.authorisationReferenceNumber.value',
        [manufacturingBusinessOperation.authorisationReferenceNumber.period] NVARCHAR(MAX)       '$.authorisationReferenceNumber.period',
        [manufacturingBusinessOperation.authorisationReferenceNumber.assigner] NVARCHAR(MAX)       '$.authorisationReferenceNumber.assigner',
        [manufacturingBusinessOperation.effectiveDate] VARCHAR(64)         '$.effectiveDate',
        [manufacturingBusinessOperation.confidentialityIndicator.id] NVARCHAR(100)       '$.confidentialityIndicator.id',
        [manufacturingBusinessOperation.confidentialityIndicator.extension] NVARCHAR(MAX)       '$.confidentialityIndicator.extension',
        [manufacturingBusinessOperation.confidentialityIndicator.coding] NVARCHAR(MAX)       '$.confidentialityIndicator.coding',
        [manufacturingBusinessOperation.confidentialityIndicator.text] NVARCHAR(4000)      '$.confidentialityIndicator.text',
        [manufacturingBusinessOperation.manufacturer] NVARCHAR(MAX)       '$.manufacturer' AS JSON,
        [manufacturingBusinessOperation.regulator.id] NVARCHAR(100)       '$.regulator.id',
        [manufacturingBusinessOperation.regulator.extension] NVARCHAR(MAX)       '$.regulator.extension',
        [manufacturingBusinessOperation.regulator.reference] NVARCHAR(4000)      '$.regulator.reference',
        [manufacturingBusinessOperation.regulator.type] VARCHAR(256)        '$.regulator.type',
        [manufacturingBusinessOperation.regulator.identifier] NVARCHAR(MAX)       '$.regulator.identifier',
        [manufacturingBusinessOperation.regulator.display] NVARCHAR(4000)      '$.regulator.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductSpecialDesignation AS
SELECT
    [id],
    [specialDesignation.JSON],
    [specialDesignation.id],
    [specialDesignation.extension],
    [specialDesignation.modifierExtension],
    [specialDesignation.identifier],
    [specialDesignation.type.id],
    [specialDesignation.type.extension],
    [specialDesignation.type.coding],
    [specialDesignation.type.text],
    [specialDesignation.intendedUse.id],
    [specialDesignation.intendedUse.extension],
    [specialDesignation.intendedUse.coding],
    [specialDesignation.intendedUse.text],
    [specialDesignation.status.id],
    [specialDesignation.status.extension],
    [specialDesignation.status.coding],
    [specialDesignation.status.text],
    [specialDesignation.date],
    [specialDesignation.species.id],
    [specialDesignation.species.extension],
    [specialDesignation.species.coding],
    [specialDesignation.species.text],
    [specialDesignation.indication.codeableConcept.id],
    [specialDesignation.indication.codeableConcept.extension],
    [specialDesignation.indication.codeableConcept.coding],
    [specialDesignation.indication.codeableConcept.text],
    [specialDesignation.indication.reference.id],
    [specialDesignation.indication.reference.extension],
    [specialDesignation.indication.reference.reference],
    [specialDesignation.indication.reference.type],
    [specialDesignation.indication.reference.identifier],
    [specialDesignation.indication.reference.display]
FROM openrowset (
        BULK 'MedicinalProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specialDesignation.JSON]  VARCHAR(MAX) '$.specialDesignation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specialDesignation.JSON]) with (
        [specialDesignation.id]        NVARCHAR(100)       '$.id',
        [specialDesignation.extension] NVARCHAR(MAX)       '$.extension',
        [specialDesignation.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [specialDesignation.identifier] NVARCHAR(MAX)       '$.identifier' AS JSON,
        [specialDesignation.type.id]   NVARCHAR(100)       '$.type.id',
        [specialDesignation.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [specialDesignation.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [specialDesignation.type.text] NVARCHAR(4000)      '$.type.text',
        [specialDesignation.intendedUse.id] NVARCHAR(100)       '$.intendedUse.id',
        [specialDesignation.intendedUse.extension] NVARCHAR(MAX)       '$.intendedUse.extension',
        [specialDesignation.intendedUse.coding] NVARCHAR(MAX)       '$.intendedUse.coding',
        [specialDesignation.intendedUse.text] NVARCHAR(4000)      '$.intendedUse.text',
        [specialDesignation.status.id] NVARCHAR(100)       '$.status.id',
        [specialDesignation.status.extension] NVARCHAR(MAX)       '$.status.extension',
        [specialDesignation.status.coding] NVARCHAR(MAX)       '$.status.coding',
        [specialDesignation.status.text] NVARCHAR(4000)      '$.status.text',
        [specialDesignation.date]      VARCHAR(64)         '$.date',
        [specialDesignation.species.id] NVARCHAR(100)       '$.species.id',
        [specialDesignation.species.extension] NVARCHAR(MAX)       '$.species.extension',
        [specialDesignation.species.coding] NVARCHAR(MAX)       '$.species.coding',
        [specialDesignation.species.text] NVARCHAR(4000)      '$.species.text',
        [specialDesignation.indication.codeableConcept.id] NVARCHAR(100)       '$.indication.codeableConcept.id',
        [specialDesignation.indication.codeableConcept.extension] NVARCHAR(MAX)       '$.indication.codeableConcept.extension',
        [specialDesignation.indication.codeableConcept.coding] NVARCHAR(MAX)       '$.indication.codeableConcept.coding',
        [specialDesignation.indication.codeableConcept.text] NVARCHAR(4000)      '$.indication.codeableConcept.text',
        [specialDesignation.indication.reference.id] NVARCHAR(100)       '$.indication.reference.id',
        [specialDesignation.indication.reference.extension] NVARCHAR(MAX)       '$.indication.reference.extension',
        [specialDesignation.indication.reference.reference] NVARCHAR(4000)      '$.indication.reference.reference',
        [specialDesignation.indication.reference.type] VARCHAR(256)        '$.indication.reference.type',
        [specialDesignation.indication.reference.identifier] NVARCHAR(MAX)       '$.indication.reference.identifier',
        [specialDesignation.indication.reference.display] NVARCHAR(4000)      '$.indication.reference.display'
    ) j
