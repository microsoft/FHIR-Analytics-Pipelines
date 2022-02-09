CREATE EXTERNAL TABLE [fhir].[MedicinalProductPharmaceutical] (
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
    [administrableDoseForm.id] NVARCHAR(100),
    [administrableDoseForm.extension] NVARCHAR(MAX),
    [administrableDoseForm.coding] VARCHAR(MAX),
    [administrableDoseForm.text] NVARCHAR(4000),
    [unitOfPresentation.id] NVARCHAR(100),
    [unitOfPresentation.extension] NVARCHAR(MAX),
    [unitOfPresentation.coding] VARCHAR(MAX),
    [unitOfPresentation.text] NVARCHAR(4000),
    [ingredient] VARCHAR(MAX),
    [device] VARCHAR(MAX),
    [characteristics] VARCHAR(MAX),
    [routeOfAdministration] VARCHAR(MAX),
) WITH (
    LOCATION='/MedicinalProductPharmaceutical/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicinalProductPharmaceuticalIdentifier AS
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
        BULK 'MedicinalProductPharmaceutical/**',
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

CREATE VIEW fhir.MedicinalProductPharmaceuticalIngredient AS
SELECT
    [id],
    [ingredient.JSON],
    [ingredient.id],
    [ingredient.extension],
    [ingredient.reference],
    [ingredient.type],
    [ingredient.identifier.id],
    [ingredient.identifier.extension],
    [ingredient.identifier.use],
    [ingredient.identifier.type],
    [ingredient.identifier.system],
    [ingredient.identifier.value],
    [ingredient.identifier.period],
    [ingredient.identifier.assigner],
    [ingredient.display]
FROM openrowset (
        BULK 'MedicinalProductPharmaceutical/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [ingredient.JSON]  VARCHAR(MAX) '$.ingredient'
    ) AS rowset
    CROSS APPLY openjson (rowset.[ingredient.JSON]) with (
        [ingredient.id]                NVARCHAR(100)       '$.id',
        [ingredient.extension]         NVARCHAR(MAX)       '$.extension',
        [ingredient.reference]         NVARCHAR(4000)      '$.reference',
        [ingredient.type]              VARCHAR(256)        '$.type',
        [ingredient.identifier.id]     NVARCHAR(100)       '$.identifier.id',
        [ingredient.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [ingredient.identifier.use]    NVARCHAR(64)        '$.identifier.use',
        [ingredient.identifier.type]   NVARCHAR(MAX)       '$.identifier.type',
        [ingredient.identifier.system] VARCHAR(256)        '$.identifier.system',
        [ingredient.identifier.value]  NVARCHAR(4000)      '$.identifier.value',
        [ingredient.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [ingredient.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [ingredient.display]           NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductPharmaceuticalDevice AS
SELECT
    [id],
    [device.JSON],
    [device.id],
    [device.extension],
    [device.reference],
    [device.type],
    [device.identifier.id],
    [device.identifier.extension],
    [device.identifier.use],
    [device.identifier.type],
    [device.identifier.system],
    [device.identifier.value],
    [device.identifier.period],
    [device.identifier.assigner],
    [device.display]
FROM openrowset (
        BULK 'MedicinalProductPharmaceutical/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [device.JSON]  VARCHAR(MAX) '$.device'
    ) AS rowset
    CROSS APPLY openjson (rowset.[device.JSON]) with (
        [device.id]                    NVARCHAR(100)       '$.id',
        [device.extension]             NVARCHAR(MAX)       '$.extension',
        [device.reference]             NVARCHAR(4000)      '$.reference',
        [device.type]                  VARCHAR(256)        '$.type',
        [device.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [device.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [device.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [device.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [device.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [device.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [device.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [device.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [device.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductPharmaceuticalCharacteristics AS
SELECT
    [id],
    [characteristics.JSON],
    [characteristics.id],
    [characteristics.extension],
    [characteristics.modifierExtension],
    [characteristics.code.id],
    [characteristics.code.extension],
    [characteristics.code.coding],
    [characteristics.code.text],
    [characteristics.status.id],
    [characteristics.status.extension],
    [characteristics.status.coding],
    [characteristics.status.text]
FROM openrowset (
        BULK 'MedicinalProductPharmaceutical/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [characteristics.JSON]  VARCHAR(MAX) '$.characteristics'
    ) AS rowset
    CROSS APPLY openjson (rowset.[characteristics.JSON]) with (
        [characteristics.id]           NVARCHAR(100)       '$.id',
        [characteristics.extension]    NVARCHAR(MAX)       '$.extension',
        [characteristics.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [characteristics.code.id]      NVARCHAR(100)       '$.code.id',
        [characteristics.code.extension] NVARCHAR(MAX)       '$.code.extension',
        [characteristics.code.coding]  NVARCHAR(MAX)       '$.code.coding',
        [characteristics.code.text]    NVARCHAR(4000)      '$.code.text',
        [characteristics.status.id]    NVARCHAR(100)       '$.status.id',
        [characteristics.status.extension] NVARCHAR(MAX)       '$.status.extension',
        [characteristics.status.coding] NVARCHAR(MAX)       '$.status.coding',
        [characteristics.status.text]  NVARCHAR(4000)      '$.status.text'
    ) j

GO

CREATE VIEW fhir.MedicinalProductPharmaceuticalRouteOfAdministration AS
SELECT
    [id],
    [routeOfAdministration.JSON],
    [routeOfAdministration.id],
    [routeOfAdministration.extension],
    [routeOfAdministration.modifierExtension],
    [routeOfAdministration.code.id],
    [routeOfAdministration.code.extension],
    [routeOfAdministration.code.coding],
    [routeOfAdministration.code.text],
    [routeOfAdministration.firstDose.id],
    [routeOfAdministration.firstDose.extension],
    [routeOfAdministration.firstDose.value],
    [routeOfAdministration.firstDose.comparator],
    [routeOfAdministration.firstDose.unit],
    [routeOfAdministration.firstDose.system],
    [routeOfAdministration.firstDose.code],
    [routeOfAdministration.maxSingleDose.id],
    [routeOfAdministration.maxSingleDose.extension],
    [routeOfAdministration.maxSingleDose.value],
    [routeOfAdministration.maxSingleDose.comparator],
    [routeOfAdministration.maxSingleDose.unit],
    [routeOfAdministration.maxSingleDose.system],
    [routeOfAdministration.maxSingleDose.code],
    [routeOfAdministration.maxDosePerDay.id],
    [routeOfAdministration.maxDosePerDay.extension],
    [routeOfAdministration.maxDosePerDay.value],
    [routeOfAdministration.maxDosePerDay.comparator],
    [routeOfAdministration.maxDosePerDay.unit],
    [routeOfAdministration.maxDosePerDay.system],
    [routeOfAdministration.maxDosePerDay.code],
    [routeOfAdministration.maxDosePerTreatmentPeriod.id],
    [routeOfAdministration.maxDosePerTreatmentPeriod.extension],
    [routeOfAdministration.maxDosePerTreatmentPeriod.numerator],
    [routeOfAdministration.maxDosePerTreatmentPeriod.denominator],
    [routeOfAdministration.maxTreatmentPeriod.id],
    [routeOfAdministration.maxTreatmentPeriod.extension],
    [routeOfAdministration.maxTreatmentPeriod.value],
    [routeOfAdministration.maxTreatmentPeriod.comparator],
    [routeOfAdministration.maxTreatmentPeriod.unit],
    [routeOfAdministration.maxTreatmentPeriod.system],
    [routeOfAdministration.maxTreatmentPeriod.code],
    [routeOfAdministration.targetSpecies]
FROM openrowset (
        BULK 'MedicinalProductPharmaceutical/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [routeOfAdministration.JSON]  VARCHAR(MAX) '$.routeOfAdministration'
    ) AS rowset
    CROSS APPLY openjson (rowset.[routeOfAdministration.JSON]) with (
        [routeOfAdministration.id]     NVARCHAR(100)       '$.id',
        [routeOfAdministration.extension] NVARCHAR(MAX)       '$.extension',
        [routeOfAdministration.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [routeOfAdministration.code.id] NVARCHAR(100)       '$.code.id',
        [routeOfAdministration.code.extension] NVARCHAR(MAX)       '$.code.extension',
        [routeOfAdministration.code.coding] NVARCHAR(MAX)       '$.code.coding',
        [routeOfAdministration.code.text] NVARCHAR(4000)      '$.code.text',
        [routeOfAdministration.firstDose.id] NVARCHAR(100)       '$.firstDose.id',
        [routeOfAdministration.firstDose.extension] NVARCHAR(MAX)       '$.firstDose.extension',
        [routeOfAdministration.firstDose.value] float               '$.firstDose.value',
        [routeOfAdministration.firstDose.comparator] NVARCHAR(64)        '$.firstDose.comparator',
        [routeOfAdministration.firstDose.unit] NVARCHAR(100)       '$.firstDose.unit',
        [routeOfAdministration.firstDose.system] VARCHAR(256)        '$.firstDose.system',
        [routeOfAdministration.firstDose.code] NVARCHAR(4000)      '$.firstDose.code',
        [routeOfAdministration.maxSingleDose.id] NVARCHAR(100)       '$.maxSingleDose.id',
        [routeOfAdministration.maxSingleDose.extension] NVARCHAR(MAX)       '$.maxSingleDose.extension',
        [routeOfAdministration.maxSingleDose.value] float               '$.maxSingleDose.value',
        [routeOfAdministration.maxSingleDose.comparator] NVARCHAR(64)        '$.maxSingleDose.comparator',
        [routeOfAdministration.maxSingleDose.unit] NVARCHAR(100)       '$.maxSingleDose.unit',
        [routeOfAdministration.maxSingleDose.system] VARCHAR(256)        '$.maxSingleDose.system',
        [routeOfAdministration.maxSingleDose.code] NVARCHAR(4000)      '$.maxSingleDose.code',
        [routeOfAdministration.maxDosePerDay.id] NVARCHAR(100)       '$.maxDosePerDay.id',
        [routeOfAdministration.maxDosePerDay.extension] NVARCHAR(MAX)       '$.maxDosePerDay.extension',
        [routeOfAdministration.maxDosePerDay.value] float               '$.maxDosePerDay.value',
        [routeOfAdministration.maxDosePerDay.comparator] NVARCHAR(64)        '$.maxDosePerDay.comparator',
        [routeOfAdministration.maxDosePerDay.unit] NVARCHAR(100)       '$.maxDosePerDay.unit',
        [routeOfAdministration.maxDosePerDay.system] VARCHAR(256)        '$.maxDosePerDay.system',
        [routeOfAdministration.maxDosePerDay.code] NVARCHAR(4000)      '$.maxDosePerDay.code',
        [routeOfAdministration.maxDosePerTreatmentPeriod.id] NVARCHAR(100)       '$.maxDosePerTreatmentPeriod.id',
        [routeOfAdministration.maxDosePerTreatmentPeriod.extension] NVARCHAR(MAX)       '$.maxDosePerTreatmentPeriod.extension',
        [routeOfAdministration.maxDosePerTreatmentPeriod.numerator] NVARCHAR(MAX)       '$.maxDosePerTreatmentPeriod.numerator',
        [routeOfAdministration.maxDosePerTreatmentPeriod.denominator] NVARCHAR(MAX)       '$.maxDosePerTreatmentPeriod.denominator',
        [routeOfAdministration.maxTreatmentPeriod.id] NVARCHAR(100)       '$.maxTreatmentPeriod.id',
        [routeOfAdministration.maxTreatmentPeriod.extension] NVARCHAR(MAX)       '$.maxTreatmentPeriod.extension',
        [routeOfAdministration.maxTreatmentPeriod.value] float               '$.maxTreatmentPeriod.value',
        [routeOfAdministration.maxTreatmentPeriod.comparator] NVARCHAR(64)        '$.maxTreatmentPeriod.comparator',
        [routeOfAdministration.maxTreatmentPeriod.unit] NVARCHAR(100)       '$.maxTreatmentPeriod.unit',
        [routeOfAdministration.maxTreatmentPeriod.system] VARCHAR(256)        '$.maxTreatmentPeriod.system',
        [routeOfAdministration.maxTreatmentPeriod.code] NVARCHAR(4000)      '$.maxTreatmentPeriod.code',
        [routeOfAdministration.targetSpecies] NVARCHAR(MAX)       '$.targetSpecies' AS JSON
    ) j
