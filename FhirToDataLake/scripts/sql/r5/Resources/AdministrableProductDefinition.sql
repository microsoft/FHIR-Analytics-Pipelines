CREATE EXTERNAL TABLE [fhir].[AdministrableProductDefinition] (
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
    [formOf] VARCHAR(MAX),
    [administrableDoseForm.id] NVARCHAR(100),
    [administrableDoseForm.extension] NVARCHAR(MAX),
    [administrableDoseForm.coding] VARCHAR(MAX),
    [administrableDoseForm.text] NVARCHAR(4000),
    [unitOfPresentation.id] NVARCHAR(100),
    [unitOfPresentation.extension] NVARCHAR(MAX),
    [unitOfPresentation.coding] VARCHAR(MAX),
    [unitOfPresentation.text] NVARCHAR(4000),
    [producedFrom] VARCHAR(MAX),
    [ingredient] VARCHAR(MAX),
    [device.id] NVARCHAR(100),
    [device.extension] NVARCHAR(MAX),
    [device.reference] NVARCHAR(4000),
    [device.type] VARCHAR(256),
    [device.identifier.id] NVARCHAR(100),
    [device.identifier.extension] NVARCHAR(MAX),
    [device.identifier.use] NVARCHAR(64),
    [device.identifier.type] NVARCHAR(MAX),
    [device.identifier.system] VARCHAR(256),
    [device.identifier.value] NVARCHAR(4000),
    [device.identifier.period] NVARCHAR(MAX),
    [device.identifier.assigner] NVARCHAR(MAX),
    [device.display] NVARCHAR(4000),
    [property] VARCHAR(MAX),
    [routeOfAdministration] VARCHAR(MAX),
) WITH (
    LOCATION='/AdministrableProductDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.AdministrableProductDefinitionIdentifier AS
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
        BULK 'AdministrableProductDefinition/**',
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

CREATE VIEW fhir.AdministrableProductDefinitionFormOf AS
SELECT
    [id],
    [formOf.JSON],
    [formOf.id],
    [formOf.extension],
    [formOf.reference],
    [formOf.type],
    [formOf.identifier.id],
    [formOf.identifier.extension],
    [formOf.identifier.use],
    [formOf.identifier.type],
    [formOf.identifier.system],
    [formOf.identifier.value],
    [formOf.identifier.period],
    [formOf.identifier.assigner],
    [formOf.display]
FROM openrowset (
        BULK 'AdministrableProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [formOf.JSON]  VARCHAR(MAX) '$.formOf'
    ) AS rowset
    CROSS APPLY openjson (rowset.[formOf.JSON]) with (
        [formOf.id]                    NVARCHAR(100)       '$.id',
        [formOf.extension]             NVARCHAR(MAX)       '$.extension',
        [formOf.reference]             NVARCHAR(4000)      '$.reference',
        [formOf.type]                  VARCHAR(256)        '$.type',
        [formOf.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [formOf.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [formOf.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [formOf.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [formOf.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [formOf.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [formOf.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [formOf.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [formOf.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.AdministrableProductDefinitionProducedFrom AS
SELECT
    [id],
    [producedFrom.JSON],
    [producedFrom.id],
    [producedFrom.extension],
    [producedFrom.reference],
    [producedFrom.type],
    [producedFrom.identifier.id],
    [producedFrom.identifier.extension],
    [producedFrom.identifier.use],
    [producedFrom.identifier.type],
    [producedFrom.identifier.system],
    [producedFrom.identifier.value],
    [producedFrom.identifier.period],
    [producedFrom.identifier.assigner],
    [producedFrom.display]
FROM openrowset (
        BULK 'AdministrableProductDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [producedFrom.JSON]  VARCHAR(MAX) '$.producedFrom'
    ) AS rowset
    CROSS APPLY openjson (rowset.[producedFrom.JSON]) with (
        [producedFrom.id]              NVARCHAR(100)       '$.id',
        [producedFrom.extension]       NVARCHAR(MAX)       '$.extension',
        [producedFrom.reference]       NVARCHAR(4000)      '$.reference',
        [producedFrom.type]            VARCHAR(256)        '$.type',
        [producedFrom.identifier.id]   NVARCHAR(100)       '$.identifier.id',
        [producedFrom.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [producedFrom.identifier.use]  NVARCHAR(64)        '$.identifier.use',
        [producedFrom.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [producedFrom.identifier.system] VARCHAR(256)        '$.identifier.system',
        [producedFrom.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [producedFrom.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [producedFrom.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [producedFrom.display]         NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.AdministrableProductDefinitionIngredient AS
SELECT
    [id],
    [ingredient.JSON],
    [ingredient.id],
    [ingredient.extension],
    [ingredient.coding],
    [ingredient.text]
FROM openrowset (
        BULK 'AdministrableProductDefinition/**',
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

CREATE VIEW fhir.AdministrableProductDefinitionProperty AS
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
    [property.status.id],
    [property.status.extension],
    [property.status.coding],
    [property.status.text],
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
        BULK 'AdministrableProductDefinition/**',
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
        [property.status.id]           NVARCHAR(100)       '$.status.id',
        [property.status.extension]    NVARCHAR(MAX)       '$.status.extension',
        [property.status.coding]       NVARCHAR(MAX)       '$.status.coding',
        [property.status.text]         NVARCHAR(4000)      '$.status.text',
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

GO

CREATE VIEW fhir.AdministrableProductDefinitionRouteOfAdministration AS
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
        BULK 'AdministrableProductDefinition/**',
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
