CREATE EXTERNAL TABLE [fhir].[MedicationUsage] (
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
    [category] VARCHAR(MAX),
    [medication.id] NVARCHAR(100),
    [medication.extension] NVARCHAR(MAX),
    [medication.concept.id] NVARCHAR(100),
    [medication.concept.extension] NVARCHAR(MAX),
    [medication.concept.coding] NVARCHAR(MAX),
    [medication.concept.text] NVARCHAR(4000),
    [medication.reference.id] NVARCHAR(100),
    [medication.reference.extension] NVARCHAR(MAX),
    [medication.reference.reference] NVARCHAR(4000),
    [medication.reference.type] VARCHAR(256),
    [medication.reference.identifier] NVARCHAR(MAX),
    [medication.reference.display] NVARCHAR(4000),
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
    [dateAsserted] VARCHAR(64),
    [informationSource.id] NVARCHAR(100),
    [informationSource.extension] NVARCHAR(MAX),
    [informationSource.reference] NVARCHAR(4000),
    [informationSource.type] VARCHAR(256),
    [informationSource.identifier.id] NVARCHAR(100),
    [informationSource.identifier.extension] NVARCHAR(MAX),
    [informationSource.identifier.use] NVARCHAR(64),
    [informationSource.identifier.type] NVARCHAR(MAX),
    [informationSource.identifier.system] VARCHAR(256),
    [informationSource.identifier.value] NVARCHAR(4000),
    [informationSource.identifier.period] NVARCHAR(MAX),
    [informationSource.identifier.assigner] NVARCHAR(MAX),
    [informationSource.display] NVARCHAR(4000),
    [derivedFrom] VARCHAR(MAX),
    [reason] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [renderedDosageInstruction] NVARCHAR(4000),
    [dosage] VARCHAR(MAX),
    [adherence.id] NVARCHAR(100),
    [adherence.extension] NVARCHAR(MAX),
    [adherence.modifierExtension] NVARCHAR(MAX),
    [adherence.code.id] NVARCHAR(100),
    [adherence.code.extension] NVARCHAR(MAX),
    [adherence.code.coding] NVARCHAR(MAX),
    [adherence.code.text] NVARCHAR(4000),
    [adherence.reason.id] NVARCHAR(100),
    [adherence.reason.extension] NVARCHAR(MAX),
    [adherence.reason.coding] NVARCHAR(MAX),
    [adherence.reason.text] NVARCHAR(4000),
    [effective.dateTime] VARCHAR(64),
    [effective.period.id] NVARCHAR(100),
    [effective.period.extension] NVARCHAR(MAX),
    [effective.period.start] VARCHAR(64),
    [effective.period.end] VARCHAR(64),
) WITH (
    LOCATION='/MedicationUsage/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicationUsageIdentifier AS
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
        BULK 'MedicationUsage/**',
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

CREATE VIEW fhir.MedicationUsageCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'MedicationUsage/**',
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

CREATE VIEW fhir.MedicationUsageDerivedFrom AS
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
        BULK 'MedicationUsage/**',
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

CREATE VIEW fhir.MedicationUsageReason AS
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
        BULK 'MedicationUsage/**',
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

CREATE VIEW fhir.MedicationUsageNote AS
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
        BULK 'MedicationUsage/**',
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

GO

CREATE VIEW fhir.MedicationUsageDosage AS
SELECT
    [id],
    [dosage.JSON],
    [dosage.id],
    [dosage.extension],
    [dosage.modifierExtension],
    [dosage.sequence],
    [dosage.text],
    [dosage.additionalInstruction],
    [dosage.patientInstruction],
    [dosage.timing.id],
    [dosage.timing.extension],
    [dosage.timing.modifierExtension],
    [dosage.timing.event],
    [dosage.timing.repeat],
    [dosage.timing.code],
    [dosage.site.id],
    [dosage.site.extension],
    [dosage.site.coding],
    [dosage.site.text],
    [dosage.route.id],
    [dosage.route.extension],
    [dosage.route.coding],
    [dosage.route.text],
    [dosage.method.id],
    [dosage.method.extension],
    [dosage.method.coding],
    [dosage.method.text],
    [dosage.doseAndRate],
    [dosage.maxDosePerPeriod.id],
    [dosage.maxDosePerPeriod.extension],
    [dosage.maxDosePerPeriod.numerator],
    [dosage.maxDosePerPeriod.denominator],
    [dosage.maxDosePerAdministration.id],
    [dosage.maxDosePerAdministration.extension],
    [dosage.maxDosePerAdministration.value],
    [dosage.maxDosePerAdministration.comparator],
    [dosage.maxDosePerAdministration.unit],
    [dosage.maxDosePerAdministration.system],
    [dosage.maxDosePerAdministration.code],
    [dosage.maxDosePerLifetime.id],
    [dosage.maxDosePerLifetime.extension],
    [dosage.maxDosePerLifetime.value],
    [dosage.maxDosePerLifetime.comparator],
    [dosage.maxDosePerLifetime.unit],
    [dosage.maxDosePerLifetime.system],
    [dosage.maxDosePerLifetime.code],
    [dosage.asNeeded.boolean],
    [dosage.asNeeded.codeableConcept.id],
    [dosage.asNeeded.codeableConcept.extension],
    [dosage.asNeeded.codeableConcept.coding],
    [dosage.asNeeded.codeableConcept.text]
FROM openrowset (
        BULK 'MedicationUsage/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [dosage.JSON]  VARCHAR(MAX) '$.dosage'
    ) AS rowset
    CROSS APPLY openjson (rowset.[dosage.JSON]) with (
        [dosage.id]                    NVARCHAR(100)       '$.id',
        [dosage.extension]             NVARCHAR(MAX)       '$.extension',
        [dosage.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [dosage.sequence]              bigint              '$.sequence',
        [dosage.text]                  NVARCHAR(4000)      '$.text',
        [dosage.additionalInstruction] NVARCHAR(MAX)       '$.additionalInstruction' AS JSON,
        [dosage.patientInstruction]    NVARCHAR(4000)      '$.patientInstruction',
        [dosage.timing.id]             NVARCHAR(100)       '$.timing.id',
        [dosage.timing.extension]      NVARCHAR(MAX)       '$.timing.extension',
        [dosage.timing.modifierExtension] NVARCHAR(MAX)       '$.timing.modifierExtension',
        [dosage.timing.event]          NVARCHAR(MAX)       '$.timing.event',
        [dosage.timing.repeat]         NVARCHAR(MAX)       '$.timing.repeat',
        [dosage.timing.code]           NVARCHAR(MAX)       '$.timing.code',
        [dosage.site.id]               NVARCHAR(100)       '$.site.id',
        [dosage.site.extension]        NVARCHAR(MAX)       '$.site.extension',
        [dosage.site.coding]           NVARCHAR(MAX)       '$.site.coding',
        [dosage.site.text]             NVARCHAR(4000)      '$.site.text',
        [dosage.route.id]              NVARCHAR(100)       '$.route.id',
        [dosage.route.extension]       NVARCHAR(MAX)       '$.route.extension',
        [dosage.route.coding]          NVARCHAR(MAX)       '$.route.coding',
        [dosage.route.text]            NVARCHAR(4000)      '$.route.text',
        [dosage.method.id]             NVARCHAR(100)       '$.method.id',
        [dosage.method.extension]      NVARCHAR(MAX)       '$.method.extension',
        [dosage.method.coding]         NVARCHAR(MAX)       '$.method.coding',
        [dosage.method.text]           NVARCHAR(4000)      '$.method.text',
        [dosage.doseAndRate]           NVARCHAR(MAX)       '$.doseAndRate' AS JSON,
        [dosage.maxDosePerPeriod.id]   NVARCHAR(100)       '$.maxDosePerPeriod.id',
        [dosage.maxDosePerPeriod.extension] NVARCHAR(MAX)       '$.maxDosePerPeriod.extension',
        [dosage.maxDosePerPeriod.numerator] NVARCHAR(MAX)       '$.maxDosePerPeriod.numerator',
        [dosage.maxDosePerPeriod.denominator] NVARCHAR(MAX)       '$.maxDosePerPeriod.denominator',
        [dosage.maxDosePerAdministration.id] NVARCHAR(100)       '$.maxDosePerAdministration.id',
        [dosage.maxDosePerAdministration.extension] NVARCHAR(MAX)       '$.maxDosePerAdministration.extension',
        [dosage.maxDosePerAdministration.value] float               '$.maxDosePerAdministration.value',
        [dosage.maxDosePerAdministration.comparator] NVARCHAR(64)        '$.maxDosePerAdministration.comparator',
        [dosage.maxDosePerAdministration.unit] NVARCHAR(100)       '$.maxDosePerAdministration.unit',
        [dosage.maxDosePerAdministration.system] VARCHAR(256)        '$.maxDosePerAdministration.system',
        [dosage.maxDosePerAdministration.code] NVARCHAR(4000)      '$.maxDosePerAdministration.code',
        [dosage.maxDosePerLifetime.id] NVARCHAR(100)       '$.maxDosePerLifetime.id',
        [dosage.maxDosePerLifetime.extension] NVARCHAR(MAX)       '$.maxDosePerLifetime.extension',
        [dosage.maxDosePerLifetime.value] float               '$.maxDosePerLifetime.value',
        [dosage.maxDosePerLifetime.comparator] NVARCHAR(64)        '$.maxDosePerLifetime.comparator',
        [dosage.maxDosePerLifetime.unit] NVARCHAR(100)       '$.maxDosePerLifetime.unit',
        [dosage.maxDosePerLifetime.system] VARCHAR(256)        '$.maxDosePerLifetime.system',
        [dosage.maxDosePerLifetime.code] NVARCHAR(4000)      '$.maxDosePerLifetime.code',
        [dosage.asNeeded.boolean]      bit                 '$.asNeeded.boolean',
        [dosage.asNeeded.codeableConcept.id] NVARCHAR(100)       '$.asNeeded.codeableConcept.id',
        [dosage.asNeeded.codeableConcept.extension] NVARCHAR(MAX)       '$.asNeeded.codeableConcept.extension',
        [dosage.asNeeded.codeableConcept.coding] NVARCHAR(MAX)       '$.asNeeded.codeableConcept.coding',
        [dosage.asNeeded.codeableConcept.text] NVARCHAR(4000)      '$.asNeeded.codeableConcept.text'
    ) j
