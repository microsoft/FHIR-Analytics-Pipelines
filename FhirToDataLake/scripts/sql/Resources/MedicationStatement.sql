CREATE EXTERNAL TABLE [fhir].[MedicationStatement] (
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
    [basedOn] VARCHAR(MAX),
    [partOf] VARCHAR(MAX),
    [status] NVARCHAR(100),
    [statusReason] VARCHAR(MAX),
    [category.id] NVARCHAR(100),
    [category.extension] NVARCHAR(MAX),
    [category.coding] VARCHAR(MAX),
    [category.text] NVARCHAR(4000),
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
    [context.id] NVARCHAR(100),
    [context.extension] NVARCHAR(MAX),
    [context.reference] NVARCHAR(4000),
    [context.type] VARCHAR(256),
    [context.identifier.id] NVARCHAR(100),
    [context.identifier.extension] NVARCHAR(MAX),
    [context.identifier.use] NVARCHAR(64),
    [context.identifier.type] NVARCHAR(MAX),
    [context.identifier.system] VARCHAR(256),
    [context.identifier.value] NVARCHAR(4000),
    [context.identifier.period] NVARCHAR(MAX),
    [context.identifier.assigner] NVARCHAR(MAX),
    [context.display] NVARCHAR(4000),
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
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [dosage] VARCHAR(MAX),
    [medication.codeableConcept.id] NVARCHAR(100),
    [medication.codeableConcept.extension] NVARCHAR(MAX),
    [medication.codeableConcept.coding] VARCHAR(MAX),
    [medication.codeableConcept.text] NVARCHAR(4000),
    [medication.reference.id] NVARCHAR(100),
    [medication.reference.extension] NVARCHAR(MAX),
    [medication.reference.reference] NVARCHAR(4000),
    [medication.reference.type] VARCHAR(256),
    [medication.reference.identifier.id] NVARCHAR(100),
    [medication.reference.identifier.extension] NVARCHAR(MAX),
    [medication.reference.identifier.use] NVARCHAR(64),
    [medication.reference.identifier.type] NVARCHAR(MAX),
    [medication.reference.identifier.system] VARCHAR(256),
    [medication.reference.identifier.value] NVARCHAR(4000),
    [medication.reference.identifier.period] NVARCHAR(MAX),
    [medication.reference.identifier.assigner] NVARCHAR(MAX),
    [medication.reference.display] NVARCHAR(4000),
    [effective.dateTime] VARCHAR(64),
    [effective.period.id] NVARCHAR(100),
    [effective.period.extension] NVARCHAR(MAX),
    [effective.period.start] VARCHAR(64),
    [effective.period.end] VARCHAR(64),
) WITH (
    LOCATION='/MedicationStatement/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicationStatementIdentifier AS
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
        BULK 'MedicationStatement/**',
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

CREATE VIEW fhir.MedicationStatementBasedOn AS
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
        BULK 'MedicationStatement/**',
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

CREATE VIEW fhir.MedicationStatementPartOf AS
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
        BULK 'MedicationStatement/**',
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

CREATE VIEW fhir.MedicationStatementStatusReason AS
SELECT
    [id],
    [statusReason.JSON],
    [statusReason.id],
    [statusReason.extension],
    [statusReason.coding],
    [statusReason.text]
FROM openrowset (
        BULK 'MedicationStatement/**',
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

CREATE VIEW fhir.MedicationStatementDerivedFrom AS
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
        BULK 'MedicationStatement/**',
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

CREATE VIEW fhir.MedicationStatementReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'MedicationStatement/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonCode.JSON]  VARCHAR(MAX) '$.reasonCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonCode.JSON]) with (
        [reasonCode.id]                NVARCHAR(100)       '$.id',
        [reasonCode.extension]         NVARCHAR(MAX)       '$.extension',
        [reasonCode.coding]            NVARCHAR(MAX)       '$.coding' AS JSON,
        [reasonCode.text]              NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.MedicationStatementReasonReference AS
SELECT
    [id],
    [reasonReference.JSON],
    [reasonReference.id],
    [reasonReference.extension],
    [reasonReference.reference],
    [reasonReference.type],
    [reasonReference.identifier.id],
    [reasonReference.identifier.extension],
    [reasonReference.identifier.use],
    [reasonReference.identifier.type],
    [reasonReference.identifier.system],
    [reasonReference.identifier.value],
    [reasonReference.identifier.period],
    [reasonReference.identifier.assigner],
    [reasonReference.display]
FROM openrowset (
        BULK 'MedicationStatement/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonReference.JSON]  VARCHAR(MAX) '$.reasonReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonReference.JSON]) with (
        [reasonReference.id]           NVARCHAR(100)       '$.id',
        [reasonReference.extension]    NVARCHAR(MAX)       '$.extension',
        [reasonReference.reference]    NVARCHAR(4000)      '$.reference',
        [reasonReference.type]         VARCHAR(256)        '$.type',
        [reasonReference.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [reasonReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [reasonReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [reasonReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [reasonReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [reasonReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [reasonReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [reasonReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [reasonReference.display]      NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicationStatementNote AS
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
        BULK 'MedicationStatement/**',
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

CREATE VIEW fhir.MedicationStatementDosage AS
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
        BULK 'MedicationStatement/**',
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
