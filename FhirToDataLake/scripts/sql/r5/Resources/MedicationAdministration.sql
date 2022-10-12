CREATE EXTERNAL TABLE [fhir].[MedicationAdministration] (
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
    [supportingInformation] VARCHAR(MAX),
    [recorded] VARCHAR(64),
    [performer] VARCHAR(MAX),
    [reason] VARCHAR(MAX),
    [request.id] NVARCHAR(100),
    [request.extension] NVARCHAR(MAX),
    [request.reference] NVARCHAR(4000),
    [request.type] VARCHAR(256),
    [request.identifier.id] NVARCHAR(100),
    [request.identifier.extension] NVARCHAR(MAX),
    [request.identifier.use] NVARCHAR(64),
    [request.identifier.type] NVARCHAR(MAX),
    [request.identifier.system] VARCHAR(256),
    [request.identifier.value] NVARCHAR(4000),
    [request.identifier.period] NVARCHAR(MAX),
    [request.identifier.assigner] NVARCHAR(MAX),
    [request.display] NVARCHAR(4000),
    [device] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [dosage.id] NVARCHAR(100),
    [dosage.extension] NVARCHAR(MAX),
    [dosage.modifierExtension] NVARCHAR(MAX),
    [dosage.text] NVARCHAR(4000),
    [dosage.site.id] NVARCHAR(100),
    [dosage.site.extension] NVARCHAR(MAX),
    [dosage.site.coding] NVARCHAR(MAX),
    [dosage.site.text] NVARCHAR(4000),
    [dosage.route.id] NVARCHAR(100),
    [dosage.route.extension] NVARCHAR(MAX),
    [dosage.route.coding] NVARCHAR(MAX),
    [dosage.route.text] NVARCHAR(4000),
    [dosage.method.id] NVARCHAR(100),
    [dosage.method.extension] NVARCHAR(MAX),
    [dosage.method.coding] NVARCHAR(MAX),
    [dosage.method.text] NVARCHAR(4000),
    [dosage.dose.id] NVARCHAR(100),
    [dosage.dose.extension] NVARCHAR(MAX),
    [dosage.dose.value] float,
    [dosage.dose.comparator] NVARCHAR(64),
    [dosage.dose.unit] NVARCHAR(100),
    [dosage.dose.system] VARCHAR(256),
    [dosage.dose.code] NVARCHAR(4000),
    [dosage.rateQuantity.id] NVARCHAR(100),
    [dosage.rateQuantity.extension] NVARCHAR(MAX),
    [dosage.rateQuantity.value] float,
    [dosage.rateQuantity.comparator] NVARCHAR(64),
    [dosage.rateQuantity.unit] NVARCHAR(100),
    [dosage.rateQuantity.system] VARCHAR(256),
    [dosage.rateQuantity.code] NVARCHAR(4000),
    [dosage.rate.ratio.id] NVARCHAR(100),
    [dosage.rate.ratio.extension] NVARCHAR(MAX),
    [dosage.rate.ratio.numerator] NVARCHAR(MAX),
    [dosage.rate.ratio.denominator] NVARCHAR(MAX),
    [dosage.rate.quantity.id] NVARCHAR(100),
    [dosage.rate.quantity.extension] NVARCHAR(MAX),
    [dosage.rate.quantity.value] float,
    [dosage.rate.quantity.comparator] NVARCHAR(64),
    [dosage.rate.quantity.unit] NVARCHAR(100),
    [dosage.rate.quantity.system] VARCHAR(256),
    [dosage.rate.quantity.code] NVARCHAR(4000),
    [eventHistory] VARCHAR(MAX),
    [occurence.dateTime] VARCHAR(64),
    [occurence.period.id] NVARCHAR(100),
    [occurence.period.extension] NVARCHAR(MAX),
    [occurence.period.start] VARCHAR(64),
    [occurence.period.end] VARCHAR(64),
) WITH (
    LOCATION='/MedicationAdministration/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicationAdministrationIdentifier AS
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
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationBasedOn AS
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
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationPartOf AS
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
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationStatusReason AS
SELECT
    [id],
    [statusReason.JSON],
    [statusReason.id],
    [statusReason.extension],
    [statusReason.coding],
    [statusReason.text]
FROM openrowset (
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationSupportingInformation AS
SELECT
    [id],
    [supportingInformation.JSON],
    [supportingInformation.id],
    [supportingInformation.extension],
    [supportingInformation.reference],
    [supportingInformation.type],
    [supportingInformation.identifier.id],
    [supportingInformation.identifier.extension],
    [supportingInformation.identifier.use],
    [supportingInformation.identifier.type],
    [supportingInformation.identifier.system],
    [supportingInformation.identifier.value],
    [supportingInformation.identifier.period],
    [supportingInformation.identifier.assigner],
    [supportingInformation.display]
FROM openrowset (
        BULK 'MedicationAdministration/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [supportingInformation.JSON]  VARCHAR(MAX) '$.supportingInformation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[supportingInformation.JSON]) with (
        [supportingInformation.id]     NVARCHAR(100)       '$.id',
        [supportingInformation.extension] NVARCHAR(MAX)       '$.extension',
        [supportingInformation.reference] NVARCHAR(4000)      '$.reference',
        [supportingInformation.type]   VARCHAR(256)        '$.type',
        [supportingInformation.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [supportingInformation.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [supportingInformation.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [supportingInformation.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [supportingInformation.identifier.system] VARCHAR(256)        '$.identifier.system',
        [supportingInformation.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [supportingInformation.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [supportingInformation.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [supportingInformation.display] NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicationAdministrationPerformer AS
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
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationReason AS
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
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationDevice AS
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
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationNote AS
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
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationEventHistory AS
SELECT
    [id],
    [eventHistory.JSON],
    [eventHistory.id],
    [eventHistory.extension],
    [eventHistory.reference],
    [eventHistory.type],
    [eventHistory.identifier.id],
    [eventHistory.identifier.extension],
    [eventHistory.identifier.use],
    [eventHistory.identifier.type],
    [eventHistory.identifier.system],
    [eventHistory.identifier.value],
    [eventHistory.identifier.period],
    [eventHistory.identifier.assigner],
    [eventHistory.display]
FROM openrowset (
        BULK 'MedicationAdministration/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [eventHistory.JSON]  VARCHAR(MAX) '$.eventHistory'
    ) AS rowset
    CROSS APPLY openjson (rowset.[eventHistory.JSON]) with (
        [eventHistory.id]              NVARCHAR(100)       '$.id',
        [eventHistory.extension]       NVARCHAR(MAX)       '$.extension',
        [eventHistory.reference]       NVARCHAR(4000)      '$.reference',
        [eventHistory.type]            VARCHAR(256)        '$.type',
        [eventHistory.identifier.id]   NVARCHAR(100)       '$.identifier.id',
        [eventHistory.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [eventHistory.identifier.use]  NVARCHAR(64)        '$.identifier.use',
        [eventHistory.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [eventHistory.identifier.system] VARCHAR(256)        '$.identifier.system',
        [eventHistory.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [eventHistory.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [eventHistory.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [eventHistory.display]         NVARCHAR(4000)      '$.display'
    ) j
