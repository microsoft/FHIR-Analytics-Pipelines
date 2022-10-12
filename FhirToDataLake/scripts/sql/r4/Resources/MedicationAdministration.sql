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
    [instantiates] VARCHAR(MAX),
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
    [supportingInformation] VARCHAR(MAX),
    [performer] VARCHAR(MAX),
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
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

CREATE VIEW fhir.MedicationAdministrationInstantiates AS
SELECT
    [id],
    [instantiates.JSON],
    [instantiates]
FROM openrowset (
        BULK 'MedicationAdministration/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiates.JSON]  VARCHAR(MAX) '$.instantiates'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiates.JSON]) with (
        [instantiates]                 NVARCHAR(MAX)       '$'
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

CREATE VIEW fhir.MedicationAdministrationReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'MedicationAdministration/**',
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

CREATE VIEW fhir.MedicationAdministrationReasonReference AS
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
        BULK 'MedicationAdministration/**',
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
