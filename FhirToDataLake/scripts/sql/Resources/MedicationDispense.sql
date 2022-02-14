CREATE EXTERNAL TABLE [fhir].[MedicationDispense] (
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
    [partOf] VARCHAR(MAX),
    [status] NVARCHAR(100),
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
    [location.id] NVARCHAR(100),
    [location.extension] NVARCHAR(MAX),
    [location.reference] NVARCHAR(4000),
    [location.type] VARCHAR(256),
    [location.identifier.id] NVARCHAR(100),
    [location.identifier.extension] NVARCHAR(MAX),
    [location.identifier.use] NVARCHAR(64),
    [location.identifier.type] NVARCHAR(MAX),
    [location.identifier.system] VARCHAR(256),
    [location.identifier.value] NVARCHAR(4000),
    [location.identifier.period] NVARCHAR(MAX),
    [location.identifier.assigner] NVARCHAR(MAX),
    [location.display] NVARCHAR(4000),
    [authorizingPrescription] VARCHAR(MAX),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [quantity.id] NVARCHAR(100),
    [quantity.extension] NVARCHAR(MAX),
    [quantity.value] float,
    [quantity.comparator] NVARCHAR(64),
    [quantity.unit] NVARCHAR(100),
    [quantity.system] VARCHAR(256),
    [quantity.code] NVARCHAR(4000),
    [daysSupply.id] NVARCHAR(100),
    [daysSupply.extension] NVARCHAR(MAX),
    [daysSupply.value] float,
    [daysSupply.comparator] NVARCHAR(64),
    [daysSupply.unit] NVARCHAR(100),
    [daysSupply.system] VARCHAR(256),
    [daysSupply.code] NVARCHAR(4000),
    [whenPrepared] VARCHAR(64),
    [whenHandedOver] VARCHAR(64),
    [destination.id] NVARCHAR(100),
    [destination.extension] NVARCHAR(MAX),
    [destination.reference] NVARCHAR(4000),
    [destination.type] VARCHAR(256),
    [destination.identifier.id] NVARCHAR(100),
    [destination.identifier.extension] NVARCHAR(MAX),
    [destination.identifier.use] NVARCHAR(64),
    [destination.identifier.type] NVARCHAR(MAX),
    [destination.identifier.system] VARCHAR(256),
    [destination.identifier.value] NVARCHAR(4000),
    [destination.identifier.period] NVARCHAR(MAX),
    [destination.identifier.assigner] NVARCHAR(MAX),
    [destination.display] NVARCHAR(4000),
    [receiver] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [dosageInstruction] VARCHAR(MAX),
    [substitution.id] NVARCHAR(100),
    [substitution.extension] NVARCHAR(MAX),
    [substitution.modifierExtension] NVARCHAR(MAX),
    [substitution.wasSubstituted] bit,
    [substitution.type.id] NVARCHAR(100),
    [substitution.type.extension] NVARCHAR(MAX),
    [substitution.type.coding] NVARCHAR(MAX),
    [substitution.type.text] NVARCHAR(4000),
    [substitution.reason] VARCHAR(MAX),
    [substitution.responsibleParty] VARCHAR(MAX),
    [detectedIssue] VARCHAR(MAX),
    [eventHistory] VARCHAR(MAX),
    [statusReason.codeableConcept.id] NVARCHAR(100),
    [statusReason.codeableConcept.extension] NVARCHAR(MAX),
    [statusReason.codeableConcept.coding] VARCHAR(MAX),
    [statusReason.codeableConcept.text] NVARCHAR(4000),
    [statusReason.reference.id] NVARCHAR(100),
    [statusReason.reference.extension] NVARCHAR(MAX),
    [statusReason.reference.reference] NVARCHAR(4000),
    [statusReason.reference.type] VARCHAR(256),
    [statusReason.reference.identifier.id] NVARCHAR(100),
    [statusReason.reference.identifier.extension] NVARCHAR(MAX),
    [statusReason.reference.identifier.use] NVARCHAR(64),
    [statusReason.reference.identifier.type] NVARCHAR(MAX),
    [statusReason.reference.identifier.system] VARCHAR(256),
    [statusReason.reference.identifier.value] NVARCHAR(4000),
    [statusReason.reference.identifier.period] NVARCHAR(MAX),
    [statusReason.reference.identifier.assigner] NVARCHAR(MAX),
    [statusReason.reference.display] NVARCHAR(4000),
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
) WITH (
    LOCATION='/MedicationDispense/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicationDispenseIdentifier AS
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
        BULK 'MedicationDispense/**',
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

CREATE VIEW fhir.MedicationDispensePartOf AS
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
        BULK 'MedicationDispense/**',
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

CREATE VIEW fhir.MedicationDispenseSupportingInformation AS
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
        BULK 'MedicationDispense/**',
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

CREATE VIEW fhir.MedicationDispensePerformer AS
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
        BULK 'MedicationDispense/**',
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

CREATE VIEW fhir.MedicationDispenseAuthorizingPrescription AS
SELECT
    [id],
    [authorizingPrescription.JSON],
    [authorizingPrescription.id],
    [authorizingPrescription.extension],
    [authorizingPrescription.reference],
    [authorizingPrescription.type],
    [authorizingPrescription.identifier.id],
    [authorizingPrescription.identifier.extension],
    [authorizingPrescription.identifier.use],
    [authorizingPrescription.identifier.type],
    [authorizingPrescription.identifier.system],
    [authorizingPrescription.identifier.value],
    [authorizingPrescription.identifier.period],
    [authorizingPrescription.identifier.assigner],
    [authorizingPrescription.display]
FROM openrowset (
        BULK 'MedicationDispense/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [authorizingPrescription.JSON]  VARCHAR(MAX) '$.authorizingPrescription'
    ) AS rowset
    CROSS APPLY openjson (rowset.[authorizingPrescription.JSON]) with (
        [authorizingPrescription.id]   NVARCHAR(100)       '$.id',
        [authorizingPrescription.extension] NVARCHAR(MAX)       '$.extension',
        [authorizingPrescription.reference] NVARCHAR(4000)      '$.reference',
        [authorizingPrescription.type] VARCHAR(256)        '$.type',
        [authorizingPrescription.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [authorizingPrescription.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [authorizingPrescription.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [authorizingPrescription.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [authorizingPrescription.identifier.system] VARCHAR(256)        '$.identifier.system',
        [authorizingPrescription.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [authorizingPrescription.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [authorizingPrescription.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [authorizingPrescription.display] NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicationDispenseReceiver AS
SELECT
    [id],
    [receiver.JSON],
    [receiver.id],
    [receiver.extension],
    [receiver.reference],
    [receiver.type],
    [receiver.identifier.id],
    [receiver.identifier.extension],
    [receiver.identifier.use],
    [receiver.identifier.type],
    [receiver.identifier.system],
    [receiver.identifier.value],
    [receiver.identifier.period],
    [receiver.identifier.assigner],
    [receiver.display]
FROM openrowset (
        BULK 'MedicationDispense/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [receiver.JSON]  VARCHAR(MAX) '$.receiver'
    ) AS rowset
    CROSS APPLY openjson (rowset.[receiver.JSON]) with (
        [receiver.id]                  NVARCHAR(100)       '$.id',
        [receiver.extension]           NVARCHAR(MAX)       '$.extension',
        [receiver.reference]           NVARCHAR(4000)      '$.reference',
        [receiver.type]                VARCHAR(256)        '$.type',
        [receiver.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [receiver.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [receiver.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [receiver.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [receiver.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [receiver.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [receiver.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [receiver.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [receiver.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicationDispenseNote AS
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
        BULK 'MedicationDispense/**',
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

CREATE VIEW fhir.MedicationDispenseDosageInstruction AS
SELECT
    [id],
    [dosageInstruction.JSON],
    [dosageInstruction.id],
    [dosageInstruction.extension],
    [dosageInstruction.modifierExtension],
    [dosageInstruction.sequence],
    [dosageInstruction.text],
    [dosageInstruction.additionalInstruction],
    [dosageInstruction.patientInstruction],
    [dosageInstruction.timing.id],
    [dosageInstruction.timing.extension],
    [dosageInstruction.timing.modifierExtension],
    [dosageInstruction.timing.event],
    [dosageInstruction.timing.repeat],
    [dosageInstruction.timing.code],
    [dosageInstruction.site.id],
    [dosageInstruction.site.extension],
    [dosageInstruction.site.coding],
    [dosageInstruction.site.text],
    [dosageInstruction.route.id],
    [dosageInstruction.route.extension],
    [dosageInstruction.route.coding],
    [dosageInstruction.route.text],
    [dosageInstruction.method.id],
    [dosageInstruction.method.extension],
    [dosageInstruction.method.coding],
    [dosageInstruction.method.text],
    [dosageInstruction.doseAndRate],
    [dosageInstruction.maxDosePerPeriod.id],
    [dosageInstruction.maxDosePerPeriod.extension],
    [dosageInstruction.maxDosePerPeriod.numerator],
    [dosageInstruction.maxDosePerPeriod.denominator],
    [dosageInstruction.maxDosePerAdministration.id],
    [dosageInstruction.maxDosePerAdministration.extension],
    [dosageInstruction.maxDosePerAdministration.value],
    [dosageInstruction.maxDosePerAdministration.comparator],
    [dosageInstruction.maxDosePerAdministration.unit],
    [dosageInstruction.maxDosePerAdministration.system],
    [dosageInstruction.maxDosePerAdministration.code],
    [dosageInstruction.maxDosePerLifetime.id],
    [dosageInstruction.maxDosePerLifetime.extension],
    [dosageInstruction.maxDosePerLifetime.value],
    [dosageInstruction.maxDosePerLifetime.comparator],
    [dosageInstruction.maxDosePerLifetime.unit],
    [dosageInstruction.maxDosePerLifetime.system],
    [dosageInstruction.maxDosePerLifetime.code],
    [dosageInstruction.asNeeded.boolean],
    [dosageInstruction.asNeeded.codeableConcept.id],
    [dosageInstruction.asNeeded.codeableConcept.extension],
    [dosageInstruction.asNeeded.codeableConcept.coding],
    [dosageInstruction.asNeeded.codeableConcept.text]
FROM openrowset (
        BULK 'MedicationDispense/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [dosageInstruction.JSON]  VARCHAR(MAX) '$.dosageInstruction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[dosageInstruction.JSON]) with (
        [dosageInstruction.id]         NVARCHAR(100)       '$.id',
        [dosageInstruction.extension]  NVARCHAR(MAX)       '$.extension',
        [dosageInstruction.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [dosageInstruction.sequence]   bigint              '$.sequence',
        [dosageInstruction.text]       NVARCHAR(4000)      '$.text',
        [dosageInstruction.additionalInstruction] NVARCHAR(MAX)       '$.additionalInstruction' AS JSON,
        [dosageInstruction.patientInstruction] NVARCHAR(4000)      '$.patientInstruction',
        [dosageInstruction.timing.id]  NVARCHAR(100)       '$.timing.id',
        [dosageInstruction.timing.extension] NVARCHAR(MAX)       '$.timing.extension',
        [dosageInstruction.timing.modifierExtension] NVARCHAR(MAX)       '$.timing.modifierExtension',
        [dosageInstruction.timing.event] NVARCHAR(MAX)       '$.timing.event',
        [dosageInstruction.timing.repeat] NVARCHAR(MAX)       '$.timing.repeat',
        [dosageInstruction.timing.code] NVARCHAR(MAX)       '$.timing.code',
        [dosageInstruction.site.id]    NVARCHAR(100)       '$.site.id',
        [dosageInstruction.site.extension] NVARCHAR(MAX)       '$.site.extension',
        [dosageInstruction.site.coding] NVARCHAR(MAX)       '$.site.coding',
        [dosageInstruction.site.text]  NVARCHAR(4000)      '$.site.text',
        [dosageInstruction.route.id]   NVARCHAR(100)       '$.route.id',
        [dosageInstruction.route.extension] NVARCHAR(MAX)       '$.route.extension',
        [dosageInstruction.route.coding] NVARCHAR(MAX)       '$.route.coding',
        [dosageInstruction.route.text] NVARCHAR(4000)      '$.route.text',
        [dosageInstruction.method.id]  NVARCHAR(100)       '$.method.id',
        [dosageInstruction.method.extension] NVARCHAR(MAX)       '$.method.extension',
        [dosageInstruction.method.coding] NVARCHAR(MAX)       '$.method.coding',
        [dosageInstruction.method.text] NVARCHAR(4000)      '$.method.text',
        [dosageInstruction.doseAndRate] NVARCHAR(MAX)       '$.doseAndRate' AS JSON,
        [dosageInstruction.maxDosePerPeriod.id] NVARCHAR(100)       '$.maxDosePerPeriod.id',
        [dosageInstruction.maxDosePerPeriod.extension] NVARCHAR(MAX)       '$.maxDosePerPeriod.extension',
        [dosageInstruction.maxDosePerPeriod.numerator] NVARCHAR(MAX)       '$.maxDosePerPeriod.numerator',
        [dosageInstruction.maxDosePerPeriod.denominator] NVARCHAR(MAX)       '$.maxDosePerPeriod.denominator',
        [dosageInstruction.maxDosePerAdministration.id] NVARCHAR(100)       '$.maxDosePerAdministration.id',
        [dosageInstruction.maxDosePerAdministration.extension] NVARCHAR(MAX)       '$.maxDosePerAdministration.extension',
        [dosageInstruction.maxDosePerAdministration.value] float               '$.maxDosePerAdministration.value',
        [dosageInstruction.maxDosePerAdministration.comparator] NVARCHAR(64)        '$.maxDosePerAdministration.comparator',
        [dosageInstruction.maxDosePerAdministration.unit] NVARCHAR(100)       '$.maxDosePerAdministration.unit',
        [dosageInstruction.maxDosePerAdministration.system] VARCHAR(256)        '$.maxDosePerAdministration.system',
        [dosageInstruction.maxDosePerAdministration.code] NVARCHAR(4000)      '$.maxDosePerAdministration.code',
        [dosageInstruction.maxDosePerLifetime.id] NVARCHAR(100)       '$.maxDosePerLifetime.id',
        [dosageInstruction.maxDosePerLifetime.extension] NVARCHAR(MAX)       '$.maxDosePerLifetime.extension',
        [dosageInstruction.maxDosePerLifetime.value] float               '$.maxDosePerLifetime.value',
        [dosageInstruction.maxDosePerLifetime.comparator] NVARCHAR(64)        '$.maxDosePerLifetime.comparator',
        [dosageInstruction.maxDosePerLifetime.unit] NVARCHAR(100)       '$.maxDosePerLifetime.unit',
        [dosageInstruction.maxDosePerLifetime.system] VARCHAR(256)        '$.maxDosePerLifetime.system',
        [dosageInstruction.maxDosePerLifetime.code] NVARCHAR(4000)      '$.maxDosePerLifetime.code',
        [dosageInstruction.asNeeded.boolean] bit                 '$.asNeeded.boolean',
        [dosageInstruction.asNeeded.codeableConcept.id] NVARCHAR(100)       '$.asNeeded.codeableConcept.id',
        [dosageInstruction.asNeeded.codeableConcept.extension] NVARCHAR(MAX)       '$.asNeeded.codeableConcept.extension',
        [dosageInstruction.asNeeded.codeableConcept.coding] NVARCHAR(MAX)       '$.asNeeded.codeableConcept.coding',
        [dosageInstruction.asNeeded.codeableConcept.text] NVARCHAR(4000)      '$.asNeeded.codeableConcept.text'
    ) j

GO

CREATE VIEW fhir.MedicationDispenseDetectedIssue AS
SELECT
    [id],
    [detectedIssue.JSON],
    [detectedIssue.id],
    [detectedIssue.extension],
    [detectedIssue.reference],
    [detectedIssue.type],
    [detectedIssue.identifier.id],
    [detectedIssue.identifier.extension],
    [detectedIssue.identifier.use],
    [detectedIssue.identifier.type],
    [detectedIssue.identifier.system],
    [detectedIssue.identifier.value],
    [detectedIssue.identifier.period],
    [detectedIssue.identifier.assigner],
    [detectedIssue.display]
FROM openrowset (
        BULK 'MedicationDispense/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [detectedIssue.JSON]  VARCHAR(MAX) '$.detectedIssue'
    ) AS rowset
    CROSS APPLY openjson (rowset.[detectedIssue.JSON]) with (
        [detectedIssue.id]             NVARCHAR(100)       '$.id',
        [detectedIssue.extension]      NVARCHAR(MAX)       '$.extension',
        [detectedIssue.reference]      NVARCHAR(4000)      '$.reference',
        [detectedIssue.type]           VARCHAR(256)        '$.type',
        [detectedIssue.identifier.id]  NVARCHAR(100)       '$.identifier.id',
        [detectedIssue.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [detectedIssue.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [detectedIssue.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [detectedIssue.identifier.system] VARCHAR(256)        '$.identifier.system',
        [detectedIssue.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [detectedIssue.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [detectedIssue.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [detectedIssue.display]        NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicationDispenseEventHistory AS
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
        BULK 'MedicationDispense/**',
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
