CREATE EXTERNAL TABLE [fhir].[MedicationRequest] (
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
    [statusReason.id] NVARCHAR(100),
    [statusReason.extension] NVARCHAR(MAX),
    [statusReason.coding] VARCHAR(MAX),
    [statusReason.text] NVARCHAR(4000),
    [intent] NVARCHAR(100),
    [category] VARCHAR(MAX),
    [priority] NVARCHAR(100),
    [doNotPerform] bit,
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
    [authoredOn] VARCHAR(64),
    [requester.id] NVARCHAR(100),
    [requester.extension] NVARCHAR(MAX),
    [requester.reference] NVARCHAR(4000),
    [requester.type] VARCHAR(256),
    [requester.identifier.id] NVARCHAR(100),
    [requester.identifier.extension] NVARCHAR(MAX),
    [requester.identifier.use] NVARCHAR(64),
    [requester.identifier.type] NVARCHAR(MAX),
    [requester.identifier.system] VARCHAR(256),
    [requester.identifier.value] NVARCHAR(4000),
    [requester.identifier.period] NVARCHAR(MAX),
    [requester.identifier.assigner] NVARCHAR(MAX),
    [requester.display] NVARCHAR(4000),
    [performer.id] NVARCHAR(100),
    [performer.extension] NVARCHAR(MAX),
    [performer.reference] NVARCHAR(4000),
    [performer.type] VARCHAR(256),
    [performer.identifier.id] NVARCHAR(100),
    [performer.identifier.extension] NVARCHAR(MAX),
    [performer.identifier.use] NVARCHAR(64),
    [performer.identifier.type] NVARCHAR(MAX),
    [performer.identifier.system] VARCHAR(256),
    [performer.identifier.value] NVARCHAR(4000),
    [performer.identifier.period] NVARCHAR(MAX),
    [performer.identifier.assigner] NVARCHAR(MAX),
    [performer.display] NVARCHAR(4000),
    [performerType.id] NVARCHAR(100),
    [performerType.extension] NVARCHAR(MAX),
    [performerType.coding] VARCHAR(MAX),
    [performerType.text] NVARCHAR(4000),
    [recorder.id] NVARCHAR(100),
    [recorder.extension] NVARCHAR(MAX),
    [recorder.reference] NVARCHAR(4000),
    [recorder.type] VARCHAR(256),
    [recorder.identifier.id] NVARCHAR(100),
    [recorder.identifier.extension] NVARCHAR(MAX),
    [recorder.identifier.use] NVARCHAR(64),
    [recorder.identifier.type] NVARCHAR(MAX),
    [recorder.identifier.system] VARCHAR(256),
    [recorder.identifier.value] NVARCHAR(4000),
    [recorder.identifier.period] NVARCHAR(MAX),
    [recorder.identifier.assigner] NVARCHAR(MAX),
    [recorder.display] NVARCHAR(4000),
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [instantiatesCanonical] VARCHAR(MAX),
    [instantiatesUri] VARCHAR(MAX),
    [basedOn] VARCHAR(MAX),
    [groupIdentifier.id] NVARCHAR(100),
    [groupIdentifier.extension] NVARCHAR(MAX),
    [groupIdentifier.use] NVARCHAR(64),
    [groupIdentifier.type.id] NVARCHAR(100),
    [groupIdentifier.type.extension] NVARCHAR(MAX),
    [groupIdentifier.type.coding] NVARCHAR(MAX),
    [groupIdentifier.type.text] NVARCHAR(4000),
    [groupIdentifier.system] VARCHAR(256),
    [groupIdentifier.value] NVARCHAR(4000),
    [groupIdentifier.period.id] NVARCHAR(100),
    [groupIdentifier.period.extension] NVARCHAR(MAX),
    [groupIdentifier.period.start] VARCHAR(64),
    [groupIdentifier.period.end] VARCHAR(64),
    [groupIdentifier.assigner.id] NVARCHAR(100),
    [groupIdentifier.assigner.extension] NVARCHAR(MAX),
    [groupIdentifier.assigner.reference] NVARCHAR(4000),
    [groupIdentifier.assigner.type] VARCHAR(256),
    [groupIdentifier.assigner.identifier] NVARCHAR(MAX),
    [groupIdentifier.assigner.display] NVARCHAR(4000),
    [courseOfTherapyType.id] NVARCHAR(100),
    [courseOfTherapyType.extension] NVARCHAR(MAX),
    [courseOfTherapyType.coding] VARCHAR(MAX),
    [courseOfTherapyType.text] NVARCHAR(4000),
    [insurance] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [dosageInstruction] VARCHAR(MAX),
    [dispenseRequest.id] NVARCHAR(100),
    [dispenseRequest.extension] NVARCHAR(MAX),
    [dispenseRequest.modifierExtension] NVARCHAR(MAX),
    [dispenseRequest.initialFill.id] NVARCHAR(100),
    [dispenseRequest.initialFill.extension] NVARCHAR(MAX),
    [dispenseRequest.initialFill.modifierExtension] NVARCHAR(MAX),
    [dispenseRequest.initialFill.quantity] NVARCHAR(MAX),
    [dispenseRequest.initialFill.duration] NVARCHAR(MAX),
    [dispenseRequest.dispenseInterval.id] NVARCHAR(100),
    [dispenseRequest.dispenseInterval.extension] NVARCHAR(MAX),
    [dispenseRequest.dispenseInterval.value] float,
    [dispenseRequest.dispenseInterval.comparator] NVARCHAR(64),
    [dispenseRequest.dispenseInterval.unit] NVARCHAR(100),
    [dispenseRequest.dispenseInterval.system] VARCHAR(256),
    [dispenseRequest.dispenseInterval.code] NVARCHAR(4000),
    [dispenseRequest.validityPeriod.id] NVARCHAR(100),
    [dispenseRequest.validityPeriod.extension] NVARCHAR(MAX),
    [dispenseRequest.validityPeriod.start] VARCHAR(64),
    [dispenseRequest.validityPeriod.end] VARCHAR(64),
    [dispenseRequest.numberOfRepeatsAllowed] bigint,
    [dispenseRequest.quantity.id] NVARCHAR(100),
    [dispenseRequest.quantity.extension] NVARCHAR(MAX),
    [dispenseRequest.quantity.value] float,
    [dispenseRequest.quantity.comparator] NVARCHAR(64),
    [dispenseRequest.quantity.unit] NVARCHAR(100),
    [dispenseRequest.quantity.system] VARCHAR(256),
    [dispenseRequest.quantity.code] NVARCHAR(4000),
    [dispenseRequest.expectedSupplyDuration.id] NVARCHAR(100),
    [dispenseRequest.expectedSupplyDuration.extension] NVARCHAR(MAX),
    [dispenseRequest.expectedSupplyDuration.value] float,
    [dispenseRequest.expectedSupplyDuration.comparator] NVARCHAR(64),
    [dispenseRequest.expectedSupplyDuration.unit] NVARCHAR(100),
    [dispenseRequest.expectedSupplyDuration.system] VARCHAR(256),
    [dispenseRequest.expectedSupplyDuration.code] NVARCHAR(4000),
    [dispenseRequest.performer.id] NVARCHAR(100),
    [dispenseRequest.performer.extension] NVARCHAR(MAX),
    [dispenseRequest.performer.reference] NVARCHAR(4000),
    [dispenseRequest.performer.type] VARCHAR(256),
    [dispenseRequest.performer.identifier] NVARCHAR(MAX),
    [dispenseRequest.performer.display] NVARCHAR(4000),
    [substitution.id] NVARCHAR(100),
    [substitution.extension] NVARCHAR(MAX),
    [substitution.modifierExtension] NVARCHAR(MAX),
    [substitution.reason.id] NVARCHAR(100),
    [substitution.reason.extension] NVARCHAR(MAX),
    [substitution.reason.coding] NVARCHAR(MAX),
    [substitution.reason.text] NVARCHAR(4000),
    [substitution.allowed.boolean] bit,
    [substitution.allowed.codeableConcept.id] NVARCHAR(100),
    [substitution.allowed.codeableConcept.extension] NVARCHAR(MAX),
    [substitution.allowed.codeableConcept.coding] NVARCHAR(MAX),
    [substitution.allowed.codeableConcept.text] NVARCHAR(4000),
    [priorPrescription.id] NVARCHAR(100),
    [priorPrescription.extension] NVARCHAR(MAX),
    [priorPrescription.reference] NVARCHAR(4000),
    [priorPrescription.type] VARCHAR(256),
    [priorPrescription.identifier.id] NVARCHAR(100),
    [priorPrescription.identifier.extension] NVARCHAR(MAX),
    [priorPrescription.identifier.use] NVARCHAR(64),
    [priorPrescription.identifier.type] NVARCHAR(MAX),
    [priorPrescription.identifier.system] VARCHAR(256),
    [priorPrescription.identifier.value] NVARCHAR(4000),
    [priorPrescription.identifier.period] NVARCHAR(MAX),
    [priorPrescription.identifier.assigner] NVARCHAR(MAX),
    [priorPrescription.display] NVARCHAR(4000),
    [detectedIssue] VARCHAR(MAX),
    [eventHistory] VARCHAR(MAX),
    [reported.boolean] bit,
    [reported.reference.id] NVARCHAR(100),
    [reported.reference.extension] NVARCHAR(MAX),
    [reported.reference.reference] NVARCHAR(4000),
    [reported.reference.type] VARCHAR(256),
    [reported.reference.identifier.id] NVARCHAR(100),
    [reported.reference.identifier.extension] NVARCHAR(MAX),
    [reported.reference.identifier.use] NVARCHAR(64),
    [reported.reference.identifier.type] NVARCHAR(MAX),
    [reported.reference.identifier.system] VARCHAR(256),
    [reported.reference.identifier.value] NVARCHAR(4000),
    [reported.reference.identifier.period] NVARCHAR(MAX),
    [reported.reference.identifier.assigner] NVARCHAR(MAX),
    [reported.reference.display] NVARCHAR(4000),
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
    LOCATION='/MedicationRequest/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicationRequestIdentifier AS
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
        BULK 'MedicationRequest/**',
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

CREATE VIEW fhir.MedicationRequestCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'MedicationRequest/**',
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

CREATE VIEW fhir.MedicationRequestSupportingInformation AS
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
        BULK 'MedicationRequest/**',
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

CREATE VIEW fhir.MedicationRequestReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'MedicationRequest/**',
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

CREATE VIEW fhir.MedicationRequestReasonReference AS
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
        BULK 'MedicationRequest/**',
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

CREATE VIEW fhir.MedicationRequestInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'MedicationRequest/**',
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

CREATE VIEW fhir.MedicationRequestInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'MedicationRequest/**',
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

CREATE VIEW fhir.MedicationRequestBasedOn AS
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
        BULK 'MedicationRequest/**',
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

CREATE VIEW fhir.MedicationRequestInsurance AS
SELECT
    [id],
    [insurance.JSON],
    [insurance.id],
    [insurance.extension],
    [insurance.reference],
    [insurance.type],
    [insurance.identifier.id],
    [insurance.identifier.extension],
    [insurance.identifier.use],
    [insurance.identifier.type],
    [insurance.identifier.system],
    [insurance.identifier.value],
    [insurance.identifier.period],
    [insurance.identifier.assigner],
    [insurance.display]
FROM openrowset (
        BULK 'MedicationRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [insurance.JSON]  VARCHAR(MAX) '$.insurance'
    ) AS rowset
    CROSS APPLY openjson (rowset.[insurance.JSON]) with (
        [insurance.id]                 NVARCHAR(100)       '$.id',
        [insurance.extension]          NVARCHAR(MAX)       '$.extension',
        [insurance.reference]          NVARCHAR(4000)      '$.reference',
        [insurance.type]               VARCHAR(256)        '$.type',
        [insurance.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [insurance.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [insurance.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [insurance.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [insurance.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [insurance.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [insurance.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [insurance.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [insurance.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicationRequestNote AS
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
        BULK 'MedicationRequest/**',
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

CREATE VIEW fhir.MedicationRequestDosageInstruction AS
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
        BULK 'MedicationRequest/**',
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

CREATE VIEW fhir.MedicationRequestDetectedIssue AS
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
        BULK 'MedicationRequest/**',
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

CREATE VIEW fhir.MedicationRequestEventHistory AS
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
        BULK 'MedicationRequest/**',
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
