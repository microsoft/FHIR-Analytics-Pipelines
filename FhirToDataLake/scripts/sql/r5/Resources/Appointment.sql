CREATE EXTERNAL TABLE [fhir].[Appointment] (
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
    [cancellationReason.id] NVARCHAR(100),
    [cancellationReason.extension] NVARCHAR(MAX),
    [cancellationReason.coding] VARCHAR(MAX),
    [cancellationReason.text] NVARCHAR(4000),
    [serviceCategory] VARCHAR(MAX),
    [serviceType] VARCHAR(MAX),
    [specialty] VARCHAR(MAX),
    [appointmentType.id] NVARCHAR(100),
    [appointmentType.extension] NVARCHAR(MAX),
    [appointmentType.coding] VARCHAR(MAX),
    [appointmentType.text] NVARCHAR(4000),
    [reason] VARCHAR(MAX),
    [priority.id] NVARCHAR(100),
    [priority.extension] NVARCHAR(MAX),
    [priority.coding] VARCHAR(MAX),
    [priority.text] NVARCHAR(4000),
    [description] NVARCHAR(4000),
    [replaces] VARCHAR(MAX),
    [supportingInformation] VARCHAR(MAX),
    [start] VARCHAR(64),
    [end] VARCHAR(64),
    [minutesDuration] bigint,
    [slot] VARCHAR(MAX),
    [account] VARCHAR(MAX),
    [created] VARCHAR(64),
    [note] VARCHAR(MAX),
    [patientInstruction] VARCHAR(MAX),
    [basedOn] VARCHAR(MAX),
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
    [participant] VARCHAR(MAX),
    [requestedPeriod] VARCHAR(MAX),
) WITH (
    LOCATION='/Appointment/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.AppointmentIdentifier AS
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
        BULK 'Appointment/**',
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

CREATE VIEW fhir.AppointmentServiceCategory AS
SELECT
    [id],
    [serviceCategory.JSON],
    [serviceCategory.id],
    [serviceCategory.extension],
    [serviceCategory.coding],
    [serviceCategory.text]
FROM openrowset (
        BULK 'Appointment/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [serviceCategory.JSON]  VARCHAR(MAX) '$.serviceCategory'
    ) AS rowset
    CROSS APPLY openjson (rowset.[serviceCategory.JSON]) with (
        [serviceCategory.id]           NVARCHAR(100)       '$.id',
        [serviceCategory.extension]    NVARCHAR(MAX)       '$.extension',
        [serviceCategory.coding]       NVARCHAR(MAX)       '$.coding' AS JSON,
        [serviceCategory.text]         NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.AppointmentServiceType AS
SELECT
    [id],
    [serviceType.JSON],
    [serviceType.id],
    [serviceType.extension],
    [serviceType.coding],
    [serviceType.text]
FROM openrowset (
        BULK 'Appointment/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [serviceType.JSON]  VARCHAR(MAX) '$.serviceType'
    ) AS rowset
    CROSS APPLY openjson (rowset.[serviceType.JSON]) with (
        [serviceType.id]               NVARCHAR(100)       '$.id',
        [serviceType.extension]        NVARCHAR(MAX)       '$.extension',
        [serviceType.coding]           NVARCHAR(MAX)       '$.coding' AS JSON,
        [serviceType.text]             NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.AppointmentSpecialty AS
SELECT
    [id],
    [specialty.JSON],
    [specialty.id],
    [specialty.extension],
    [specialty.coding],
    [specialty.text]
FROM openrowset (
        BULK 'Appointment/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specialty.JSON]  VARCHAR(MAX) '$.specialty'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specialty.JSON]) with (
        [specialty.id]                 NVARCHAR(100)       '$.id',
        [specialty.extension]          NVARCHAR(MAX)       '$.extension',
        [specialty.coding]             NVARCHAR(MAX)       '$.coding' AS JSON,
        [specialty.text]               NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.AppointmentReason AS
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
        BULK 'Appointment/**',
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

CREATE VIEW fhir.AppointmentReplaces AS
SELECT
    [id],
    [replaces.JSON],
    [replaces.id],
    [replaces.extension],
    [replaces.reference],
    [replaces.type],
    [replaces.identifier.id],
    [replaces.identifier.extension],
    [replaces.identifier.use],
    [replaces.identifier.type],
    [replaces.identifier.system],
    [replaces.identifier.value],
    [replaces.identifier.period],
    [replaces.identifier.assigner],
    [replaces.display]
FROM openrowset (
        BULK 'Appointment/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [replaces.JSON]  VARCHAR(MAX) '$.replaces'
    ) AS rowset
    CROSS APPLY openjson (rowset.[replaces.JSON]) with (
        [replaces.id]                  NVARCHAR(100)       '$.id',
        [replaces.extension]           NVARCHAR(MAX)       '$.extension',
        [replaces.reference]           NVARCHAR(4000)      '$.reference',
        [replaces.type]                VARCHAR(256)        '$.type',
        [replaces.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [replaces.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [replaces.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [replaces.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [replaces.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [replaces.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [replaces.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [replaces.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [replaces.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.AppointmentSupportingInformation AS
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
        BULK 'Appointment/**',
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

CREATE VIEW fhir.AppointmentSlot AS
SELECT
    [id],
    [slot.JSON],
    [slot.id],
    [slot.extension],
    [slot.reference],
    [slot.type],
    [slot.identifier.id],
    [slot.identifier.extension],
    [slot.identifier.use],
    [slot.identifier.type],
    [slot.identifier.system],
    [slot.identifier.value],
    [slot.identifier.period],
    [slot.identifier.assigner],
    [slot.display]
FROM openrowset (
        BULK 'Appointment/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [slot.JSON]  VARCHAR(MAX) '$.slot'
    ) AS rowset
    CROSS APPLY openjson (rowset.[slot.JSON]) with (
        [slot.id]                      NVARCHAR(100)       '$.id',
        [slot.extension]               NVARCHAR(MAX)       '$.extension',
        [slot.reference]               NVARCHAR(4000)      '$.reference',
        [slot.type]                    VARCHAR(256)        '$.type',
        [slot.identifier.id]           NVARCHAR(100)       '$.identifier.id',
        [slot.identifier.extension]    NVARCHAR(MAX)       '$.identifier.extension',
        [slot.identifier.use]          NVARCHAR(64)        '$.identifier.use',
        [slot.identifier.type]         NVARCHAR(MAX)       '$.identifier.type',
        [slot.identifier.system]       VARCHAR(256)        '$.identifier.system',
        [slot.identifier.value]        NVARCHAR(4000)      '$.identifier.value',
        [slot.identifier.period]       NVARCHAR(MAX)       '$.identifier.period',
        [slot.identifier.assigner]     NVARCHAR(MAX)       '$.identifier.assigner',
        [slot.display]                 NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.AppointmentAccount AS
SELECT
    [id],
    [account.JSON],
    [account.id],
    [account.extension],
    [account.reference],
    [account.type],
    [account.identifier.id],
    [account.identifier.extension],
    [account.identifier.use],
    [account.identifier.type],
    [account.identifier.system],
    [account.identifier.value],
    [account.identifier.period],
    [account.identifier.assigner],
    [account.display]
FROM openrowset (
        BULK 'Appointment/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [account.JSON]  VARCHAR(MAX) '$.account'
    ) AS rowset
    CROSS APPLY openjson (rowset.[account.JSON]) with (
        [account.id]                   NVARCHAR(100)       '$.id',
        [account.extension]            NVARCHAR(MAX)       '$.extension',
        [account.reference]            NVARCHAR(4000)      '$.reference',
        [account.type]                 VARCHAR(256)        '$.type',
        [account.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [account.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [account.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [account.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [account.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [account.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [account.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [account.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [account.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.AppointmentNote AS
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
        BULK 'Appointment/**',
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

CREATE VIEW fhir.AppointmentPatientInstruction AS
SELECT
    [id],
    [patientInstruction.JSON],
    [patientInstruction.id],
    [patientInstruction.extension],
    [patientInstruction.concept.id],
    [patientInstruction.concept.extension],
    [patientInstruction.concept.coding],
    [patientInstruction.concept.text],
    [patientInstruction.reference.id],
    [patientInstruction.reference.extension],
    [patientInstruction.reference.reference],
    [patientInstruction.reference.type],
    [patientInstruction.reference.identifier],
    [patientInstruction.reference.display]
FROM openrowset (
        BULK 'Appointment/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [patientInstruction.JSON]  VARCHAR(MAX) '$.patientInstruction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[patientInstruction.JSON]) with (
        [patientInstruction.id]        NVARCHAR(100)       '$.id',
        [patientInstruction.extension] NVARCHAR(MAX)       '$.extension',
        [patientInstruction.concept.id] NVARCHAR(100)       '$.concept.id',
        [patientInstruction.concept.extension] NVARCHAR(MAX)       '$.concept.extension',
        [patientInstruction.concept.coding] NVARCHAR(MAX)       '$.concept.coding',
        [patientInstruction.concept.text] NVARCHAR(4000)      '$.concept.text',
        [patientInstruction.reference.id] NVARCHAR(100)       '$.reference.id',
        [patientInstruction.reference.extension] NVARCHAR(MAX)       '$.reference.extension',
        [patientInstruction.reference.reference] NVARCHAR(4000)      '$.reference.reference',
        [patientInstruction.reference.type] VARCHAR(256)        '$.reference.type',
        [patientInstruction.reference.identifier] NVARCHAR(MAX)       '$.reference.identifier',
        [patientInstruction.reference.display] NVARCHAR(4000)      '$.reference.display'
    ) j

GO

CREATE VIEW fhir.AppointmentBasedOn AS
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
        BULK 'Appointment/**',
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

CREATE VIEW fhir.AppointmentParticipant AS
SELECT
    [id],
    [participant.JSON],
    [participant.id],
    [participant.extension],
    [participant.modifierExtension],
    [participant.type],
    [participant.period.id],
    [participant.period.extension],
    [participant.period.start],
    [participant.period.end],
    [participant.actor.id],
    [participant.actor.extension],
    [participant.actor.reference],
    [participant.actor.type],
    [participant.actor.identifier],
    [participant.actor.display],
    [participant.required],
    [participant.status]
FROM openrowset (
        BULK 'Appointment/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [participant.JSON]  VARCHAR(MAX) '$.participant'
    ) AS rowset
    CROSS APPLY openjson (rowset.[participant.JSON]) with (
        [participant.id]               NVARCHAR(100)       '$.id',
        [participant.extension]        NVARCHAR(MAX)       '$.extension',
        [participant.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [participant.type]             NVARCHAR(MAX)       '$.type' AS JSON,
        [participant.period.id]        NVARCHAR(100)       '$.period.id',
        [participant.period.extension] NVARCHAR(MAX)       '$.period.extension',
        [participant.period.start]     VARCHAR(64)         '$.period.start',
        [participant.period.end]       VARCHAR(64)         '$.period.end',
        [participant.actor.id]         NVARCHAR(100)       '$.actor.id',
        [participant.actor.extension]  NVARCHAR(MAX)       '$.actor.extension',
        [participant.actor.reference]  NVARCHAR(4000)      '$.actor.reference',
        [participant.actor.type]       VARCHAR(256)        '$.actor.type',
        [participant.actor.identifier] NVARCHAR(MAX)       '$.actor.identifier',
        [participant.actor.display]    NVARCHAR(4000)      '$.actor.display',
        [participant.required]         bit                 '$.required',
        [participant.status]           NVARCHAR(100)       '$.status'
    ) j

GO

CREATE VIEW fhir.AppointmentRequestedPeriod AS
SELECT
    [id],
    [requestedPeriod.JSON],
    [requestedPeriod.id],
    [requestedPeriod.extension],
    [requestedPeriod.start],
    [requestedPeriod.end]
FROM openrowset (
        BULK 'Appointment/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [requestedPeriod.JSON]  VARCHAR(MAX) '$.requestedPeriod'
    ) AS rowset
    CROSS APPLY openjson (rowset.[requestedPeriod.JSON]) with (
        [requestedPeriod.id]           NVARCHAR(100)       '$.id',
        [requestedPeriod.extension]    NVARCHAR(MAX)       '$.extension',
        [requestedPeriod.start]        VARCHAR(64)         '$.start',
        [requestedPeriod.end]          VARCHAR(64)         '$.end'
    ) j
