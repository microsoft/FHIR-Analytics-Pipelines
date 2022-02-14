CREATE EXTERNAL TABLE [fhir].[Encounter] (
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
    [status] NVARCHAR(64),
    [statusHistory] VARCHAR(MAX),
    [class.id] NVARCHAR(100),
    [class.extension] NVARCHAR(MAX),
    [class.system] VARCHAR(256),
    [class.version] NVARCHAR(100),
    [class.code] NVARCHAR(4000),
    [class.display] NVARCHAR(4000),
    [class.userSelected] bit,
    [classHistory] VARCHAR(MAX),
    [type] VARCHAR(MAX),
    [serviceType.id] NVARCHAR(100),
    [serviceType.extension] NVARCHAR(MAX),
    [serviceType.coding] VARCHAR(MAX),
    [serviceType.text] NVARCHAR(4000),
    [priority.id] NVARCHAR(100),
    [priority.extension] NVARCHAR(MAX),
    [priority.coding] VARCHAR(MAX),
    [priority.text] NVARCHAR(4000),
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
    [episodeOfCare] VARCHAR(MAX),
    [basedOn] VARCHAR(MAX),
    [participant] VARCHAR(MAX),
    [appointment] VARCHAR(MAX),
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [length.id] NVARCHAR(100),
    [length.extension] NVARCHAR(MAX),
    [length.value] float,
    [length.comparator] NVARCHAR(64),
    [length.unit] NVARCHAR(100),
    [length.system] VARCHAR(256),
    [length.code] NVARCHAR(4000),
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [diagnosis] VARCHAR(MAX),
    [account] VARCHAR(MAX),
    [hospitalization.id] NVARCHAR(100),
    [hospitalization.extension] NVARCHAR(MAX),
    [hospitalization.modifierExtension] NVARCHAR(MAX),
    [hospitalization.preAdmissionIdentifier.id] NVARCHAR(100),
    [hospitalization.preAdmissionIdentifier.extension] NVARCHAR(MAX),
    [hospitalization.preAdmissionIdentifier.use] NVARCHAR(64),
    [hospitalization.preAdmissionIdentifier.type] NVARCHAR(MAX),
    [hospitalization.preAdmissionIdentifier.system] VARCHAR(256),
    [hospitalization.preAdmissionIdentifier.value] NVARCHAR(4000),
    [hospitalization.preAdmissionIdentifier.period] NVARCHAR(MAX),
    [hospitalization.preAdmissionIdentifier.assigner] NVARCHAR(MAX),
    [hospitalization.origin.id] NVARCHAR(100),
    [hospitalization.origin.extension] NVARCHAR(MAX),
    [hospitalization.origin.reference] NVARCHAR(4000),
    [hospitalization.origin.type] VARCHAR(256),
    [hospitalization.origin.identifier] NVARCHAR(MAX),
    [hospitalization.origin.display] NVARCHAR(4000),
    [hospitalization.admitSource.id] NVARCHAR(100),
    [hospitalization.admitSource.extension] NVARCHAR(MAX),
    [hospitalization.admitSource.coding] NVARCHAR(MAX),
    [hospitalization.admitSource.text] NVARCHAR(4000),
    [hospitalization.reAdmission.id] NVARCHAR(100),
    [hospitalization.reAdmission.extension] NVARCHAR(MAX),
    [hospitalization.reAdmission.coding] NVARCHAR(MAX),
    [hospitalization.reAdmission.text] NVARCHAR(4000),
    [hospitalization.dietPreference] VARCHAR(MAX),
    [hospitalization.specialCourtesy] VARCHAR(MAX),
    [hospitalization.specialArrangement] VARCHAR(MAX),
    [hospitalization.destination.id] NVARCHAR(100),
    [hospitalization.destination.extension] NVARCHAR(MAX),
    [hospitalization.destination.reference] NVARCHAR(4000),
    [hospitalization.destination.type] VARCHAR(256),
    [hospitalization.destination.identifier] NVARCHAR(MAX),
    [hospitalization.destination.display] NVARCHAR(4000),
    [hospitalization.dischargeDisposition.id] NVARCHAR(100),
    [hospitalization.dischargeDisposition.extension] NVARCHAR(MAX),
    [hospitalization.dischargeDisposition.coding] NVARCHAR(MAX),
    [hospitalization.dischargeDisposition.text] NVARCHAR(4000),
    [location] VARCHAR(MAX),
    [serviceProvider.id] NVARCHAR(100),
    [serviceProvider.extension] NVARCHAR(MAX),
    [serviceProvider.reference] NVARCHAR(4000),
    [serviceProvider.type] VARCHAR(256),
    [serviceProvider.identifier.id] NVARCHAR(100),
    [serviceProvider.identifier.extension] NVARCHAR(MAX),
    [serviceProvider.identifier.use] NVARCHAR(64),
    [serviceProvider.identifier.type] NVARCHAR(MAX),
    [serviceProvider.identifier.system] VARCHAR(256),
    [serviceProvider.identifier.value] NVARCHAR(4000),
    [serviceProvider.identifier.period] NVARCHAR(MAX),
    [serviceProvider.identifier.assigner] NVARCHAR(MAX),
    [serviceProvider.display] NVARCHAR(4000),
    [partOf.id] NVARCHAR(100),
    [partOf.extension] NVARCHAR(MAX),
    [partOf.reference] NVARCHAR(4000),
    [partOf.type] VARCHAR(256),
    [partOf.identifier.id] NVARCHAR(100),
    [partOf.identifier.extension] NVARCHAR(MAX),
    [partOf.identifier.use] NVARCHAR(64),
    [partOf.identifier.type] NVARCHAR(MAX),
    [partOf.identifier.system] VARCHAR(256),
    [partOf.identifier.value] NVARCHAR(4000),
    [partOf.identifier.period] NVARCHAR(MAX),
    [partOf.identifier.assigner] NVARCHAR(MAX),
    [partOf.display] NVARCHAR(4000),
) WITH (
    LOCATION='/Encounter/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.EncounterIdentifier AS
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
        BULK 'Encounter/**',
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

CREATE VIEW fhir.EncounterStatusHistory AS
SELECT
    [id],
    [statusHistory.JSON],
    [statusHistory.id],
    [statusHistory.extension],
    [statusHistory.modifierExtension],
    [statusHistory.status],
    [statusHistory.period.id],
    [statusHistory.period.extension],
    [statusHistory.period.start],
    [statusHistory.period.end]
FROM openrowset (
        BULK 'Encounter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [statusHistory.JSON]  VARCHAR(MAX) '$.statusHistory'
    ) AS rowset
    CROSS APPLY openjson (rowset.[statusHistory.JSON]) with (
        [statusHistory.id]             NVARCHAR(100)       '$.id',
        [statusHistory.extension]      NVARCHAR(MAX)       '$.extension',
        [statusHistory.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [statusHistory.status]         NVARCHAR(64)        '$.status',
        [statusHistory.period.id]      NVARCHAR(100)       '$.period.id',
        [statusHistory.period.extension] NVARCHAR(MAX)       '$.period.extension',
        [statusHistory.period.start]   VARCHAR(64)         '$.period.start',
        [statusHistory.period.end]     VARCHAR(64)         '$.period.end'
    ) j

GO

CREATE VIEW fhir.EncounterClassHistory AS
SELECT
    [id],
    [classHistory.JSON],
    [classHistory.id],
    [classHistory.extension],
    [classHistory.modifierExtension],
    [classHistory.class.id],
    [classHistory.class.extension],
    [classHistory.class.system],
    [classHistory.class.version],
    [classHistory.class.code],
    [classHistory.class.display],
    [classHistory.class.userSelected],
    [classHistory.period.id],
    [classHistory.period.extension],
    [classHistory.period.start],
    [classHistory.period.end]
FROM openrowset (
        BULK 'Encounter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [classHistory.JSON]  VARCHAR(MAX) '$.classHistory'
    ) AS rowset
    CROSS APPLY openjson (rowset.[classHistory.JSON]) with (
        [classHistory.id]              NVARCHAR(100)       '$.id',
        [classHistory.extension]       NVARCHAR(MAX)       '$.extension',
        [classHistory.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [classHistory.class.id]        NVARCHAR(100)       '$.class.id',
        [classHistory.class.extension] NVARCHAR(MAX)       '$.class.extension',
        [classHistory.class.system]    VARCHAR(256)        '$.class.system',
        [classHistory.class.version]   NVARCHAR(100)       '$.class.version',
        [classHistory.class.code]      NVARCHAR(4000)      '$.class.code',
        [classHistory.class.display]   NVARCHAR(4000)      '$.class.display',
        [classHistory.class.userSelected] bit                 '$.class.userSelected',
        [classHistory.period.id]       NVARCHAR(100)       '$.period.id',
        [classHistory.period.extension] NVARCHAR(MAX)       '$.period.extension',
        [classHistory.period.start]    VARCHAR(64)         '$.period.start',
        [classHistory.period.end]      VARCHAR(64)         '$.period.end'
    ) j

GO

CREATE VIEW fhir.EncounterType AS
SELECT
    [id],
    [type.JSON],
    [type.id],
    [type.extension],
    [type.coding],
    [type.text]
FROM openrowset (
        BULK 'Encounter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [type.JSON]  VARCHAR(MAX) '$.type'
    ) AS rowset
    CROSS APPLY openjson (rowset.[type.JSON]) with (
        [type.id]                      NVARCHAR(100)       '$.id',
        [type.extension]               NVARCHAR(MAX)       '$.extension',
        [type.coding]                  NVARCHAR(MAX)       '$.coding' AS JSON,
        [type.text]                    NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.EncounterEpisodeOfCare AS
SELECT
    [id],
    [episodeOfCare.JSON],
    [episodeOfCare.id],
    [episodeOfCare.extension],
    [episodeOfCare.reference],
    [episodeOfCare.type],
    [episodeOfCare.identifier.id],
    [episodeOfCare.identifier.extension],
    [episodeOfCare.identifier.use],
    [episodeOfCare.identifier.type],
    [episodeOfCare.identifier.system],
    [episodeOfCare.identifier.value],
    [episodeOfCare.identifier.period],
    [episodeOfCare.identifier.assigner],
    [episodeOfCare.display]
FROM openrowset (
        BULK 'Encounter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [episodeOfCare.JSON]  VARCHAR(MAX) '$.episodeOfCare'
    ) AS rowset
    CROSS APPLY openjson (rowset.[episodeOfCare.JSON]) with (
        [episodeOfCare.id]             NVARCHAR(100)       '$.id',
        [episodeOfCare.extension]      NVARCHAR(MAX)       '$.extension',
        [episodeOfCare.reference]      NVARCHAR(4000)      '$.reference',
        [episodeOfCare.type]           VARCHAR(256)        '$.type',
        [episodeOfCare.identifier.id]  NVARCHAR(100)       '$.identifier.id',
        [episodeOfCare.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [episodeOfCare.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [episodeOfCare.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [episodeOfCare.identifier.system] VARCHAR(256)        '$.identifier.system',
        [episodeOfCare.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [episodeOfCare.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [episodeOfCare.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [episodeOfCare.display]        NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.EncounterBasedOn AS
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
        BULK 'Encounter/**',
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

CREATE VIEW fhir.EncounterParticipant AS
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
    [participant.individual.id],
    [participant.individual.extension],
    [participant.individual.reference],
    [participant.individual.type],
    [participant.individual.identifier],
    [participant.individual.display]
FROM openrowset (
        BULK 'Encounter/**',
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
        [participant.individual.id]    NVARCHAR(100)       '$.individual.id',
        [participant.individual.extension] NVARCHAR(MAX)       '$.individual.extension',
        [participant.individual.reference] NVARCHAR(4000)      '$.individual.reference',
        [participant.individual.type]  VARCHAR(256)        '$.individual.type',
        [participant.individual.identifier] NVARCHAR(MAX)       '$.individual.identifier',
        [participant.individual.display] NVARCHAR(4000)      '$.individual.display'
    ) j

GO

CREATE VIEW fhir.EncounterAppointment AS
SELECT
    [id],
    [appointment.JSON],
    [appointment.id],
    [appointment.extension],
    [appointment.reference],
    [appointment.type],
    [appointment.identifier.id],
    [appointment.identifier.extension],
    [appointment.identifier.use],
    [appointment.identifier.type],
    [appointment.identifier.system],
    [appointment.identifier.value],
    [appointment.identifier.period],
    [appointment.identifier.assigner],
    [appointment.display]
FROM openrowset (
        BULK 'Encounter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [appointment.JSON]  VARCHAR(MAX) '$.appointment'
    ) AS rowset
    CROSS APPLY openjson (rowset.[appointment.JSON]) with (
        [appointment.id]               NVARCHAR(100)       '$.id',
        [appointment.extension]        NVARCHAR(MAX)       '$.extension',
        [appointment.reference]        NVARCHAR(4000)      '$.reference',
        [appointment.type]             VARCHAR(256)        '$.type',
        [appointment.identifier.id]    NVARCHAR(100)       '$.identifier.id',
        [appointment.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [appointment.identifier.use]   NVARCHAR(64)        '$.identifier.use',
        [appointment.identifier.type]  NVARCHAR(MAX)       '$.identifier.type',
        [appointment.identifier.system] VARCHAR(256)        '$.identifier.system',
        [appointment.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [appointment.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [appointment.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [appointment.display]          NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.EncounterReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'Encounter/**',
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

CREATE VIEW fhir.EncounterReasonReference AS
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
        BULK 'Encounter/**',
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

CREATE VIEW fhir.EncounterDiagnosis AS
SELECT
    [id],
    [diagnosis.JSON],
    [diagnosis.id],
    [diagnosis.extension],
    [diagnosis.modifierExtension],
    [diagnosis.condition.id],
    [diagnosis.condition.extension],
    [diagnosis.condition.reference],
    [diagnosis.condition.type],
    [diagnosis.condition.identifier],
    [diagnosis.condition.display],
    [diagnosis.use.id],
    [diagnosis.use.extension],
    [diagnosis.use.coding],
    [diagnosis.use.text],
    [diagnosis.rank]
FROM openrowset (
        BULK 'Encounter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [diagnosis.JSON]  VARCHAR(MAX) '$.diagnosis'
    ) AS rowset
    CROSS APPLY openjson (rowset.[diagnosis.JSON]) with (
        [diagnosis.id]                 NVARCHAR(100)       '$.id',
        [diagnosis.extension]          NVARCHAR(MAX)       '$.extension',
        [diagnosis.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [diagnosis.condition.id]       NVARCHAR(100)       '$.condition.id',
        [diagnosis.condition.extension] NVARCHAR(MAX)       '$.condition.extension',
        [diagnosis.condition.reference] NVARCHAR(4000)      '$.condition.reference',
        [diagnosis.condition.type]     VARCHAR(256)        '$.condition.type',
        [diagnosis.condition.identifier] NVARCHAR(MAX)       '$.condition.identifier',
        [diagnosis.condition.display]  NVARCHAR(4000)      '$.condition.display',
        [diagnosis.use.id]             NVARCHAR(100)       '$.use.id',
        [diagnosis.use.extension]      NVARCHAR(MAX)       '$.use.extension',
        [diagnosis.use.coding]         NVARCHAR(MAX)       '$.use.coding',
        [diagnosis.use.text]           NVARCHAR(4000)      '$.use.text',
        [diagnosis.rank]               bigint              '$.rank'
    ) j

GO

CREATE VIEW fhir.EncounterAccount AS
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
        BULK 'Encounter/**',
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

CREATE VIEW fhir.EncounterLocation AS
SELECT
    [id],
    [location.JSON],
    [location.id],
    [location.extension],
    [location.modifierExtension],
    [location.location.id],
    [location.location.extension],
    [location.location.reference],
    [location.location.type],
    [location.location.identifier],
    [location.location.display],
    [location.status],
    [location.physicalType.id],
    [location.physicalType.extension],
    [location.physicalType.coding],
    [location.physicalType.text],
    [location.period.id],
    [location.period.extension],
    [location.period.start],
    [location.period.end]
FROM openrowset (
        BULK 'Encounter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [location.JSON]  VARCHAR(MAX) '$.location'
    ) AS rowset
    CROSS APPLY openjson (rowset.[location.JSON]) with (
        [location.id]                  NVARCHAR(100)       '$.id',
        [location.extension]           NVARCHAR(MAX)       '$.extension',
        [location.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [location.location.id]         NVARCHAR(100)       '$.location.id',
        [location.location.extension]  NVARCHAR(MAX)       '$.location.extension',
        [location.location.reference]  NVARCHAR(4000)      '$.location.reference',
        [location.location.type]       VARCHAR(256)        '$.location.type',
        [location.location.identifier] NVARCHAR(MAX)       '$.location.identifier',
        [location.location.display]    NVARCHAR(4000)      '$.location.display',
        [location.status]              NVARCHAR(64)        '$.status',
        [location.physicalType.id]     NVARCHAR(100)       '$.physicalType.id',
        [location.physicalType.extension] NVARCHAR(MAX)       '$.physicalType.extension',
        [location.physicalType.coding] NVARCHAR(MAX)       '$.physicalType.coding',
        [location.physicalType.text]   NVARCHAR(4000)      '$.physicalType.text',
        [location.period.id]           NVARCHAR(100)       '$.period.id',
        [location.period.extension]    NVARCHAR(MAX)       '$.period.extension',
        [location.period.start]        VARCHAR(64)         '$.period.start',
        [location.period.end]          VARCHAR(64)         '$.period.end'
    ) j
