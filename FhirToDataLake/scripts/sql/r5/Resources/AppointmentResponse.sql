CREATE EXTERNAL TABLE [fhir].[AppointmentResponse] (
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
    [appointment.id] NVARCHAR(100),
    [appointment.extension] NVARCHAR(MAX),
    [appointment.reference] NVARCHAR(4000),
    [appointment.type] VARCHAR(256),
    [appointment.identifier.id] NVARCHAR(100),
    [appointment.identifier.extension] NVARCHAR(MAX),
    [appointment.identifier.use] NVARCHAR(64),
    [appointment.identifier.type] NVARCHAR(MAX),
    [appointment.identifier.system] VARCHAR(256),
    [appointment.identifier.value] NVARCHAR(4000),
    [appointment.identifier.period] NVARCHAR(MAX),
    [appointment.identifier.assigner] NVARCHAR(MAX),
    [appointment.display] NVARCHAR(4000),
    [start] VARCHAR(64),
    [end] VARCHAR(64),
    [participantType] VARCHAR(MAX),
    [actor.id] NVARCHAR(100),
    [actor.extension] NVARCHAR(MAX),
    [actor.reference] NVARCHAR(4000),
    [actor.type] VARCHAR(256),
    [actor.identifier.id] NVARCHAR(100),
    [actor.identifier.extension] NVARCHAR(MAX),
    [actor.identifier.use] NVARCHAR(64),
    [actor.identifier.type] NVARCHAR(MAX),
    [actor.identifier.system] VARCHAR(256),
    [actor.identifier.value] NVARCHAR(4000),
    [actor.identifier.period] NVARCHAR(MAX),
    [actor.identifier.assigner] NVARCHAR(MAX),
    [actor.display] NVARCHAR(4000),
    [participantStatus] NVARCHAR(100),
    [comment] NVARCHAR(4000),
) WITH (
    LOCATION='/AppointmentResponse/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.AppointmentResponseIdentifier AS
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
        BULK 'AppointmentResponse/**',
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

CREATE VIEW fhir.AppointmentResponseParticipantType AS
SELECT
    [id],
    [participantType.JSON],
    [participantType.id],
    [participantType.extension],
    [participantType.coding],
    [participantType.text]
FROM openrowset (
        BULK 'AppointmentResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [participantType.JSON]  VARCHAR(MAX) '$.participantType'
    ) AS rowset
    CROSS APPLY openjson (rowset.[participantType.JSON]) with (
        [participantType.id]           NVARCHAR(100)       '$.id',
        [participantType.extension]    NVARCHAR(MAX)       '$.extension',
        [participantType.coding]       NVARCHAR(MAX)       '$.coding' AS JSON,
        [participantType.text]         NVARCHAR(4000)      '$.text'
    ) j
