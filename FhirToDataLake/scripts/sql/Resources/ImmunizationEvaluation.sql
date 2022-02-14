CREATE EXTERNAL TABLE [fhir].[ImmunizationEvaluation] (
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
    [patient.id] NVARCHAR(100),
    [patient.extension] NVARCHAR(MAX),
    [patient.reference] NVARCHAR(4000),
    [patient.type] VARCHAR(256),
    [patient.identifier.id] NVARCHAR(100),
    [patient.identifier.extension] NVARCHAR(MAX),
    [patient.identifier.use] NVARCHAR(64),
    [patient.identifier.type] NVARCHAR(MAX),
    [patient.identifier.system] VARCHAR(256),
    [patient.identifier.value] NVARCHAR(4000),
    [patient.identifier.period] NVARCHAR(MAX),
    [patient.identifier.assigner] NVARCHAR(MAX),
    [patient.display] NVARCHAR(4000),
    [date] VARCHAR(64),
    [authority.id] NVARCHAR(100),
    [authority.extension] NVARCHAR(MAX),
    [authority.reference] NVARCHAR(4000),
    [authority.type] VARCHAR(256),
    [authority.identifier.id] NVARCHAR(100),
    [authority.identifier.extension] NVARCHAR(MAX),
    [authority.identifier.use] NVARCHAR(64),
    [authority.identifier.type] NVARCHAR(MAX),
    [authority.identifier.system] VARCHAR(256),
    [authority.identifier.value] NVARCHAR(4000),
    [authority.identifier.period] NVARCHAR(MAX),
    [authority.identifier.assigner] NVARCHAR(MAX),
    [authority.display] NVARCHAR(4000),
    [targetDisease.id] NVARCHAR(100),
    [targetDisease.extension] NVARCHAR(MAX),
    [targetDisease.coding] VARCHAR(MAX),
    [targetDisease.text] NVARCHAR(4000),
    [immunizationEvent.id] NVARCHAR(100),
    [immunizationEvent.extension] NVARCHAR(MAX),
    [immunizationEvent.reference] NVARCHAR(4000),
    [immunizationEvent.type] VARCHAR(256),
    [immunizationEvent.identifier.id] NVARCHAR(100),
    [immunizationEvent.identifier.extension] NVARCHAR(MAX),
    [immunizationEvent.identifier.use] NVARCHAR(64),
    [immunizationEvent.identifier.type] NVARCHAR(MAX),
    [immunizationEvent.identifier.system] VARCHAR(256),
    [immunizationEvent.identifier.value] NVARCHAR(4000),
    [immunizationEvent.identifier.period] NVARCHAR(MAX),
    [immunizationEvent.identifier.assigner] NVARCHAR(MAX),
    [immunizationEvent.display] NVARCHAR(4000),
    [doseStatus.id] NVARCHAR(100),
    [doseStatus.extension] NVARCHAR(MAX),
    [doseStatus.coding] VARCHAR(MAX),
    [doseStatus.text] NVARCHAR(4000),
    [doseStatusReason] VARCHAR(MAX),
    [description] NVARCHAR(4000),
    [series] NVARCHAR(4000),
    [doseNumber.positiveInt] bigint,
    [doseNumber.string] NVARCHAR(4000),
    [seriesDoses.positiveInt] bigint,
    [seriesDoses.string] NVARCHAR(4000),
) WITH (
    LOCATION='/ImmunizationEvaluation/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ImmunizationEvaluationIdentifier AS
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
        BULK 'ImmunizationEvaluation/**',
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

CREATE VIEW fhir.ImmunizationEvaluationDoseStatusReason AS
SELECT
    [id],
    [doseStatusReason.JSON],
    [doseStatusReason.id],
    [doseStatusReason.extension],
    [doseStatusReason.coding],
    [doseStatusReason.text]
FROM openrowset (
        BULK 'ImmunizationEvaluation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [doseStatusReason.JSON]  VARCHAR(MAX) '$.doseStatusReason'
    ) AS rowset
    CROSS APPLY openjson (rowset.[doseStatusReason.JSON]) with (
        [doseStatusReason.id]          NVARCHAR(100)       '$.id',
        [doseStatusReason.extension]   NVARCHAR(MAX)       '$.extension',
        [doseStatusReason.coding]      NVARCHAR(MAX)       '$.coding' AS JSON,
        [doseStatusReason.text]        NVARCHAR(4000)      '$.text'
    ) j
