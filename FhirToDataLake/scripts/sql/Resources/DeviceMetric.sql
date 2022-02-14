CREATE EXTERNAL TABLE [fhir].[DeviceMetric] (
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
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [unit.id] NVARCHAR(100),
    [unit.extension] NVARCHAR(MAX),
    [unit.coding] VARCHAR(MAX),
    [unit.text] NVARCHAR(4000),
    [source.id] NVARCHAR(100),
    [source.extension] NVARCHAR(MAX),
    [source.reference] NVARCHAR(4000),
    [source.type] VARCHAR(256),
    [source.identifier.id] NVARCHAR(100),
    [source.identifier.extension] NVARCHAR(MAX),
    [source.identifier.use] NVARCHAR(64),
    [source.identifier.type] NVARCHAR(MAX),
    [source.identifier.system] VARCHAR(256),
    [source.identifier.value] NVARCHAR(4000),
    [source.identifier.period] NVARCHAR(MAX),
    [source.identifier.assigner] NVARCHAR(MAX),
    [source.display] NVARCHAR(4000),
    [parent.id] NVARCHAR(100),
    [parent.extension] NVARCHAR(MAX),
    [parent.reference] NVARCHAR(4000),
    [parent.type] VARCHAR(256),
    [parent.identifier.id] NVARCHAR(100),
    [parent.identifier.extension] NVARCHAR(MAX),
    [parent.identifier.use] NVARCHAR(64),
    [parent.identifier.type] NVARCHAR(MAX),
    [parent.identifier.system] VARCHAR(256),
    [parent.identifier.value] NVARCHAR(4000),
    [parent.identifier.period] NVARCHAR(MAX),
    [parent.identifier.assigner] NVARCHAR(MAX),
    [parent.display] NVARCHAR(4000),
    [operationalStatus] NVARCHAR(64),
    [color] NVARCHAR(64),
    [category] NVARCHAR(64),
    [measurementPeriod.id] NVARCHAR(100),
    [measurementPeriod.extension] NVARCHAR(MAX),
    [measurementPeriod.modifierExtension] NVARCHAR(MAX),
    [measurementPeriod.event] VARCHAR(MAX),
    [measurementPeriod.repeat.id] NVARCHAR(100),
    [measurementPeriod.repeat.extension] NVARCHAR(MAX),
    [measurementPeriod.repeat.modifierExtension] NVARCHAR(MAX),
    [measurementPeriod.repeat.count] bigint,
    [measurementPeriod.repeat.countMax] bigint,
    [measurementPeriod.repeat.duration] float,
    [measurementPeriod.repeat.durationMax] float,
    [measurementPeriod.repeat.durationUnit] NVARCHAR(64),
    [measurementPeriod.repeat.frequency] bigint,
    [measurementPeriod.repeat.frequencyMax] bigint,
    [measurementPeriod.repeat.period] float,
    [measurementPeriod.repeat.periodMax] float,
    [measurementPeriod.repeat.periodUnit] NVARCHAR(64),
    [measurementPeriod.repeat.dayOfWeek] NVARCHAR(MAX),
    [measurementPeriod.repeat.timeOfDay] NVARCHAR(MAX),
    [measurementPeriod.repeat.when] NVARCHAR(MAX),
    [measurementPeriod.repeat.offset] bigint,
    [measurementPeriod.repeat.bounds.duration] NVARCHAR(MAX),
    [measurementPeriod.repeat.bounds.range] NVARCHAR(MAX),
    [measurementPeriod.repeat.bounds.period] NVARCHAR(MAX),
    [measurementPeriod.code.id] NVARCHAR(100),
    [measurementPeriod.code.extension] NVARCHAR(MAX),
    [measurementPeriod.code.coding] NVARCHAR(MAX),
    [measurementPeriod.code.text] NVARCHAR(4000),
    [calibration] VARCHAR(MAX),
) WITH (
    LOCATION='/DeviceMetric/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.DeviceMetricIdentifier AS
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
        BULK 'DeviceMetric/**',
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
<<<<<<< HEAD
        [identifier.assigner.id]       NVARCHAR(100)       '$.assigner.id',
=======
        [identifier.assigner.id]       NVARCHAR(4000)      '$.assigner.id',
>>>>>>> origin/main
        [identifier.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [identifier.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [identifier.assigner.type]     VARCHAR(256)        '$.assigner.type',
        [identifier.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [identifier.assigner.display]  NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.DeviceMetricCalibration AS
SELECT
    [id],
    [calibration.JSON],
    [calibration.id],
    [calibration.extension],
    [calibration.modifierExtension],
    [calibration.type],
    [calibration.state],
    [calibration.time]
FROM openrowset (
        BULK 'DeviceMetric/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [calibration.JSON]  VARCHAR(MAX) '$.calibration'
    ) AS rowset
    CROSS APPLY openjson (rowset.[calibration.JSON]) with (
        [calibration.id]               NVARCHAR(100)       '$.id',
        [calibration.extension]        NVARCHAR(MAX)       '$.extension',
        [calibration.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [calibration.type]             NVARCHAR(64)        '$.type',
        [calibration.state]            NVARCHAR(64)        '$.state',
        [calibration.time]             VARCHAR(64)         '$.time'
    ) j
