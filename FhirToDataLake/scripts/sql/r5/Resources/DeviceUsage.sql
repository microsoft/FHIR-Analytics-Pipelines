CREATE EXTERNAL TABLE [fhir].[DeviceUsage] (
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
    [status] NVARCHAR(100),
    [category] VARCHAR(MAX),
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
    [derivedFrom] VARCHAR(MAX),
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
    [usageStatus.id] NVARCHAR(100),
    [usageStatus.extension] NVARCHAR(MAX),
    [usageStatus.coding] VARCHAR(MAX),
    [usageStatus.text] NVARCHAR(4000),
    [usageReason] VARCHAR(MAX),
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
    [device.id] NVARCHAR(100),
    [device.extension] NVARCHAR(MAX),
    [device.concept.id] NVARCHAR(100),
    [device.concept.extension] NVARCHAR(MAX),
    [device.concept.coding] NVARCHAR(MAX),
    [device.concept.text] NVARCHAR(4000),
    [device.reference.id] NVARCHAR(100),
    [device.reference.extension] NVARCHAR(MAX),
    [device.reference.reference] NVARCHAR(4000),
    [device.reference.type] VARCHAR(256),
    [device.reference.identifier] NVARCHAR(MAX),
    [device.reference.display] NVARCHAR(4000),
    [reason] VARCHAR(MAX),
    [bodySite.id] NVARCHAR(100),
    [bodySite.extension] NVARCHAR(MAX),
    [bodySite.concept.id] NVARCHAR(100),
    [bodySite.concept.extension] NVARCHAR(MAX),
    [bodySite.concept.coding] NVARCHAR(MAX),
    [bodySite.concept.text] NVARCHAR(4000),
    [bodySite.reference.id] NVARCHAR(100),
    [bodySite.reference.extension] NVARCHAR(MAX),
    [bodySite.reference.reference] NVARCHAR(4000),
    [bodySite.reference.type] VARCHAR(256),
    [bodySite.reference.identifier] NVARCHAR(MAX),
    [bodySite.reference.display] NVARCHAR(4000),
    [note] VARCHAR(MAX),
    [timing.timing.id] NVARCHAR(100),
    [timing.timing.extension] NVARCHAR(MAX),
    [timing.timing.modifierExtension] NVARCHAR(MAX),
    [timing.timing.event] VARCHAR(MAX),
    [timing.timing.repeat.id] NVARCHAR(100),
    [timing.timing.repeat.extension] NVARCHAR(MAX),
    [timing.timing.repeat.modifierExtension] NVARCHAR(MAX),
    [timing.timing.repeat.count] bigint,
    [timing.timing.repeat.countMax] bigint,
    [timing.timing.repeat.duration] float,
    [timing.timing.repeat.durationMax] float,
    [timing.timing.repeat.durationUnit] NVARCHAR(64),
    [timing.timing.repeat.frequency] bigint,
    [timing.timing.repeat.frequencyMax] bigint,
    [timing.timing.repeat.period] float,
    [timing.timing.repeat.periodMax] float,
    [timing.timing.repeat.periodUnit] NVARCHAR(64),
    [timing.timing.repeat.dayOfWeek] NVARCHAR(MAX),
    [timing.timing.repeat.timeOfDay] NVARCHAR(MAX),
    [timing.timing.repeat.when] NVARCHAR(MAX),
    [timing.timing.repeat.offset] bigint,
    [timing.timing.repeat.bounds.duration] NVARCHAR(MAX),
    [timing.timing.repeat.bounds.range] NVARCHAR(MAX),
    [timing.timing.repeat.bounds.period] NVARCHAR(MAX),
    [timing.timing.code.id] NVARCHAR(100),
    [timing.timing.code.extension] NVARCHAR(MAX),
    [timing.timing.code.coding] NVARCHAR(MAX),
    [timing.timing.code.text] NVARCHAR(4000),
    [timing.period.id] NVARCHAR(100),
    [timing.period.extension] NVARCHAR(MAX),
    [timing.period.start] VARCHAR(64),
    [timing.period.end] VARCHAR(64),
    [timing.dateTime] VARCHAR(64),
) WITH (
    LOCATION='/DeviceUsage/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.DeviceUsageIdentifier AS
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
        BULK 'DeviceUsage/**',
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

CREATE VIEW fhir.DeviceUsageBasedOn AS
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
        BULK 'DeviceUsage/**',
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

CREATE VIEW fhir.DeviceUsageCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'DeviceUsage/**',
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

CREATE VIEW fhir.DeviceUsageDerivedFrom AS
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
        BULK 'DeviceUsage/**',
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

CREATE VIEW fhir.DeviceUsageUsageReason AS
SELECT
    [id],
    [usageReason.JSON],
    [usageReason.id],
    [usageReason.extension],
    [usageReason.coding],
    [usageReason.text]
FROM openrowset (
        BULK 'DeviceUsage/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [usageReason.JSON]  VARCHAR(MAX) '$.usageReason'
    ) AS rowset
    CROSS APPLY openjson (rowset.[usageReason.JSON]) with (
        [usageReason.id]               NVARCHAR(100)       '$.id',
        [usageReason.extension]        NVARCHAR(MAX)       '$.extension',
        [usageReason.coding]           NVARCHAR(MAX)       '$.coding' AS JSON,
        [usageReason.text]             NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.DeviceUsageReason AS
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
        BULK 'DeviceUsage/**',
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

CREATE VIEW fhir.DeviceUsageNote AS
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
        BULK 'DeviceUsage/**',
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
