CREATE EXTERNAL TABLE [fhir].[DeviceUseStatement] (
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
    [status] NVARCHAR(64),
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
    [derivedFrom] VARCHAR(MAX),
    [recordedOn] VARCHAR(64),
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
    [device.id] NVARCHAR(100),
    [device.extension] NVARCHAR(MAX),
    [device.reference] NVARCHAR(4000),
    [device.type] VARCHAR(256),
    [device.identifier.id] NVARCHAR(100),
    [device.identifier.extension] NVARCHAR(MAX),
    [device.identifier.use] NVARCHAR(64),
    [device.identifier.type] NVARCHAR(MAX),
    [device.identifier.system] VARCHAR(256),
    [device.identifier.value] NVARCHAR(4000),
    [device.identifier.period] NVARCHAR(MAX),
    [device.identifier.assigner] NVARCHAR(MAX),
    [device.display] NVARCHAR(4000),
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [bodySite.id] NVARCHAR(100),
    [bodySite.extension] NVARCHAR(MAX),
    [bodySite.coding] VARCHAR(MAX),
    [bodySite.text] NVARCHAR(4000),
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
    LOCATION='/DeviceUseStatement/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.DeviceUseStatementIdentifier AS
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
        BULK 'DeviceUseStatement/**',
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

CREATE VIEW fhir.DeviceUseStatementBasedOn AS
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
        BULK 'DeviceUseStatement/**',
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

CREATE VIEW fhir.DeviceUseStatementDerivedFrom AS
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
        BULK 'DeviceUseStatement/**',
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

CREATE VIEW fhir.DeviceUseStatementReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'DeviceUseStatement/**',
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

CREATE VIEW fhir.DeviceUseStatementReasonReference AS
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
        BULK 'DeviceUseStatement/**',
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

CREATE VIEW fhir.DeviceUseStatementNote AS
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
        BULK 'DeviceUseStatement/**',
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
