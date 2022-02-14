CREATE EXTERNAL TABLE [fhir].[List] (
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
    [mode] NVARCHAR(64),
    [title] NVARCHAR(4000),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
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
    [date] VARCHAR(64),
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
    [orderedBy.id] NVARCHAR(100),
    [orderedBy.extension] NVARCHAR(MAX),
    [orderedBy.coding] VARCHAR(MAX),
    [orderedBy.text] NVARCHAR(4000),
    [note] VARCHAR(MAX),
    [entry] VARCHAR(MAX),
    [emptyReason.id] NVARCHAR(100),
    [emptyReason.extension] NVARCHAR(MAX),
    [emptyReason.coding] VARCHAR(MAX),
    [emptyReason.text] NVARCHAR(4000),
) WITH (
    LOCATION='/List/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ListIdentifier AS
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
        BULK 'List/**',
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

CREATE VIEW fhir.ListNote AS
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
        BULK 'List/**',
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

CREATE VIEW fhir.ListEntry AS
SELECT
    [id],
    [entry.JSON],
    [entry.id],
    [entry.extension],
    [entry.modifierExtension],
    [entry.flag.id],
    [entry.flag.extension],
    [entry.flag.coding],
    [entry.flag.text],
    [entry.deleted],
    [entry.date],
    [entry.item.id],
    [entry.item.extension],
    [entry.item.reference],
    [entry.item.type],
    [entry.item.identifier],
    [entry.item.display]
FROM openrowset (
        BULK 'List/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [entry.JSON]  VARCHAR(MAX) '$.entry'
    ) AS rowset
    CROSS APPLY openjson (rowset.[entry.JSON]) with (
        [entry.id]                     NVARCHAR(100)       '$.id',
        [entry.extension]              NVARCHAR(MAX)       '$.extension',
        [entry.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [entry.flag.id]                NVARCHAR(100)       '$.flag.id',
        [entry.flag.extension]         NVARCHAR(MAX)       '$.flag.extension',
        [entry.flag.coding]            NVARCHAR(MAX)       '$.flag.coding',
        [entry.flag.text]              NVARCHAR(4000)      '$.flag.text',
        [entry.deleted]                bit                 '$.deleted',
        [entry.date]                   VARCHAR(64)         '$.date',
        [entry.item.id]                NVARCHAR(100)       '$.item.id',
        [entry.item.extension]         NVARCHAR(MAX)       '$.item.extension',
        [entry.item.reference]         NVARCHAR(4000)      '$.item.reference',
        [entry.item.type]              VARCHAR(256)        '$.item.type',
        [entry.item.identifier]        NVARCHAR(MAX)       '$.item.identifier',
        [entry.item.display]           NVARCHAR(4000)      '$.item.display'
    ) j
