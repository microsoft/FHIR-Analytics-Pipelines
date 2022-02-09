CREATE EXTERNAL TABLE [fhir].[DocumentManifest] (
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
    [masterIdentifier.id] NVARCHAR(100),
    [masterIdentifier.extension] NVARCHAR(MAX),
    [masterIdentifier.use] NVARCHAR(64),
    [masterIdentifier.type.id] NVARCHAR(100),
    [masterIdentifier.type.extension] NVARCHAR(MAX),
    [masterIdentifier.type.coding] NVARCHAR(MAX),
    [masterIdentifier.type.text] NVARCHAR(4000),
    [masterIdentifier.system] VARCHAR(256),
    [masterIdentifier.value] NVARCHAR(4000),
    [masterIdentifier.period.id] NVARCHAR(100),
    [masterIdentifier.period.extension] NVARCHAR(MAX),
    [masterIdentifier.period.start] VARCHAR(64),
    [masterIdentifier.period.end] VARCHAR(64),
    [masterIdentifier.assigner.id] NVARCHAR(100),
    [masterIdentifier.assigner.extension] NVARCHAR(MAX),
    [masterIdentifier.assigner.reference] NVARCHAR(4000),
    [masterIdentifier.assigner.type] VARCHAR(256),
    [masterIdentifier.assigner.identifier] NVARCHAR(MAX),
    [masterIdentifier.assigner.display] NVARCHAR(4000),
    [identifier] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
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
    [created] VARCHAR(64),
    [author] VARCHAR(MAX),
    [recipient] VARCHAR(MAX),
    [source] VARCHAR(256),
    [description] NVARCHAR(4000),
    [content] VARCHAR(MAX),
    [related] VARCHAR(MAX),
) WITH (
    LOCATION='/DocumentManifest/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.DocumentManifestIdentifier AS
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
        BULK 'DocumentManifest/**',
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

CREATE VIEW fhir.DocumentManifestAuthor AS
SELECT
    [id],
    [author.JSON],
    [author.id],
    [author.extension],
    [author.reference],
    [author.type],
    [author.identifier.id],
    [author.identifier.extension],
    [author.identifier.use],
    [author.identifier.type],
    [author.identifier.system],
    [author.identifier.value],
    [author.identifier.period],
    [author.identifier.assigner],
    [author.display]
FROM openrowset (
        BULK 'DocumentManifest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [author.JSON]  VARCHAR(MAX) '$.author'
    ) AS rowset
    CROSS APPLY openjson (rowset.[author.JSON]) with (
        [author.id]                    NVARCHAR(100)       '$.id',
        [author.extension]             NVARCHAR(MAX)       '$.extension',
        [author.reference]             NVARCHAR(4000)      '$.reference',
        [author.type]                  VARCHAR(256)        '$.type',
        [author.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [author.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [author.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [author.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [author.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [author.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [author.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [author.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [author.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DocumentManifestRecipient AS
SELECT
    [id],
    [recipient.JSON],
    [recipient.id],
    [recipient.extension],
    [recipient.reference],
    [recipient.type],
    [recipient.identifier.id],
    [recipient.identifier.extension],
    [recipient.identifier.use],
    [recipient.identifier.type],
    [recipient.identifier.system],
    [recipient.identifier.value],
    [recipient.identifier.period],
    [recipient.identifier.assigner],
    [recipient.display]
FROM openrowset (
        BULK 'DocumentManifest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [recipient.JSON]  VARCHAR(MAX) '$.recipient'
    ) AS rowset
    CROSS APPLY openjson (rowset.[recipient.JSON]) with (
        [recipient.id]                 NVARCHAR(100)       '$.id',
        [recipient.extension]          NVARCHAR(MAX)       '$.extension',
        [recipient.reference]          NVARCHAR(4000)      '$.reference',
        [recipient.type]               VARCHAR(256)        '$.type',
        [recipient.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [recipient.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [recipient.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [recipient.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [recipient.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [recipient.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [recipient.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [recipient.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [recipient.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DocumentManifestContent AS
SELECT
    [id],
    [content.JSON],
    [content.id],
    [content.extension],
    [content.reference],
    [content.type],
    [content.identifier.id],
    [content.identifier.extension],
    [content.identifier.use],
    [content.identifier.type],
    [content.identifier.system],
    [content.identifier.value],
    [content.identifier.period],
    [content.identifier.assigner],
    [content.display]
FROM openrowset (
        BULK 'DocumentManifest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [content.JSON]  VARCHAR(MAX) '$.content'
    ) AS rowset
    CROSS APPLY openjson (rowset.[content.JSON]) with (
        [content.id]                   NVARCHAR(100)       '$.id',
        [content.extension]            NVARCHAR(MAX)       '$.extension',
        [content.reference]            NVARCHAR(4000)      '$.reference',
        [content.type]                 VARCHAR(256)        '$.type',
        [content.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [content.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [content.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [content.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [content.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [content.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [content.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [content.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [content.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DocumentManifestRelated AS
SELECT
    [id],
    [related.JSON],
    [related.id],
    [related.extension],
    [related.modifierExtension],
    [related.identifier.id],
    [related.identifier.extension],
    [related.identifier.use],
    [related.identifier.type],
    [related.identifier.system],
    [related.identifier.value],
    [related.identifier.period],
    [related.identifier.assigner],
    [related.ref.id],
    [related.ref.extension],
    [related.ref.reference],
    [related.ref.type],
    [related.ref.identifier],
    [related.ref.display]
FROM openrowset (
        BULK 'DocumentManifest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [related.JSON]  VARCHAR(MAX) '$.related'
    ) AS rowset
    CROSS APPLY openjson (rowset.[related.JSON]) with (
        [related.id]                   NVARCHAR(100)       '$.id',
        [related.extension]            NVARCHAR(MAX)       '$.extension',
        [related.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [related.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [related.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [related.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [related.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [related.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [related.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [related.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [related.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [related.ref.id]               NVARCHAR(100)       '$.ref.id',
        [related.ref.extension]        NVARCHAR(MAX)       '$.ref.extension',
        [related.ref.reference]        NVARCHAR(4000)      '$.ref.reference',
        [related.ref.type]             VARCHAR(256)        '$.ref.type',
        [related.ref.identifier]       NVARCHAR(MAX)       '$.ref.identifier',
        [related.ref.display]          NVARCHAR(4000)      '$.ref.display'
    ) j
