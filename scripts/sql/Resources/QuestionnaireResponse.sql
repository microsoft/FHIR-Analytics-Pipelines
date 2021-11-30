CREATE EXTERNAL TABLE [fhir].[QuestionnaireResponse] (
    [resourceType] NVARCHAR(4000),
    [id] VARCHAR(64),
    [meta.id] NVARCHAR(4000),
    [meta.extension] NVARCHAR(MAX),
    [meta.versionId] VARCHAR(64),
    [meta.lastUpdated] VARCHAR(30),
    [meta.source] VARCHAR(256),
    [meta.profile] VARCHAR(MAX),
    [meta.security] VARCHAR(MAX),
    [meta.tag] VARCHAR(MAX),
    [implicitRules] VARCHAR(256),
    [language] NVARCHAR(4000),
    [text.id] NVARCHAR(4000),
    [text.extension] NVARCHAR(MAX),
    [text.status] NVARCHAR(64),
    [text.div] NVARCHAR(MAX),
    [extension] NVARCHAR(MAX),
    [modifierExtension] NVARCHAR(MAX),
    [identifier.id] NVARCHAR(4000),
    [identifier.extension] NVARCHAR(MAX),
    [identifier.use] NVARCHAR(64),
    [identifier.type.id] NVARCHAR(4000),
    [identifier.type.extension] NVARCHAR(MAX),
    [identifier.type.coding] NVARCHAR(MAX),
    [identifier.type.text] NVARCHAR(4000),
    [identifier.system] VARCHAR(256),
    [identifier.value] NVARCHAR(4000),
    [identifier.period.id] NVARCHAR(4000),
    [identifier.period.extension] NVARCHAR(MAX),
    [identifier.period.start] VARCHAR(30),
    [identifier.period.end] VARCHAR(30),
    [identifier.assigner.id] NVARCHAR(4000),
    [identifier.assigner.extension] NVARCHAR(MAX),
    [identifier.assigner.reference] NVARCHAR(4000),
    [identifier.assigner.type] VARCHAR(256),
    [identifier.assigner.identifier] NVARCHAR(MAX),
    [identifier.assigner.display] NVARCHAR(4000),
    [basedOn] VARCHAR(MAX),
    [partOf] VARCHAR(MAX),
    [questionnaire] VARCHAR(256),
    [status] NVARCHAR(64),
    [subject.id] NVARCHAR(4000),
    [subject.extension] NVARCHAR(MAX),
    [subject.reference] NVARCHAR(4000),
    [subject.type] VARCHAR(256),
    [subject.identifier.id] NVARCHAR(4000),
    [subject.identifier.extension] NVARCHAR(MAX),
    [subject.identifier.use] NVARCHAR(64),
    [subject.identifier.type] NVARCHAR(MAX),
    [subject.identifier.system] VARCHAR(256),
    [subject.identifier.value] NVARCHAR(4000),
    [subject.identifier.period] NVARCHAR(MAX),
    [subject.identifier.assigner] NVARCHAR(MAX),
    [subject.display] NVARCHAR(4000),
    [encounter.id] NVARCHAR(4000),
    [encounter.extension] NVARCHAR(MAX),
    [encounter.reference] NVARCHAR(4000),
    [encounter.type] VARCHAR(256),
    [encounter.identifier.id] NVARCHAR(4000),
    [encounter.identifier.extension] NVARCHAR(MAX),
    [encounter.identifier.use] NVARCHAR(64),
    [encounter.identifier.type] NVARCHAR(MAX),
    [encounter.identifier.system] VARCHAR(256),
    [encounter.identifier.value] NVARCHAR(4000),
    [encounter.identifier.period] NVARCHAR(MAX),
    [encounter.identifier.assigner] NVARCHAR(MAX),
    [encounter.display] NVARCHAR(4000),
    [authored] VARCHAR(30),
    [author.id] NVARCHAR(4000),
    [author.extension] NVARCHAR(MAX),
    [author.reference] NVARCHAR(4000),
    [author.type] VARCHAR(256),
    [author.identifier.id] NVARCHAR(4000),
    [author.identifier.extension] NVARCHAR(MAX),
    [author.identifier.use] NVARCHAR(64),
    [author.identifier.type] NVARCHAR(MAX),
    [author.identifier.system] VARCHAR(256),
    [author.identifier.value] NVARCHAR(4000),
    [author.identifier.period] NVARCHAR(MAX),
    [author.identifier.assigner] NVARCHAR(MAX),
    [author.display] NVARCHAR(4000),
    [source.id] NVARCHAR(4000),
    [source.extension] NVARCHAR(MAX),
    [source.reference] NVARCHAR(4000),
    [source.type] VARCHAR(256),
    [source.identifier.id] NVARCHAR(4000),
    [source.identifier.extension] NVARCHAR(MAX),
    [source.identifier.use] NVARCHAR(64),
    [source.identifier.type] NVARCHAR(MAX),
    [source.identifier.system] VARCHAR(256),
    [source.identifier.value] NVARCHAR(4000),
    [source.identifier.period] NVARCHAR(MAX),
    [source.identifier.assigner] NVARCHAR(MAX),
    [source.display] NVARCHAR(4000),
    [item] VARCHAR(MAX),
) WITH (
    LOCATION='/QuestionnaireResponse/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.QuestionnaireResponseBasedOn AS
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
        BULK 'QuestionnaireResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [basedOn.JSON]  VARCHAR(MAX) '$.basedOn'
    ) AS rowset
    CROSS APPLY openjson (rowset.[basedOn.JSON]) with (
        [basedOn.id]                   NVARCHAR(4000)      '$.id',
        [basedOn.extension]            NVARCHAR(MAX)       '$.extension',
        [basedOn.reference]            NVARCHAR(4000)      '$.reference',
        [basedOn.type]                 VARCHAR(256)        '$.type',
        [basedOn.identifier.id]        NVARCHAR(4000)      '$.identifier.id',
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

CREATE VIEW fhir.QuestionnaireResponsePartOf AS
SELECT
    [id],
    [partOf.JSON],
    [partOf.id],
    [partOf.extension],
    [partOf.reference],
    [partOf.type],
    [partOf.identifier.id],
    [partOf.identifier.extension],
    [partOf.identifier.use],
    [partOf.identifier.type],
    [partOf.identifier.system],
    [partOf.identifier.value],
    [partOf.identifier.period],
    [partOf.identifier.assigner],
    [partOf.display]
FROM openrowset (
        BULK 'QuestionnaireResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [partOf.JSON]  VARCHAR(MAX) '$.partOf'
    ) AS rowset
    CROSS APPLY openjson (rowset.[partOf.JSON]) with (
        [partOf.id]                    NVARCHAR(4000)      '$.id',
        [partOf.extension]             NVARCHAR(MAX)       '$.extension',
        [partOf.reference]             NVARCHAR(4000)      '$.reference',
        [partOf.type]                  VARCHAR(256)        '$.type',
        [partOf.identifier.id]         NVARCHAR(4000)      '$.identifier.id',
        [partOf.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [partOf.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [partOf.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [partOf.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [partOf.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [partOf.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [partOf.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [partOf.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.QuestionnaireResponseItem AS
SELECT
    [id],
    [item.JSON],
    [item.id],
    [item.extension],
    [item.modifierExtension],
    [item.linkId],
    [item.definition],
    [item.text],
    [item.answer],
    [item.item]
FROM openrowset (
        BULK 'QuestionnaireResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [item.JSON]  VARCHAR(MAX) '$.item'
    ) AS rowset
    CROSS APPLY openjson (rowset.[item.JSON]) with (
        [item.id]                      NVARCHAR(4000)      '$.id',
        [item.extension]               NVARCHAR(MAX)       '$.extension',
        [item.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [item.linkId]                  NVARCHAR(4000)      '$.linkId',
        [item.definition]              VARCHAR(256)        '$.definition',
        [item.text]                    NVARCHAR(4000)      '$.text',
        [item.answer]                  NVARCHAR(MAX)       '$.answer' AS JSON,
        [item.item]                    NVARCHAR(MAX)       '$.item' AS JSON
    ) j
