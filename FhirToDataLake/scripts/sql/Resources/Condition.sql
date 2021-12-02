CREATE EXTERNAL TABLE [fhir].[Condition] (
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
    [identifier] VARCHAR(MAX),
    [clinicalStatus.id] NVARCHAR(4000),
    [clinicalStatus.extension] NVARCHAR(MAX),
    [clinicalStatus.coding] VARCHAR(MAX),
    [clinicalStatus.text] NVARCHAR(4000),
    [verificationStatus.id] NVARCHAR(4000),
    [verificationStatus.extension] NVARCHAR(MAX),
    [verificationStatus.coding] VARCHAR(MAX),
    [verificationStatus.text] NVARCHAR(4000),
    [category] VARCHAR(MAX),
    [severity.id] NVARCHAR(4000),
    [severity.extension] NVARCHAR(MAX),
    [severity.coding] VARCHAR(MAX),
    [severity.text] NVARCHAR(4000),
    [code.id] NVARCHAR(4000),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [bodySite] VARCHAR(MAX),
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
    [recordedDate] VARCHAR(30),
    [recorder.id] NVARCHAR(4000),
    [recorder.extension] NVARCHAR(MAX),
    [recorder.reference] NVARCHAR(4000),
    [recorder.type] VARCHAR(256),
    [recorder.identifier.id] NVARCHAR(4000),
    [recorder.identifier.extension] NVARCHAR(MAX),
    [recorder.identifier.use] NVARCHAR(64),
    [recorder.identifier.type] NVARCHAR(MAX),
    [recorder.identifier.system] VARCHAR(256),
    [recorder.identifier.value] NVARCHAR(4000),
    [recorder.identifier.period] NVARCHAR(MAX),
    [recorder.identifier.assigner] NVARCHAR(MAX),
    [recorder.display] NVARCHAR(4000),
    [asserter.id] NVARCHAR(4000),
    [asserter.extension] NVARCHAR(MAX),
    [asserter.reference] NVARCHAR(4000),
    [asserter.type] VARCHAR(256),
    [asserter.identifier.id] NVARCHAR(4000),
    [asserter.identifier.extension] NVARCHAR(MAX),
    [asserter.identifier.use] NVARCHAR(64),
    [asserter.identifier.type] NVARCHAR(MAX),
    [asserter.identifier.system] VARCHAR(256),
    [asserter.identifier.value] NVARCHAR(4000),
    [asserter.identifier.period] NVARCHAR(MAX),
    [asserter.identifier.assigner] NVARCHAR(MAX),
    [asserter.display] NVARCHAR(4000),
    [stage] VARCHAR(MAX),
    [evidence] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [onset.dateTime] VARCHAR(30),
    [onset.age.id] NVARCHAR(4000),
    [onset.age.extension] NVARCHAR(MAX),
    [onset.age.value] float,
    [onset.age.comparator] NVARCHAR(64),
    [onset.age.unit] NVARCHAR(4000),
    [onset.age.system] VARCHAR(256),
    [onset.age.code] NVARCHAR(4000),
    [onset.period.id] NVARCHAR(4000),
    [onset.period.extension] NVARCHAR(MAX),
    [onset.period.start] VARCHAR(30),
    [onset.period.end] VARCHAR(30),
    [onset.range.id] NVARCHAR(4000),
    [onset.range.extension] NVARCHAR(MAX),
    [onset.range.low.id] NVARCHAR(4000),
    [onset.range.low.extension] NVARCHAR(MAX),
    [onset.range.low.value] float,
    [onset.range.low.comparator] NVARCHAR(64),
    [onset.range.low.unit] NVARCHAR(4000),
    [onset.range.low.system] VARCHAR(256),
    [onset.range.low.code] NVARCHAR(4000),
    [onset.range.high.id] NVARCHAR(4000),
    [onset.range.high.extension] NVARCHAR(MAX),
    [onset.range.high.value] float,
    [onset.range.high.comparator] NVARCHAR(64),
    [onset.range.high.unit] NVARCHAR(4000),
    [onset.range.high.system] VARCHAR(256),
    [onset.range.high.code] NVARCHAR(4000),
    [onset.string] NVARCHAR(4000),
    [abatement.dateTime] VARCHAR(30),
    [abatement.age.id] NVARCHAR(4000),
    [abatement.age.extension] NVARCHAR(MAX),
    [abatement.age.value] float,
    [abatement.age.comparator] NVARCHAR(64),
    [abatement.age.unit] NVARCHAR(4000),
    [abatement.age.system] VARCHAR(256),
    [abatement.age.code] NVARCHAR(4000),
    [abatement.period.id] NVARCHAR(4000),
    [abatement.period.extension] NVARCHAR(MAX),
    [abatement.period.start] VARCHAR(30),
    [abatement.period.end] VARCHAR(30),
    [abatement.range.id] NVARCHAR(4000),
    [abatement.range.extension] NVARCHAR(MAX),
    [abatement.range.low.id] NVARCHAR(4000),
    [abatement.range.low.extension] NVARCHAR(MAX),
    [abatement.range.low.value] float,
    [abatement.range.low.comparator] NVARCHAR(64),
    [abatement.range.low.unit] NVARCHAR(4000),
    [abatement.range.low.system] VARCHAR(256),
    [abatement.range.low.code] NVARCHAR(4000),
    [abatement.range.high.id] NVARCHAR(4000),
    [abatement.range.high.extension] NVARCHAR(MAX),
    [abatement.range.high.value] float,
    [abatement.range.high.comparator] NVARCHAR(64),
    [abatement.range.high.unit] NVARCHAR(4000),
    [abatement.range.high.system] VARCHAR(256),
    [abatement.range.high.code] NVARCHAR(4000),
    [abatement.string] NVARCHAR(4000),
) WITH (
    LOCATION='/Condition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ConditionIdentifier AS
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
        BULK 'Condition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [identifier.JSON]  VARCHAR(MAX) '$.identifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[identifier.JSON]) with (
        [identifier.id]                NVARCHAR(4000)      '$.id',
        [identifier.extension]         NVARCHAR(MAX)       '$.extension',
        [identifier.use]               NVARCHAR(64)        '$.use',
        [identifier.type.id]           NVARCHAR(4000)      '$.type.id',
        [identifier.type.extension]    NVARCHAR(MAX)       '$.type.extension',
        [identifier.type.coding]       NVARCHAR(MAX)       '$.type.coding',
        [identifier.type.text]         NVARCHAR(4000)      '$.type.text',
        [identifier.system]            VARCHAR(256)        '$.system',
        [identifier.value]             NVARCHAR(4000)      '$.value',
        [identifier.period.id]         NVARCHAR(4000)      '$.period.id',
        [identifier.period.extension]  NVARCHAR(MAX)       '$.period.extension',
        [identifier.period.start]      VARCHAR(30)         '$.period.start',
        [identifier.period.end]        VARCHAR(30)         '$.period.end',
        [identifier.assigner.id]       NVARCHAR(4000)      '$.assigner.id',
        [identifier.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [identifier.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [identifier.assigner.type]     VARCHAR(256)        '$.assigner.type',
        [identifier.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [identifier.assigner.display]  NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.ConditionCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'Condition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [category.JSON]  VARCHAR(MAX) '$.category'
    ) AS rowset
    CROSS APPLY openjson (rowset.[category.JSON]) with (
        [category.id]                  NVARCHAR(4000)      '$.id',
        [category.extension]           NVARCHAR(MAX)       '$.extension',
        [category.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [category.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ConditionBodySite AS
SELECT
    [id],
    [bodySite.JSON],
    [bodySite.id],
    [bodySite.extension],
    [bodySite.coding],
    [bodySite.text]
FROM openrowset (
        BULK 'Condition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [bodySite.JSON]  VARCHAR(MAX) '$.bodySite'
    ) AS rowset
    CROSS APPLY openjson (rowset.[bodySite.JSON]) with (
        [bodySite.id]                  NVARCHAR(4000)      '$.id',
        [bodySite.extension]           NVARCHAR(MAX)       '$.extension',
        [bodySite.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [bodySite.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ConditionStage AS
SELECT
    [id],
    [stage.JSON],
    [stage.id],
    [stage.extension],
    [stage.modifierExtension],
    [stage.summary.id],
    [stage.summary.extension],
    [stage.summary.coding],
    [stage.summary.text],
    [stage.assessment],
    [stage.type.id],
    [stage.type.extension],
    [stage.type.coding],
    [stage.type.text]
FROM openrowset (
        BULK 'Condition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [stage.JSON]  VARCHAR(MAX) '$.stage'
    ) AS rowset
    CROSS APPLY openjson (rowset.[stage.JSON]) with (
        [stage.id]                     NVARCHAR(4000)      '$.id',
        [stage.extension]              NVARCHAR(MAX)       '$.extension',
        [stage.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [stage.summary.id]             NVARCHAR(4000)      '$.summary.id',
        [stage.summary.extension]      NVARCHAR(MAX)       '$.summary.extension',
        [stage.summary.coding]         NVARCHAR(MAX)       '$.summary.coding',
        [stage.summary.text]           NVARCHAR(4000)      '$.summary.text',
        [stage.assessment]             NVARCHAR(MAX)       '$.assessment' AS JSON,
        [stage.type.id]                NVARCHAR(4000)      '$.type.id',
        [stage.type.extension]         NVARCHAR(MAX)       '$.type.extension',
        [stage.type.coding]            NVARCHAR(MAX)       '$.type.coding',
        [stage.type.text]              NVARCHAR(4000)      '$.type.text'
    ) j

GO

CREATE VIEW fhir.ConditionEvidence AS
SELECT
    [id],
    [evidence.JSON],
    [evidence.id],
    [evidence.extension],
    [evidence.modifierExtension],
    [evidence.code],
    [evidence.detail]
FROM openrowset (
        BULK 'Condition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [evidence.JSON]  VARCHAR(MAX) '$.evidence'
    ) AS rowset
    CROSS APPLY openjson (rowset.[evidence.JSON]) with (
        [evidence.id]                  NVARCHAR(4000)      '$.id',
        [evidence.extension]           NVARCHAR(MAX)       '$.extension',
        [evidence.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [evidence.code]                NVARCHAR(MAX)       '$.code' AS JSON,
        [evidence.detail]              NVARCHAR(MAX)       '$.detail' AS JSON
    ) j

GO

CREATE VIEW fhir.ConditionNote AS
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
        BULK 'Condition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [note.JSON]  VARCHAR(MAX) '$.note'
    ) AS rowset
    CROSS APPLY openjson (rowset.[note.JSON]) with (
        [note.id]                      NVARCHAR(4000)      '$.id',
        [note.extension]               NVARCHAR(MAX)       '$.extension',
        [note.time]                    VARCHAR(30)         '$.time',
        [note.text]                    NVARCHAR(MAX)       '$.text',
        [note.author.reference.id]     NVARCHAR(4000)      '$.author.reference.id',
        [note.author.reference.extension] NVARCHAR(MAX)       '$.author.reference.extension',
        [note.author.reference.reference] NVARCHAR(4000)      '$.author.reference.reference',
        [note.author.reference.type]   VARCHAR(256)        '$.author.reference.type',
        [note.author.reference.identifier] NVARCHAR(MAX)       '$.author.reference.identifier',
        [note.author.reference.display] NVARCHAR(4000)      '$.author.reference.display',
        [note.author.string]           NVARCHAR(4000)      '$.author.string'
    ) j
