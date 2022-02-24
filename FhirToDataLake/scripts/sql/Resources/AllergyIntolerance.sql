CREATE EXTERNAL TABLE [fhir].[AllergyIntolerance] (
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
    [clinicalStatus.id] NVARCHAR(100),
    [clinicalStatus.extension] NVARCHAR(MAX),
    [clinicalStatus.coding] VARCHAR(MAX),
    [clinicalStatus.text] NVARCHAR(4000),
    [verificationStatus.id] NVARCHAR(100),
    [verificationStatus.extension] NVARCHAR(MAX),
    [verificationStatus.coding] VARCHAR(MAX),
    [verificationStatus.text] NVARCHAR(4000),
    [type] NVARCHAR(64),
    [category] VARCHAR(MAX),
    [criticality] NVARCHAR(64),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
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
    [recordedDate] VARCHAR(64),
    [recorder.id] NVARCHAR(100),
    [recorder.extension] NVARCHAR(MAX),
    [recorder.reference] NVARCHAR(4000),
    [recorder.type] VARCHAR(256),
    [recorder.identifier.id] NVARCHAR(100),
    [recorder.identifier.extension] NVARCHAR(MAX),
    [recorder.identifier.use] NVARCHAR(64),
    [recorder.identifier.type] NVARCHAR(MAX),
    [recorder.identifier.system] VARCHAR(256),
    [recorder.identifier.value] NVARCHAR(4000),
    [recorder.identifier.period] NVARCHAR(MAX),
    [recorder.identifier.assigner] NVARCHAR(MAX),
    [recorder.display] NVARCHAR(4000),
    [asserter.id] NVARCHAR(100),
    [asserter.extension] NVARCHAR(MAX),
    [asserter.reference] NVARCHAR(4000),
    [asserter.type] VARCHAR(256),
    [asserter.identifier.id] NVARCHAR(100),
    [asserter.identifier.extension] NVARCHAR(MAX),
    [asserter.identifier.use] NVARCHAR(64),
    [asserter.identifier.type] NVARCHAR(MAX),
    [asserter.identifier.system] VARCHAR(256),
    [asserter.identifier.value] NVARCHAR(4000),
    [asserter.identifier.period] NVARCHAR(MAX),
    [asserter.identifier.assigner] NVARCHAR(MAX),
    [asserter.display] NVARCHAR(4000),
    [lastOccurrence] VARCHAR(64),
    [note] VARCHAR(MAX),
    [reaction] VARCHAR(MAX),
    [onset.dateTime] VARCHAR(64),
    [onset.age.id] NVARCHAR(100),
    [onset.age.extension] NVARCHAR(MAX),
    [onset.age.value] float,
    [onset.age.comparator] NVARCHAR(64),
    [onset.age.unit] NVARCHAR(100),
    [onset.age.system] VARCHAR(256),
    [onset.age.code] NVARCHAR(4000),
    [onset.period.id] NVARCHAR(100),
    [onset.period.extension] NVARCHAR(MAX),
    [onset.period.start] VARCHAR(64),
    [onset.period.end] VARCHAR(64),
    [onset.range.id] NVARCHAR(100),
    [onset.range.extension] NVARCHAR(MAX),
    [onset.range.low.id] NVARCHAR(100),
    [onset.range.low.extension] NVARCHAR(MAX),
    [onset.range.low.value] float,
    [onset.range.low.comparator] NVARCHAR(64),
    [onset.range.low.unit] NVARCHAR(100),
    [onset.range.low.system] VARCHAR(256),
    [onset.range.low.code] NVARCHAR(4000),
    [onset.range.high.id] NVARCHAR(100),
    [onset.range.high.extension] NVARCHAR(MAX),
    [onset.range.high.value] float,
    [onset.range.high.comparator] NVARCHAR(64),
    [onset.range.high.unit] NVARCHAR(100),
    [onset.range.high.system] VARCHAR(256),
    [onset.range.high.code] NVARCHAR(4000),
    [onset.string] NVARCHAR(4000),
) WITH (
    LOCATION='/AllergyIntolerance/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.AllergyIntoleranceIdentifier AS
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
        BULK 'AllergyIntolerance/**',
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

CREATE VIEW fhir.AllergyIntoleranceCategory AS
SELECT
    [id],
    [category.JSON],
    [category]
FROM openrowset (
        BULK 'AllergyIntolerance/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [category.JSON]  VARCHAR(MAX) '$.category'
    ) AS rowset
    CROSS APPLY openjson (rowset.[category.JSON]) with (
        [category]                     NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.AllergyIntoleranceNote AS
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
        BULK 'AllergyIntolerance/**',
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

CREATE VIEW fhir.AllergyIntoleranceReaction AS
SELECT
    [id],
    [reaction.JSON],
    [reaction.id],
    [reaction.extension],
    [reaction.modifierExtension],
    [reaction.substance.id],
    [reaction.substance.extension],
    [reaction.substance.coding],
    [reaction.substance.text],
    [reaction.manifestation],
    [reaction.description],
    [reaction.onset],
    [reaction.severity],
    [reaction.exposureRoute.id],
    [reaction.exposureRoute.extension],
    [reaction.exposureRoute.coding],
    [reaction.exposureRoute.text],
    [reaction.note]
FROM openrowset (
        BULK 'AllergyIntolerance/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reaction.JSON]  VARCHAR(MAX) '$.reaction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reaction.JSON]) with (
        [reaction.id]                  NVARCHAR(100)       '$.id',
        [reaction.extension]           NVARCHAR(MAX)       '$.extension',
        [reaction.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [reaction.substance.id]        NVARCHAR(100)       '$.substance.id',
        [reaction.substance.extension] NVARCHAR(MAX)       '$.substance.extension',
        [reaction.substance.coding]    NVARCHAR(MAX)       '$.substance.coding',
        [reaction.substance.text]      NVARCHAR(4000)      '$.substance.text',
        [reaction.manifestation]       NVARCHAR(MAX)       '$.manifestation' AS JSON,
        [reaction.description]         NVARCHAR(4000)      '$.description',
        [reaction.onset]               VARCHAR(64)         '$.onset',
        [reaction.severity]            NVARCHAR(64)        '$.severity',
        [reaction.exposureRoute.id]    NVARCHAR(100)       '$.exposureRoute.id',
        [reaction.exposureRoute.extension] NVARCHAR(MAX)       '$.exposureRoute.extension',
        [reaction.exposureRoute.coding] NVARCHAR(MAX)       '$.exposureRoute.coding',
        [reaction.exposureRoute.text]  NVARCHAR(4000)      '$.exposureRoute.text',
        [reaction.note]                NVARCHAR(MAX)       '$.note' AS JSON
    ) j
