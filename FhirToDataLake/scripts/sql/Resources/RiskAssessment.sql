CREATE EXTERNAL TABLE [fhir].[RiskAssessment] (
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
    [basedOn.id] NVARCHAR(100),
    [basedOn.extension] NVARCHAR(MAX),
    [basedOn.reference] NVARCHAR(4000),
    [basedOn.type] VARCHAR(256),
    [basedOn.identifier.id] NVARCHAR(100),
    [basedOn.identifier.extension] NVARCHAR(MAX),
    [basedOn.identifier.use] NVARCHAR(64),
    [basedOn.identifier.type] NVARCHAR(MAX),
    [basedOn.identifier.system] VARCHAR(256),
    [basedOn.identifier.value] NVARCHAR(4000),
    [basedOn.identifier.period] NVARCHAR(MAX),
    [basedOn.identifier.assigner] NVARCHAR(MAX),
    [basedOn.display] NVARCHAR(4000),
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
    [status] NVARCHAR(100),
    [method.id] NVARCHAR(100),
    [method.extension] NVARCHAR(MAX),
    [method.coding] VARCHAR(MAX),
    [method.text] NVARCHAR(4000),
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
    [condition.id] NVARCHAR(100),
    [condition.extension] NVARCHAR(MAX),
    [condition.reference] NVARCHAR(4000),
    [condition.type] VARCHAR(256),
    [condition.identifier.id] NVARCHAR(100),
    [condition.identifier.extension] NVARCHAR(MAX),
    [condition.identifier.use] NVARCHAR(64),
    [condition.identifier.type] NVARCHAR(MAX),
    [condition.identifier.system] VARCHAR(256),
    [condition.identifier.value] NVARCHAR(4000),
    [condition.identifier.period] NVARCHAR(MAX),
    [condition.identifier.assigner] NVARCHAR(MAX),
    [condition.display] NVARCHAR(4000),
    [performer.id] NVARCHAR(100),
    [performer.extension] NVARCHAR(MAX),
    [performer.reference] NVARCHAR(4000),
    [performer.type] VARCHAR(256),
    [performer.identifier.id] NVARCHAR(100),
    [performer.identifier.extension] NVARCHAR(MAX),
    [performer.identifier.use] NVARCHAR(64),
    [performer.identifier.type] NVARCHAR(MAX),
    [performer.identifier.system] VARCHAR(256),
    [performer.identifier.value] NVARCHAR(4000),
    [performer.identifier.period] NVARCHAR(MAX),
    [performer.identifier.assigner] NVARCHAR(MAX),
    [performer.display] NVARCHAR(4000),
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [basis] VARCHAR(MAX),
    [prediction] VARCHAR(MAX),
    [mitigation] NVARCHAR(4000),
    [note] VARCHAR(MAX),
    [occurrence.dateTime] VARCHAR(64),
    [occurrence.period.id] NVARCHAR(100),
    [occurrence.period.extension] NVARCHAR(MAX),
    [occurrence.period.start] VARCHAR(64),
    [occurrence.period.end] VARCHAR(64),
) WITH (
    LOCATION='/RiskAssessment/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.RiskAssessmentIdentifier AS
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
        BULK 'RiskAssessment/**',
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

CREATE VIEW fhir.RiskAssessmentReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'RiskAssessment/**',
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

CREATE VIEW fhir.RiskAssessmentReasonReference AS
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
        BULK 'RiskAssessment/**',
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

CREATE VIEW fhir.RiskAssessmentBasis AS
SELECT
    [id],
    [basis.JSON],
    [basis.id],
    [basis.extension],
    [basis.reference],
    [basis.type],
    [basis.identifier.id],
    [basis.identifier.extension],
    [basis.identifier.use],
    [basis.identifier.type],
    [basis.identifier.system],
    [basis.identifier.value],
    [basis.identifier.period],
    [basis.identifier.assigner],
    [basis.display]
FROM openrowset (
        BULK 'RiskAssessment/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [basis.JSON]  VARCHAR(MAX) '$.basis'
    ) AS rowset
    CROSS APPLY openjson (rowset.[basis.JSON]) with (
        [basis.id]                     NVARCHAR(100)       '$.id',
        [basis.extension]              NVARCHAR(MAX)       '$.extension',
        [basis.reference]              NVARCHAR(4000)      '$.reference',
        [basis.type]                   VARCHAR(256)        '$.type',
        [basis.identifier.id]          NVARCHAR(100)       '$.identifier.id',
        [basis.identifier.extension]   NVARCHAR(MAX)       '$.identifier.extension',
        [basis.identifier.use]         NVARCHAR(64)        '$.identifier.use',
        [basis.identifier.type]        NVARCHAR(MAX)       '$.identifier.type',
        [basis.identifier.system]      VARCHAR(256)        '$.identifier.system',
        [basis.identifier.value]       NVARCHAR(4000)      '$.identifier.value',
        [basis.identifier.period]      NVARCHAR(MAX)       '$.identifier.period',
        [basis.identifier.assigner]    NVARCHAR(MAX)       '$.identifier.assigner',
        [basis.display]                NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.RiskAssessmentPrediction AS
SELECT
    [id],
    [prediction.JSON],
    [prediction.id],
    [prediction.extension],
    [prediction.modifierExtension],
    [prediction.outcome.id],
    [prediction.outcome.extension],
    [prediction.outcome.coding],
    [prediction.outcome.text],
    [prediction.qualitativeRisk.id],
    [prediction.qualitativeRisk.extension],
    [prediction.qualitativeRisk.coding],
    [prediction.qualitativeRisk.text],
    [prediction.relativeRisk],
    [prediction.rationale],
    [prediction.probability.decimal],
    [prediction.probability.range.id],
    [prediction.probability.range.extension],
    [prediction.probability.range.low],
    [prediction.probability.range.high],
    [prediction.when.period.id],
    [prediction.when.period.extension],
    [prediction.when.period.start],
    [prediction.when.period.end],
    [prediction.when.range.id],
    [prediction.when.range.extension],
    [prediction.when.range.low],
    [prediction.when.range.high]
FROM openrowset (
        BULK 'RiskAssessment/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [prediction.JSON]  VARCHAR(MAX) '$.prediction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[prediction.JSON]) with (
        [prediction.id]                NVARCHAR(100)       '$.id',
        [prediction.extension]         NVARCHAR(MAX)       '$.extension',
        [prediction.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [prediction.outcome.id]        NVARCHAR(100)       '$.outcome.id',
        [prediction.outcome.extension] NVARCHAR(MAX)       '$.outcome.extension',
        [prediction.outcome.coding]    NVARCHAR(MAX)       '$.outcome.coding',
        [prediction.outcome.text]      NVARCHAR(4000)      '$.outcome.text',
        [prediction.qualitativeRisk.id] NVARCHAR(100)       '$.qualitativeRisk.id',
        [prediction.qualitativeRisk.extension] NVARCHAR(MAX)       '$.qualitativeRisk.extension',
        [prediction.qualitativeRisk.coding] NVARCHAR(MAX)       '$.qualitativeRisk.coding',
        [prediction.qualitativeRisk.text] NVARCHAR(4000)      '$.qualitativeRisk.text',
        [prediction.relativeRisk]      float               '$.relativeRisk',
        [prediction.rationale]         NVARCHAR(4000)      '$.rationale',
        [prediction.probability.decimal] float               '$.probability.decimal',
        [prediction.probability.range.id] NVARCHAR(100)       '$.probability.range.id',
        [prediction.probability.range.extension] NVARCHAR(MAX)       '$.probability.range.extension',
        [prediction.probability.range.low] NVARCHAR(MAX)       '$.probability.range.low',
        [prediction.probability.range.high] NVARCHAR(MAX)       '$.probability.range.high',
        [prediction.when.period.id]    NVARCHAR(100)       '$.when.period.id',
        [prediction.when.period.extension] NVARCHAR(MAX)       '$.when.period.extension',
        [prediction.when.period.start] VARCHAR(64)         '$.when.period.start',
        [prediction.when.period.end]   VARCHAR(64)         '$.when.period.end',
        [prediction.when.range.id]     NVARCHAR(100)       '$.when.range.id',
        [prediction.when.range.extension] NVARCHAR(MAX)       '$.when.range.extension',
        [prediction.when.range.low]    NVARCHAR(MAX)       '$.when.range.low',
        [prediction.when.range.high]   NVARCHAR(MAX)       '$.when.range.high'
    ) j

GO

CREATE VIEW fhir.RiskAssessmentNote AS
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
        BULK 'RiskAssessment/**',
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
