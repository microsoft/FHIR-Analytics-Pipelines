CREATE EXTERNAL TABLE [fhir].[GuidanceResponse] (
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
    [requestIdentifier.id] NVARCHAR(100),
    [requestIdentifier.extension] NVARCHAR(MAX),
    [requestIdentifier.use] NVARCHAR(64),
    [requestIdentifier.type.id] NVARCHAR(100),
    [requestIdentifier.type.extension] NVARCHAR(MAX),
    [requestIdentifier.type.coding] NVARCHAR(MAX),
    [requestIdentifier.type.text] NVARCHAR(4000),
    [requestIdentifier.system] VARCHAR(256),
    [requestIdentifier.value] NVARCHAR(4000),
    [requestIdentifier.period.id] NVARCHAR(100),
    [requestIdentifier.period.extension] NVARCHAR(MAX),
    [requestIdentifier.period.start] VARCHAR(64),
    [requestIdentifier.period.end] VARCHAR(64),
    [requestIdentifier.assigner.id] NVARCHAR(100),
    [requestIdentifier.assigner.extension] NVARCHAR(MAX),
    [requestIdentifier.assigner.reference] NVARCHAR(4000),
    [requestIdentifier.assigner.type] VARCHAR(256),
    [requestIdentifier.assigner.identifier] NVARCHAR(MAX),
    [requestIdentifier.assigner.display] NVARCHAR(4000),
    [identifier] VARCHAR(MAX),
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
    [occurrenceDateTime] VARCHAR(64),
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
    [note] VARCHAR(MAX),
    [evaluationMessage] VARCHAR(MAX),
    [outputParameters.id] NVARCHAR(100),
    [outputParameters.extension] NVARCHAR(MAX),
    [outputParameters.reference] NVARCHAR(4000),
    [outputParameters.type] VARCHAR(256),
    [outputParameters.identifier.id] NVARCHAR(100),
    [outputParameters.identifier.extension] NVARCHAR(MAX),
    [outputParameters.identifier.use] NVARCHAR(64),
    [outputParameters.identifier.type] NVARCHAR(MAX),
    [outputParameters.identifier.system] VARCHAR(256),
    [outputParameters.identifier.value] NVARCHAR(4000),
    [outputParameters.identifier.period] NVARCHAR(MAX),
    [outputParameters.identifier.assigner] NVARCHAR(MAX),
    [outputParameters.display] NVARCHAR(4000),
    [result.id] NVARCHAR(100),
    [result.extension] NVARCHAR(MAX),
    [result.reference] NVARCHAR(4000),
    [result.type] VARCHAR(256),
    [result.identifier.id] NVARCHAR(100),
    [result.identifier.extension] NVARCHAR(MAX),
    [result.identifier.use] NVARCHAR(64),
    [result.identifier.type] NVARCHAR(MAX),
    [result.identifier.system] VARCHAR(256),
    [result.identifier.value] NVARCHAR(4000),
    [result.identifier.period] NVARCHAR(MAX),
    [result.identifier.assigner] NVARCHAR(MAX),
    [result.display] NVARCHAR(4000),
    [dataRequirement] VARCHAR(MAX),
    [module.uri] VARCHAR(256),
    [module.canonical] VARCHAR(256),
    [module.codeableConcept.id] NVARCHAR(100),
    [module.codeableConcept.extension] NVARCHAR(MAX),
    [module.codeableConcept.coding] VARCHAR(MAX),
    [module.codeableConcept.text] NVARCHAR(4000),
) WITH (
    LOCATION='/GuidanceResponse/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.GuidanceResponseIdentifier AS
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
        BULK 'GuidanceResponse/**',
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

CREATE VIEW fhir.GuidanceResponseReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'GuidanceResponse/**',
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

CREATE VIEW fhir.GuidanceResponseReasonReference AS
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
        BULK 'GuidanceResponse/**',
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

CREATE VIEW fhir.GuidanceResponseNote AS
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
        BULK 'GuidanceResponse/**',
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

CREATE VIEW fhir.GuidanceResponseEvaluationMessage AS
SELECT
    [id],
    [evaluationMessage.JSON],
    [evaluationMessage.id],
    [evaluationMessage.extension],
    [evaluationMessage.reference],
    [evaluationMessage.type],
    [evaluationMessage.identifier.id],
    [evaluationMessage.identifier.extension],
    [evaluationMessage.identifier.use],
    [evaluationMessage.identifier.type],
    [evaluationMessage.identifier.system],
    [evaluationMessage.identifier.value],
    [evaluationMessage.identifier.period],
    [evaluationMessage.identifier.assigner],
    [evaluationMessage.display]
FROM openrowset (
        BULK 'GuidanceResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [evaluationMessage.JSON]  VARCHAR(MAX) '$.evaluationMessage'
    ) AS rowset
    CROSS APPLY openjson (rowset.[evaluationMessage.JSON]) with (
        [evaluationMessage.id]         NVARCHAR(100)       '$.id',
        [evaluationMessage.extension]  NVARCHAR(MAX)       '$.extension',
        [evaluationMessage.reference]  NVARCHAR(4000)      '$.reference',
        [evaluationMessage.type]       VARCHAR(256)        '$.type',
        [evaluationMessage.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [evaluationMessage.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [evaluationMessage.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [evaluationMessage.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [evaluationMessage.identifier.system] VARCHAR(256)        '$.identifier.system',
        [evaluationMessage.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [evaluationMessage.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [evaluationMessage.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [evaluationMessage.display]    NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.GuidanceResponseDataRequirement AS
SELECT
    [id],
    [dataRequirement.JSON],
    [dataRequirement.id],
    [dataRequirement.extension],
    [dataRequirement.type],
    [dataRequirement.profile],
    [dataRequirement.mustSupport],
    [dataRequirement.codeFilter],
    [dataRequirement.dateFilter],
    [dataRequirement.limit],
    [dataRequirement.sort],
    [dataRequirement.subject.codeableConcept.id],
    [dataRequirement.subject.codeableConcept.extension],
    [dataRequirement.subject.codeableConcept.coding],
    [dataRequirement.subject.codeableConcept.text],
    [dataRequirement.subject.reference.id],
    [dataRequirement.subject.reference.extension],
    [dataRequirement.subject.reference.reference],
    [dataRequirement.subject.reference.type],
    [dataRequirement.subject.reference.identifier],
    [dataRequirement.subject.reference.display]
FROM openrowset (
        BULK 'GuidanceResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [dataRequirement.JSON]  VARCHAR(MAX) '$.dataRequirement'
    ) AS rowset
    CROSS APPLY openjson (rowset.[dataRequirement.JSON]) with (
        [dataRequirement.id]           NVARCHAR(100)       '$.id',
        [dataRequirement.extension]    NVARCHAR(MAX)       '$.extension',
        [dataRequirement.type]         NVARCHAR(100)       '$.type',
        [dataRequirement.profile]      NVARCHAR(MAX)       '$.profile' AS JSON,
        [dataRequirement.mustSupport]  NVARCHAR(MAX)       '$.mustSupport' AS JSON,
        [dataRequirement.codeFilter]   NVARCHAR(MAX)       '$.codeFilter' AS JSON,
        [dataRequirement.dateFilter]   NVARCHAR(MAX)       '$.dateFilter' AS JSON,
        [dataRequirement.limit]        bigint              '$.limit',
        [dataRequirement.sort]         NVARCHAR(MAX)       '$.sort' AS JSON,
        [dataRequirement.subject.codeableConcept.id] NVARCHAR(100)       '$.subject.codeableConcept.id',
        [dataRequirement.subject.codeableConcept.extension] NVARCHAR(MAX)       '$.subject.codeableConcept.extension',
        [dataRequirement.subject.codeableConcept.coding] NVARCHAR(MAX)       '$.subject.codeableConcept.coding',
        [dataRequirement.subject.codeableConcept.text] NVARCHAR(4000)      '$.subject.codeableConcept.text',
        [dataRequirement.subject.reference.id] NVARCHAR(100)       '$.subject.reference.id',
        [dataRequirement.subject.reference.extension] NVARCHAR(MAX)       '$.subject.reference.extension',
        [dataRequirement.subject.reference.reference] NVARCHAR(4000)      '$.subject.reference.reference',
        [dataRequirement.subject.reference.type] VARCHAR(256)        '$.subject.reference.type',
        [dataRequirement.subject.reference.identifier] NVARCHAR(MAX)       '$.subject.reference.identifier',
        [dataRequirement.subject.reference.display] NVARCHAR(4000)      '$.subject.reference.display'
    ) j
