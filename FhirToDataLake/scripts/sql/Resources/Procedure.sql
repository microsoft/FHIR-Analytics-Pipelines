CREATE EXTERNAL TABLE [fhir].[Procedure] (
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
    [instantiatesCanonical] VARCHAR(MAX),
    [instantiatesUri] VARCHAR(MAX),
    [basedOn] VARCHAR(MAX),
    [partOf] VARCHAR(MAX),
    [status] NVARCHAR(100),
    [statusReason.id] NVARCHAR(100),
    [statusReason.extension] NVARCHAR(MAX),
    [statusReason.coding] VARCHAR(MAX),
    [statusReason.text] NVARCHAR(4000),
    [category.id] NVARCHAR(100),
    [category.extension] NVARCHAR(MAX),
    [category.coding] VARCHAR(MAX),
    [category.text] NVARCHAR(4000),
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
    [performer] VARCHAR(MAX),
    [location.id] NVARCHAR(100),
    [location.extension] NVARCHAR(MAX),
    [location.reference] NVARCHAR(4000),
    [location.type] VARCHAR(256),
    [location.identifier.id] NVARCHAR(100),
    [location.identifier.extension] NVARCHAR(MAX),
    [location.identifier.use] NVARCHAR(64),
    [location.identifier.type] NVARCHAR(MAX),
    [location.identifier.system] VARCHAR(256),
    [location.identifier.value] NVARCHAR(4000),
    [location.identifier.period] NVARCHAR(MAX),
    [location.identifier.assigner] NVARCHAR(MAX),
    [location.display] NVARCHAR(4000),
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [bodySite] VARCHAR(MAX),
    [outcome.id] NVARCHAR(100),
    [outcome.extension] NVARCHAR(MAX),
    [outcome.coding] VARCHAR(MAX),
    [outcome.text] NVARCHAR(4000),
    [report] VARCHAR(MAX),
    [complication] VARCHAR(MAX),
    [complicationDetail] VARCHAR(MAX),
    [followUp] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [focalDevice] VARCHAR(MAX),
    [usedReference] VARCHAR(MAX),
    [usedCode] VARCHAR(MAX),
    [performed.dateTime] VARCHAR(64),
    [performed.period.id] NVARCHAR(100),
    [performed.period.extension] NVARCHAR(MAX),
    [performed.period.start] VARCHAR(64),
    [performed.period.end] VARCHAR(64),
    [performed.string] NVARCHAR(4000),
    [performed.age.id] NVARCHAR(100),
    [performed.age.extension] NVARCHAR(MAX),
    [performed.age.value] float,
    [performed.age.comparator] NVARCHAR(64),
    [performed.age.unit] NVARCHAR(100),
    [performed.age.system] VARCHAR(256),
    [performed.age.code] NVARCHAR(4000),
    [performed.range.id] NVARCHAR(100),
    [performed.range.extension] NVARCHAR(MAX),
    [performed.range.low.id] NVARCHAR(100),
    [performed.range.low.extension] NVARCHAR(MAX),
    [performed.range.low.value] float,
    [performed.range.low.comparator] NVARCHAR(64),
    [performed.range.low.unit] NVARCHAR(100),
    [performed.range.low.system] VARCHAR(256),
    [performed.range.low.code] NVARCHAR(4000),
    [performed.range.high.id] NVARCHAR(100),
    [performed.range.high.extension] NVARCHAR(MAX),
    [performed.range.high.value] float,
    [performed.range.high.comparator] NVARCHAR(64),
    [performed.range.high.unit] NVARCHAR(100),
    [performed.range.high.system] VARCHAR(256),
    [performed.range.high.code] NVARCHAR(4000),
) WITH (
    LOCATION='/Procedure/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ProcedureIdentifier AS
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
        BULK 'Procedure/**',
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

CREATE VIEW fhir.ProcedureInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesCanonical.JSON]  VARCHAR(MAX) '$.instantiatesCanonical'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesCanonical.JSON]) with (
        [instantiatesCanonical]        NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ProcedureInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesUri.JSON]  VARCHAR(MAX) '$.instantiatesUri'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesUri.JSON]) with (
        [instantiatesUri]              NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ProcedureBasedOn AS
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
        BULK 'Procedure/**',
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

CREATE VIEW fhir.ProcedurePartOf AS
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
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [partOf.JSON]  VARCHAR(MAX) '$.partOf'
    ) AS rowset
    CROSS APPLY openjson (rowset.[partOf.JSON]) with (
        [partOf.id]                    NVARCHAR(100)       '$.id',
        [partOf.extension]             NVARCHAR(MAX)       '$.extension',
        [partOf.reference]             NVARCHAR(4000)      '$.reference',
        [partOf.type]                  VARCHAR(256)        '$.type',
        [partOf.identifier.id]         NVARCHAR(100)       '$.identifier.id',
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

CREATE VIEW fhir.ProcedurePerformer AS
SELECT
    [id],
    [performer.JSON],
    [performer.id],
    [performer.extension],
    [performer.modifierExtension],
    [performer.function.id],
    [performer.function.extension],
    [performer.function.coding],
    [performer.function.text],
    [performer.actor.id],
    [performer.actor.extension],
    [performer.actor.reference],
    [performer.actor.type],
    [performer.actor.identifier],
    [performer.actor.display],
    [performer.onBehalfOf.id],
    [performer.onBehalfOf.extension],
    [performer.onBehalfOf.reference],
    [performer.onBehalfOf.type],
    [performer.onBehalfOf.identifier],
    [performer.onBehalfOf.display]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [performer.JSON]  VARCHAR(MAX) '$.performer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[performer.JSON]) with (
        [performer.id]                 NVARCHAR(100)       '$.id',
        [performer.extension]          NVARCHAR(MAX)       '$.extension',
        [performer.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [performer.function.id]        NVARCHAR(100)       '$.function.id',
        [performer.function.extension] NVARCHAR(MAX)       '$.function.extension',
        [performer.function.coding]    NVARCHAR(MAX)       '$.function.coding',
        [performer.function.text]      NVARCHAR(4000)      '$.function.text',
        [performer.actor.id]           NVARCHAR(100)       '$.actor.id',
        [performer.actor.extension]    NVARCHAR(MAX)       '$.actor.extension',
        [performer.actor.reference]    NVARCHAR(4000)      '$.actor.reference',
        [performer.actor.type]         VARCHAR(256)        '$.actor.type',
        [performer.actor.identifier]   NVARCHAR(MAX)       '$.actor.identifier',
        [performer.actor.display]      NVARCHAR(4000)      '$.actor.display',
        [performer.onBehalfOf.id]      NVARCHAR(100)       '$.onBehalfOf.id',
        [performer.onBehalfOf.extension] NVARCHAR(MAX)       '$.onBehalfOf.extension',
        [performer.onBehalfOf.reference] NVARCHAR(4000)      '$.onBehalfOf.reference',
        [performer.onBehalfOf.type]    VARCHAR(256)        '$.onBehalfOf.type',
        [performer.onBehalfOf.identifier] NVARCHAR(MAX)       '$.onBehalfOf.identifier',
        [performer.onBehalfOf.display] NVARCHAR(4000)      '$.onBehalfOf.display'
    ) j

GO

CREATE VIEW fhir.ProcedureReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'Procedure/**',
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

CREATE VIEW fhir.ProcedureReasonReference AS
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
        BULK 'Procedure/**',
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

CREATE VIEW fhir.ProcedureBodySite AS
SELECT
    [id],
    [bodySite.JSON],
    [bodySite.id],
    [bodySite.extension],
    [bodySite.coding],
    [bodySite.text]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [bodySite.JSON]  VARCHAR(MAX) '$.bodySite'
    ) AS rowset
    CROSS APPLY openjson (rowset.[bodySite.JSON]) with (
        [bodySite.id]                  NVARCHAR(100)       '$.id',
        [bodySite.extension]           NVARCHAR(MAX)       '$.extension',
        [bodySite.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [bodySite.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ProcedureReport AS
SELECT
    [id],
    [report.JSON],
    [report.id],
    [report.extension],
    [report.reference],
    [report.type],
    [report.identifier.id],
    [report.identifier.extension],
    [report.identifier.use],
    [report.identifier.type],
    [report.identifier.system],
    [report.identifier.value],
    [report.identifier.period],
    [report.identifier.assigner],
    [report.display]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [report.JSON]  VARCHAR(MAX) '$.report'
    ) AS rowset
    CROSS APPLY openjson (rowset.[report.JSON]) with (
        [report.id]                    NVARCHAR(100)       '$.id',
        [report.extension]             NVARCHAR(MAX)       '$.extension',
        [report.reference]             NVARCHAR(4000)      '$.reference',
        [report.type]                  VARCHAR(256)        '$.type',
        [report.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [report.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [report.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [report.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [report.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [report.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [report.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [report.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [report.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ProcedureComplication AS
SELECT
    [id],
    [complication.JSON],
    [complication.id],
    [complication.extension],
    [complication.coding],
    [complication.text]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [complication.JSON]  VARCHAR(MAX) '$.complication'
    ) AS rowset
    CROSS APPLY openjson (rowset.[complication.JSON]) with (
        [complication.id]              NVARCHAR(100)       '$.id',
        [complication.extension]       NVARCHAR(MAX)       '$.extension',
        [complication.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [complication.text]            NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ProcedureComplicationDetail AS
SELECT
    [id],
    [complicationDetail.JSON],
    [complicationDetail.id],
    [complicationDetail.extension],
    [complicationDetail.reference],
    [complicationDetail.type],
    [complicationDetail.identifier.id],
    [complicationDetail.identifier.extension],
    [complicationDetail.identifier.use],
    [complicationDetail.identifier.type],
    [complicationDetail.identifier.system],
    [complicationDetail.identifier.value],
    [complicationDetail.identifier.period],
    [complicationDetail.identifier.assigner],
    [complicationDetail.display]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [complicationDetail.JSON]  VARCHAR(MAX) '$.complicationDetail'
    ) AS rowset
    CROSS APPLY openjson (rowset.[complicationDetail.JSON]) with (
        [complicationDetail.id]        NVARCHAR(100)       '$.id',
        [complicationDetail.extension] NVARCHAR(MAX)       '$.extension',
        [complicationDetail.reference] NVARCHAR(4000)      '$.reference',
        [complicationDetail.type]      VARCHAR(256)        '$.type',
        [complicationDetail.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [complicationDetail.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [complicationDetail.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [complicationDetail.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [complicationDetail.identifier.system] VARCHAR(256)        '$.identifier.system',
        [complicationDetail.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [complicationDetail.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [complicationDetail.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [complicationDetail.display]   NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ProcedureFollowUp AS
SELECT
    [id],
    [followUp.JSON],
    [followUp.id],
    [followUp.extension],
    [followUp.coding],
    [followUp.text]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [followUp.JSON]  VARCHAR(MAX) '$.followUp'
    ) AS rowset
    CROSS APPLY openjson (rowset.[followUp.JSON]) with (
        [followUp.id]                  NVARCHAR(100)       '$.id',
        [followUp.extension]           NVARCHAR(MAX)       '$.extension',
        [followUp.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [followUp.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ProcedureNote AS
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
        BULK 'Procedure/**',
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

CREATE VIEW fhir.ProcedureFocalDevice AS
SELECT
    [id],
    [focalDevice.JSON],
    [focalDevice.id],
    [focalDevice.extension],
    [focalDevice.modifierExtension],
    [focalDevice.action.id],
    [focalDevice.action.extension],
    [focalDevice.action.coding],
    [focalDevice.action.text],
    [focalDevice.manipulated.id],
    [focalDevice.manipulated.extension],
    [focalDevice.manipulated.reference],
    [focalDevice.manipulated.type],
    [focalDevice.manipulated.identifier],
    [focalDevice.manipulated.display]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [focalDevice.JSON]  VARCHAR(MAX) '$.focalDevice'
    ) AS rowset
    CROSS APPLY openjson (rowset.[focalDevice.JSON]) with (
        [focalDevice.id]               NVARCHAR(100)       '$.id',
        [focalDevice.extension]        NVARCHAR(MAX)       '$.extension',
        [focalDevice.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [focalDevice.action.id]        NVARCHAR(100)       '$.action.id',
        [focalDevice.action.extension] NVARCHAR(MAX)       '$.action.extension',
        [focalDevice.action.coding]    NVARCHAR(MAX)       '$.action.coding',
        [focalDevice.action.text]      NVARCHAR(4000)      '$.action.text',
        [focalDevice.manipulated.id]   NVARCHAR(100)       '$.manipulated.id',
        [focalDevice.manipulated.extension] NVARCHAR(MAX)       '$.manipulated.extension',
        [focalDevice.manipulated.reference] NVARCHAR(4000)      '$.manipulated.reference',
        [focalDevice.manipulated.type] VARCHAR(256)        '$.manipulated.type',
        [focalDevice.manipulated.identifier] NVARCHAR(MAX)       '$.manipulated.identifier',
        [focalDevice.manipulated.display] NVARCHAR(4000)      '$.manipulated.display'
    ) j

GO

CREATE VIEW fhir.ProcedureUsedReference AS
SELECT
    [id],
    [usedReference.JSON],
    [usedReference.id],
    [usedReference.extension],
    [usedReference.reference],
    [usedReference.type],
    [usedReference.identifier.id],
    [usedReference.identifier.extension],
    [usedReference.identifier.use],
    [usedReference.identifier.type],
    [usedReference.identifier.system],
    [usedReference.identifier.value],
    [usedReference.identifier.period],
    [usedReference.identifier.assigner],
    [usedReference.display]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [usedReference.JSON]  VARCHAR(MAX) '$.usedReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[usedReference.JSON]) with (
        [usedReference.id]             NVARCHAR(100)       '$.id',
        [usedReference.extension]      NVARCHAR(MAX)       '$.extension',
        [usedReference.reference]      NVARCHAR(4000)      '$.reference',
        [usedReference.type]           VARCHAR(256)        '$.type',
        [usedReference.identifier.id]  NVARCHAR(100)       '$.identifier.id',
        [usedReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [usedReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [usedReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [usedReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [usedReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [usedReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [usedReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [usedReference.display]        NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ProcedureUsedCode AS
SELECT
    [id],
    [usedCode.JSON],
    [usedCode.id],
    [usedCode.extension],
    [usedCode.coding],
    [usedCode.text]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [usedCode.JSON]  VARCHAR(MAX) '$.usedCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[usedCode.JSON]) with (
        [usedCode.id]                  NVARCHAR(100)       '$.id',
        [usedCode.extension]           NVARCHAR(MAX)       '$.extension',
        [usedCode.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [usedCode.text]                NVARCHAR(4000)      '$.text'
    ) j
