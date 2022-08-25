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
    [category] VARCHAR(MAX),
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
    [recorded] VARCHAR(64),
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
    [reason] VARCHAR(MAX),
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
    [used] VARCHAR(MAX),
    [supportingInfo] VARCHAR(MAX),
    [occurrence.dateTime] VARCHAR(64),
    [occurrence.period.id] NVARCHAR(100),
    [occurrence.period.extension] NVARCHAR(MAX),
    [occurrence.period.start] VARCHAR(64),
    [occurrence.period.end] VARCHAR(64),
    [occurrence.string] NVARCHAR(4000),
    [occurrence.age.id] NVARCHAR(100),
    [occurrence.age.extension] NVARCHAR(MAX),
    [occurrence.age.value] float,
    [occurrence.age.comparator] NVARCHAR(64),
    [occurrence.age.unit] NVARCHAR(100),
    [occurrence.age.system] VARCHAR(256),
    [occurrence.age.code] NVARCHAR(4000),
    [occurrence.range.id] NVARCHAR(100),
    [occurrence.range.extension] NVARCHAR(MAX),
    [occurrence.range.low.id] NVARCHAR(100),
    [occurrence.range.low.extension] NVARCHAR(MAX),
    [occurrence.range.low.value] float,
    [occurrence.range.low.comparator] NVARCHAR(64),
    [occurrence.range.low.unit] NVARCHAR(100),
    [occurrence.range.low.system] VARCHAR(256),
    [occurrence.range.low.code] NVARCHAR(4000),
    [occurrence.range.high.id] NVARCHAR(100),
    [occurrence.range.high.extension] NVARCHAR(MAX),
    [occurrence.range.high.value] float,
    [occurrence.range.high.comparator] NVARCHAR(64),
    [occurrence.range.high.unit] NVARCHAR(100),
    [occurrence.range.high.system] VARCHAR(256),
    [occurrence.range.high.code] NVARCHAR(4000),
    [occurrence.timing.id] NVARCHAR(100),
    [occurrence.timing.extension] NVARCHAR(MAX),
    [occurrence.timing.modifierExtension] NVARCHAR(MAX),
    [occurrence.timing.event] VARCHAR(MAX),
    [occurrence.timing.repeat.id] NVARCHAR(100),
    [occurrence.timing.repeat.extension] NVARCHAR(MAX),
    [occurrence.timing.repeat.modifierExtension] NVARCHAR(MAX),
    [occurrence.timing.repeat.count] bigint,
    [occurrence.timing.repeat.countMax] bigint,
    [occurrence.timing.repeat.duration] float,
    [occurrence.timing.repeat.durationMax] float,
    [occurrence.timing.repeat.durationUnit] NVARCHAR(64),
    [occurrence.timing.repeat.frequency] bigint,
    [occurrence.timing.repeat.frequencyMax] bigint,
    [occurrence.timing.repeat.period] float,
    [occurrence.timing.repeat.periodMax] float,
    [occurrence.timing.repeat.periodUnit] NVARCHAR(64),
    [occurrence.timing.repeat.dayOfWeek] NVARCHAR(MAX),
    [occurrence.timing.repeat.timeOfDay] NVARCHAR(MAX),
    [occurrence.timing.repeat.when] NVARCHAR(MAX),
    [occurrence.timing.repeat.offset] bigint,
    [occurrence.timing.repeat.bounds.duration] NVARCHAR(MAX),
    [occurrence.timing.repeat.bounds.range] NVARCHAR(MAX),
    [occurrence.timing.repeat.bounds.period] NVARCHAR(MAX),
    [occurrence.timing.code.id] NVARCHAR(100),
    [occurrence.timing.code.extension] NVARCHAR(MAX),
    [occurrence.timing.code.coding] NVARCHAR(MAX),
    [occurrence.timing.code.text] NVARCHAR(4000),
    [reported.boolean] bit,
    [reported.reference.id] NVARCHAR(100),
    [reported.reference.extension] NVARCHAR(MAX),
    [reported.reference.reference] NVARCHAR(4000),
    [reported.reference.type] VARCHAR(256),
    [reported.reference.identifier.id] NVARCHAR(100),
    [reported.reference.identifier.extension] NVARCHAR(MAX),
    [reported.reference.identifier.use] NVARCHAR(64),
    [reported.reference.identifier.type] NVARCHAR(MAX),
    [reported.reference.identifier.system] VARCHAR(256),
    [reported.reference.identifier.value] NVARCHAR(4000),
    [reported.reference.identifier.period] NVARCHAR(MAX),
    [reported.reference.identifier.assigner] NVARCHAR(MAX),
    [reported.reference.display] NVARCHAR(4000),
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

CREATE VIEW fhir.ProcedureCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'Procedure/**',
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

CREATE VIEW fhir.ProcedureReason AS
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
        BULK 'Procedure/**',
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

CREATE VIEW fhir.ProcedureUsed AS
SELECT
    [id],
    [used.JSON],
    [used.id],
    [used.extension],
    [used.concept.id],
    [used.concept.extension],
    [used.concept.coding],
    [used.concept.text],
    [used.reference.id],
    [used.reference.extension],
    [used.reference.reference],
    [used.reference.type],
    [used.reference.identifier],
    [used.reference.display]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [used.JSON]  VARCHAR(MAX) '$.used'
    ) AS rowset
    CROSS APPLY openjson (rowset.[used.JSON]) with (
        [used.id]                      NVARCHAR(100)       '$.id',
        [used.extension]               NVARCHAR(MAX)       '$.extension',
        [used.concept.id]              NVARCHAR(100)       '$.concept.id',
        [used.concept.extension]       NVARCHAR(MAX)       '$.concept.extension',
        [used.concept.coding]          NVARCHAR(MAX)       '$.concept.coding',
        [used.concept.text]            NVARCHAR(4000)      '$.concept.text',
        [used.reference.id]            NVARCHAR(100)       '$.reference.id',
        [used.reference.extension]     NVARCHAR(MAX)       '$.reference.extension',
        [used.reference.reference]     NVARCHAR(4000)      '$.reference.reference',
        [used.reference.type]          VARCHAR(256)        '$.reference.type',
        [used.reference.identifier]    NVARCHAR(MAX)       '$.reference.identifier',
        [used.reference.display]       NVARCHAR(4000)      '$.reference.display'
    ) j

GO

CREATE VIEW fhir.ProcedureSupportingInfo AS
SELECT
    [id],
    [supportingInfo.JSON],
    [supportingInfo.id],
    [supportingInfo.extension],
    [supportingInfo.reference],
    [supportingInfo.type],
    [supportingInfo.identifier.id],
    [supportingInfo.identifier.extension],
    [supportingInfo.identifier.use],
    [supportingInfo.identifier.type],
    [supportingInfo.identifier.system],
    [supportingInfo.identifier.value],
    [supportingInfo.identifier.period],
    [supportingInfo.identifier.assigner],
    [supportingInfo.display]
FROM openrowset (
        BULK 'Procedure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [supportingInfo.JSON]  VARCHAR(MAX) '$.supportingInfo'
    ) AS rowset
    CROSS APPLY openjson (rowset.[supportingInfo.JSON]) with (
        [supportingInfo.id]            NVARCHAR(100)       '$.id',
        [supportingInfo.extension]     NVARCHAR(MAX)       '$.extension',
        [supportingInfo.reference]     NVARCHAR(4000)      '$.reference',
        [supportingInfo.type]          VARCHAR(256)        '$.type',
        [supportingInfo.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [supportingInfo.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [supportingInfo.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [supportingInfo.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [supportingInfo.identifier.system] VARCHAR(256)        '$.identifier.system',
        [supportingInfo.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [supportingInfo.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [supportingInfo.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [supportingInfo.display]       NVARCHAR(4000)      '$.display'
    ) j
