CREATE EXTERNAL TABLE [fhir].[Observation] (
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
    [basedOn] VARCHAR(MAX),
    [partOf] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [category] VARCHAR(MAX),
    [code.id] NVARCHAR(4000),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
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
    [focus] VARCHAR(MAX),
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
    [issued] VARCHAR(30),
    [performer] VARCHAR(MAX),
    [dataAbsentReason.id] NVARCHAR(4000),
    [dataAbsentReason.extension] NVARCHAR(MAX),
    [dataAbsentReason.coding] VARCHAR(MAX),
    [dataAbsentReason.text] NVARCHAR(4000),
    [interpretation] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [bodySite.id] NVARCHAR(4000),
    [bodySite.extension] NVARCHAR(MAX),
    [bodySite.coding] VARCHAR(MAX),
    [bodySite.text] NVARCHAR(4000),
    [method.id] NVARCHAR(4000),
    [method.extension] NVARCHAR(MAX),
    [method.coding] VARCHAR(MAX),
    [method.text] NVARCHAR(4000),
    [specimen.id] NVARCHAR(4000),
    [specimen.extension] NVARCHAR(MAX),
    [specimen.reference] NVARCHAR(4000),
    [specimen.type] VARCHAR(256),
    [specimen.identifier.id] NVARCHAR(4000),
    [specimen.identifier.extension] NVARCHAR(MAX),
    [specimen.identifier.use] NVARCHAR(64),
    [specimen.identifier.type] NVARCHAR(MAX),
    [specimen.identifier.system] VARCHAR(256),
    [specimen.identifier.value] NVARCHAR(4000),
    [specimen.identifier.period] NVARCHAR(MAX),
    [specimen.identifier.assigner] NVARCHAR(MAX),
    [specimen.display] NVARCHAR(4000),
    [device.id] NVARCHAR(4000),
    [device.extension] NVARCHAR(MAX),
    [device.reference] NVARCHAR(4000),
    [device.type] VARCHAR(256),
    [device.identifier.id] NVARCHAR(4000),
    [device.identifier.extension] NVARCHAR(MAX),
    [device.identifier.use] NVARCHAR(64),
    [device.identifier.type] NVARCHAR(MAX),
    [device.identifier.system] VARCHAR(256),
    [device.identifier.value] NVARCHAR(4000),
    [device.identifier.period] NVARCHAR(MAX),
    [device.identifier.assigner] NVARCHAR(MAX),
    [device.display] NVARCHAR(4000),
    [referenceRange] VARCHAR(MAX),
    [hasMember] VARCHAR(MAX),
    [derivedFrom] VARCHAR(MAX),
    [component] VARCHAR(MAX),
    [effective.dateTime] VARCHAR(30),
    [effective.Period.id] NVARCHAR(4000),
    [effective.Period.extension] NVARCHAR(MAX),
    [effective.Period.start] VARCHAR(30),
    [effective.Period.end] VARCHAR(30),
    [effective.Timing.id] NVARCHAR(4000),
    [effective.Timing.extension] NVARCHAR(MAX),
    [effective.Timing.modifierExtension] NVARCHAR(MAX),
    [effective.Timing.event] VARCHAR(MAX),
    [effective.Timing.repeat.id] NVARCHAR(4000),
    [effective.Timing.repeat.extension] NVARCHAR(MAX),
    [effective.Timing.repeat.modifierExtension] NVARCHAR(MAX),
    [effective.Timing.repeat.count] bigint,
    [effective.Timing.repeat.countMax] bigint,
    [effective.Timing.repeat.duration] float,
    [effective.Timing.repeat.durationMax] float,
    [effective.Timing.repeat.durationUnit] NVARCHAR(64),
    [effective.Timing.repeat.frequency] bigint,
    [effective.Timing.repeat.frequencyMax] bigint,
    [effective.Timing.repeat.period] float,
    [effective.Timing.repeat.periodMax] float,
    [effective.Timing.repeat.periodUnit] NVARCHAR(64),
    [effective.Timing.repeat.dayOfWeek] NVARCHAR(MAX),
    [effective.Timing.repeat.timeOfDay] NVARCHAR(MAX),
    [effective.Timing.repeat.when] NVARCHAR(MAX),
    [effective.Timing.repeat.offset] bigint,
    [effective.Timing.repeat.bounds.Duration] NVARCHAR(MAX),
    [effective.Timing.repeat.bounds.Range] NVARCHAR(MAX),
    [effective.Timing.repeat.bounds.Period] NVARCHAR(MAX),
    [effective.Timing.code.id] NVARCHAR(4000),
    [effective.Timing.code.extension] NVARCHAR(MAX),
    [effective.Timing.code.coding] NVARCHAR(MAX),
    [effective.Timing.code.text] NVARCHAR(4000),
    [effective.instant] VARCHAR(30),
    [value.Quantity.id] NVARCHAR(4000),
    [value.Quantity.extension] NVARCHAR(MAX),
    [value.Quantity.value] float,
    [value.Quantity.comparator] NVARCHAR(64),
    [value.Quantity.unit] NVARCHAR(4000),
    [value.Quantity.system] VARCHAR(256),
    [value.Quantity.code] NVARCHAR(4000),
    [value.CodeableConcept.id] NVARCHAR(4000),
    [value.CodeableConcept.extension] NVARCHAR(MAX),
    [value.CodeableConcept.coding] VARCHAR(MAX),
    [value.CodeableConcept.text] NVARCHAR(4000),
    [value.string] NVARCHAR(4000),
    [value.boolean] bit,
    [value.integer] bigint,
    [value.Range.id] NVARCHAR(4000),
    [value.Range.extension] NVARCHAR(MAX),
    [value.Range.low.id] NVARCHAR(4000),
    [value.Range.low.extension] NVARCHAR(MAX),
    [value.Range.low.value] float,
    [value.Range.low.comparator] NVARCHAR(64),
    [value.Range.low.unit] NVARCHAR(4000),
    [value.Range.low.system] VARCHAR(256),
    [value.Range.low.code] NVARCHAR(4000),
    [value.Range.high.id] NVARCHAR(4000),
    [value.Range.high.extension] NVARCHAR(MAX),
    [value.Range.high.value] float,
    [value.Range.high.comparator] NVARCHAR(64),
    [value.Range.high.unit] NVARCHAR(4000),
    [value.Range.high.system] VARCHAR(256),
    [value.Range.high.code] NVARCHAR(4000),
    [value.Ratio.id] NVARCHAR(4000),
    [value.Ratio.extension] NVARCHAR(MAX),
    [value.Ratio.numerator.id] NVARCHAR(4000),
    [value.Ratio.numerator.extension] NVARCHAR(MAX),
    [value.Ratio.numerator.value] float,
    [value.Ratio.numerator.comparator] NVARCHAR(64),
    [value.Ratio.numerator.unit] NVARCHAR(4000),
    [value.Ratio.numerator.system] VARCHAR(256),
    [value.Ratio.numerator.code] NVARCHAR(4000),
    [value.Ratio.denominator.id] NVARCHAR(4000),
    [value.Ratio.denominator.extension] NVARCHAR(MAX),
    [value.Ratio.denominator.value] float,
    [value.Ratio.denominator.comparator] NVARCHAR(64),
    [value.Ratio.denominator.unit] NVARCHAR(4000),
    [value.Ratio.denominator.system] VARCHAR(256),
    [value.Ratio.denominator.code] NVARCHAR(4000),
    [value.SampledData.id] NVARCHAR(4000),
    [value.SampledData.extension] NVARCHAR(MAX),
    [value.SampledData.origin.id] NVARCHAR(4000),
    [value.SampledData.origin.extension] NVARCHAR(MAX),
    [value.SampledData.origin.value] float,
    [value.SampledData.origin.comparator] NVARCHAR(64),
    [value.SampledData.origin.unit] NVARCHAR(4000),
    [value.SampledData.origin.system] VARCHAR(256),
    [value.SampledData.origin.code] NVARCHAR(4000),
    [value.SampledData.period] float,
    [value.SampledData.factor] float,
    [value.SampledData.lowerLimit] float,
    [value.SampledData.upperLimit] float,
    [value.SampledData.dimensions] bigint,
    [value.SampledData.data] NVARCHAR(4000),
    [value.time] NVARCHAR(MAX),
    [value.dateTime] VARCHAR(30),
    [value.Period.id] NVARCHAR(4000),
    [value.Period.extension] NVARCHAR(MAX),
    [value.Period.start] VARCHAR(30),
    [value.Period.end] VARCHAR(30),
) WITH (
    LOCATION='/Observation/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ObservationIdentifier AS
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
        BULK 'Observation/**',
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

CREATE VIEW fhir.ObservationBasedOn AS
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
        BULK 'Observation/**',
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

CREATE VIEW fhir.ObservationPartOf AS
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
        BULK 'Observation/**',
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

CREATE VIEW fhir.ObservationCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'Observation/**',
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

CREATE VIEW fhir.ObservationFocus AS
SELECT
    [id],
    [focus.JSON],
    [focus.id],
    [focus.extension],
    [focus.reference],
    [focus.type],
    [focus.identifier.id],
    [focus.identifier.extension],
    [focus.identifier.use],
    [focus.identifier.type],
    [focus.identifier.system],
    [focus.identifier.value],
    [focus.identifier.period],
    [focus.identifier.assigner],
    [focus.display]
FROM openrowset (
        BULK 'Observation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [focus.JSON]  VARCHAR(MAX) '$.focus'
    ) AS rowset
    CROSS APPLY openjson (rowset.[focus.JSON]) with (
        [focus.id]                     NVARCHAR(4000)      '$.id',
        [focus.extension]              NVARCHAR(MAX)       '$.extension',
        [focus.reference]              NVARCHAR(4000)      '$.reference',
        [focus.type]                   VARCHAR(256)        '$.type',
        [focus.identifier.id]          NVARCHAR(4000)      '$.identifier.id',
        [focus.identifier.extension]   NVARCHAR(MAX)       '$.identifier.extension',
        [focus.identifier.use]         NVARCHAR(64)        '$.identifier.use',
        [focus.identifier.type]        NVARCHAR(MAX)       '$.identifier.type',
        [focus.identifier.system]      VARCHAR(256)        '$.identifier.system',
        [focus.identifier.value]       NVARCHAR(4000)      '$.identifier.value',
        [focus.identifier.period]      NVARCHAR(MAX)       '$.identifier.period',
        [focus.identifier.assigner]    NVARCHAR(MAX)       '$.identifier.assigner',
        [focus.display]                NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ObservationPerformer AS
SELECT
    [id],
    [performer.JSON],
    [performer.id],
    [performer.extension],
    [performer.reference],
    [performer.type],
    [performer.identifier.id],
    [performer.identifier.extension],
    [performer.identifier.use],
    [performer.identifier.type],
    [performer.identifier.system],
    [performer.identifier.value],
    [performer.identifier.period],
    [performer.identifier.assigner],
    [performer.display]
FROM openrowset (
        BULK 'Observation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [performer.JSON]  VARCHAR(MAX) '$.performer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[performer.JSON]) with (
        [performer.id]                 NVARCHAR(4000)      '$.id',
        [performer.extension]          NVARCHAR(MAX)       '$.extension',
        [performer.reference]          NVARCHAR(4000)      '$.reference',
        [performer.type]               VARCHAR(256)        '$.type',
        [performer.identifier.id]      NVARCHAR(4000)      '$.identifier.id',
        [performer.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [performer.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [performer.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [performer.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [performer.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [performer.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [performer.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [performer.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ObservationInterpretation AS
SELECT
    [id],
    [interpretation.JSON],
    [interpretation.id],
    [interpretation.extension],
    [interpretation.coding],
    [interpretation.text]
FROM openrowset (
        BULK 'Observation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [interpretation.JSON]  VARCHAR(MAX) '$.interpretation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[interpretation.JSON]) with (
        [interpretation.id]            NVARCHAR(4000)      '$.id',
        [interpretation.extension]     NVARCHAR(MAX)       '$.extension',
        [interpretation.coding]        NVARCHAR(MAX)       '$.coding' AS JSON,
        [interpretation.text]          NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ObservationNote AS
SELECT
    [id],
    [note.JSON],
    [note.id],
    [note.extension],
    [note.time],
    [note.text],
    [note.author.Reference.id],
    [note.author.Reference.extension],
    [note.author.Reference.reference],
    [note.author.Reference.type],
    [note.author.Reference.identifier],
    [note.author.Reference.display],
    [note.author.string]
FROM openrowset (
        BULK 'Observation/**',
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
        [note.author.Reference.id]     NVARCHAR(4000)      '$.author.Reference.id',
        [note.author.Reference.extension] NVARCHAR(MAX)       '$.author.Reference.extension',
        [note.author.Reference.reference] NVARCHAR(4000)      '$.author.Reference.reference',
        [note.author.Reference.type]   VARCHAR(256)        '$.author.Reference.type',
        [note.author.Reference.identifier] NVARCHAR(MAX)       '$.author.Reference.identifier',
        [note.author.Reference.display] NVARCHAR(4000)      '$.author.Reference.display',
        [note.author.string]           NVARCHAR(4000)      '$.author.string'
    ) j

GO

CREATE VIEW fhir.ObservationReferenceRange AS
SELECT
    [id],
    [referenceRange.JSON],
    [referenceRange.id],
    [referenceRange.extension],
    [referenceRange.modifierExtension],
    [referenceRange.low.id],
    [referenceRange.low.extension],
    [referenceRange.low.value],
    [referenceRange.low.comparator],
    [referenceRange.low.unit],
    [referenceRange.low.system],
    [referenceRange.low.code],
    [referenceRange.high.id],
    [referenceRange.high.extension],
    [referenceRange.high.value],
    [referenceRange.high.comparator],
    [referenceRange.high.unit],
    [referenceRange.high.system],
    [referenceRange.high.code],
    [referenceRange.type.id],
    [referenceRange.type.extension],
    [referenceRange.type.coding],
    [referenceRange.type.text],
    [referenceRange.appliesTo],
    [referenceRange.age.id],
    [referenceRange.age.extension],
    [referenceRange.age.low],
    [referenceRange.age.high],
    [referenceRange.text]
FROM openrowset (
        BULK 'Observation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [referenceRange.JSON]  VARCHAR(MAX) '$.referenceRange'
    ) AS rowset
    CROSS APPLY openjson (rowset.[referenceRange.JSON]) with (
        [referenceRange.id]            NVARCHAR(4000)      '$.id',
        [referenceRange.extension]     NVARCHAR(MAX)       '$.extension',
        [referenceRange.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [referenceRange.low.id]        NVARCHAR(4000)      '$.low.id',
        [referenceRange.low.extension] NVARCHAR(MAX)       '$.low.extension',
        [referenceRange.low.value]     float               '$.low.value',
        [referenceRange.low.comparator] NVARCHAR(64)        '$.low.comparator',
        [referenceRange.low.unit]      NVARCHAR(4000)      '$.low.unit',
        [referenceRange.low.system]    VARCHAR(256)        '$.low.system',
        [referenceRange.low.code]      NVARCHAR(4000)      '$.low.code',
        [referenceRange.high.id]       NVARCHAR(4000)      '$.high.id',
        [referenceRange.high.extension] NVARCHAR(MAX)       '$.high.extension',
        [referenceRange.high.value]    float               '$.high.value',
        [referenceRange.high.comparator] NVARCHAR(64)        '$.high.comparator',
        [referenceRange.high.unit]     NVARCHAR(4000)      '$.high.unit',
        [referenceRange.high.system]   VARCHAR(256)        '$.high.system',
        [referenceRange.high.code]     NVARCHAR(4000)      '$.high.code',
        [referenceRange.type.id]       NVARCHAR(4000)      '$.type.id',
        [referenceRange.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [referenceRange.type.coding]   NVARCHAR(MAX)       '$.type.coding',
        [referenceRange.type.text]     NVARCHAR(4000)      '$.type.text',
        [referenceRange.appliesTo]     NVARCHAR(MAX)       '$.appliesTo' AS JSON,
        [referenceRange.age.id]        NVARCHAR(4000)      '$.age.id',
        [referenceRange.age.extension] NVARCHAR(MAX)       '$.age.extension',
        [referenceRange.age.low]       NVARCHAR(MAX)       '$.age.low',
        [referenceRange.age.high]      NVARCHAR(MAX)       '$.age.high',
        [referenceRange.text]          NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ObservationHasMember AS
SELECT
    [id],
    [hasMember.JSON],
    [hasMember.id],
    [hasMember.extension],
    [hasMember.reference],
    [hasMember.type],
    [hasMember.identifier.id],
    [hasMember.identifier.extension],
    [hasMember.identifier.use],
    [hasMember.identifier.type],
    [hasMember.identifier.system],
    [hasMember.identifier.value],
    [hasMember.identifier.period],
    [hasMember.identifier.assigner],
    [hasMember.display]
FROM openrowset (
        BULK 'Observation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [hasMember.JSON]  VARCHAR(MAX) '$.hasMember'
    ) AS rowset
    CROSS APPLY openjson (rowset.[hasMember.JSON]) with (
        [hasMember.id]                 NVARCHAR(4000)      '$.id',
        [hasMember.extension]          NVARCHAR(MAX)       '$.extension',
        [hasMember.reference]          NVARCHAR(4000)      '$.reference',
        [hasMember.type]               VARCHAR(256)        '$.type',
        [hasMember.identifier.id]      NVARCHAR(4000)      '$.identifier.id',
        [hasMember.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [hasMember.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [hasMember.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [hasMember.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [hasMember.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [hasMember.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [hasMember.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [hasMember.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ObservationDerivedFrom AS
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
        BULK 'Observation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [derivedFrom.JSON]  VARCHAR(MAX) '$.derivedFrom'
    ) AS rowset
    CROSS APPLY openjson (rowset.[derivedFrom.JSON]) with (
        [derivedFrom.id]               NVARCHAR(4000)      '$.id',
        [derivedFrom.extension]        NVARCHAR(MAX)       '$.extension',
        [derivedFrom.reference]        NVARCHAR(4000)      '$.reference',
        [derivedFrom.type]             VARCHAR(256)        '$.type',
        [derivedFrom.identifier.id]    NVARCHAR(4000)      '$.identifier.id',
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

CREATE VIEW fhir.ObservationComponent AS
SELECT
    [id],
    [component.JSON],
    [component.id],
    [component.extension],
    [component.modifierExtension],
    [component.code.id],
    [component.code.extension],
    [component.code.coding],
    [component.code.text],
    [component.dataAbsentReason.id],
    [component.dataAbsentReason.extension],
    [component.dataAbsentReason.coding],
    [component.dataAbsentReason.text],
    [component.interpretation],
    [component.referenceRange],
    [component.value.Quantity.id],
    [component.value.Quantity.extension],
    [component.value.Quantity.value],
    [component.value.Quantity.comparator],
    [component.value.Quantity.unit],
    [component.value.Quantity.system],
    [component.value.Quantity.code],
    [component.value.CodeableConcept.id],
    [component.value.CodeableConcept.extension],
    [component.value.CodeableConcept.coding],
    [component.value.CodeableConcept.text],
    [component.value.string],
    [component.value.boolean],
    [component.value.integer],
    [component.value.Range.id],
    [component.value.Range.extension],
    [component.value.Range.low],
    [component.value.Range.high],
    [component.value.Ratio.id],
    [component.value.Ratio.extension],
    [component.value.Ratio.numerator],
    [component.value.Ratio.denominator],
    [component.value.SampledData.id],
    [component.value.SampledData.extension],
    [component.value.SampledData.origin],
    [component.value.SampledData.period],
    [component.value.SampledData.factor],
    [component.value.SampledData.lowerLimit],
    [component.value.SampledData.upperLimit],
    [component.value.SampledData.dimensions],
    [component.value.SampledData.data],
    [component.value.time],
    [component.value.dateTime],
    [component.value.Period.id],
    [component.value.Period.extension],
    [component.value.Period.start],
    [component.value.Period.end]
FROM openrowset (
        BULK 'Observation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [component.JSON]  VARCHAR(MAX) '$.component'
    ) AS rowset
    CROSS APPLY openjson (rowset.[component.JSON]) with (
        [component.id]                 NVARCHAR(4000)      '$.id',
        [component.extension]          NVARCHAR(MAX)       '$.extension',
        [component.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [component.code.id]            NVARCHAR(4000)      '$.code.id',
        [component.code.extension]     NVARCHAR(MAX)       '$.code.extension',
        [component.code.coding]        NVARCHAR(MAX)       '$.code.coding',
        [component.code.text]          NVARCHAR(4000)      '$.code.text',
        [component.dataAbsentReason.id] NVARCHAR(4000)      '$.dataAbsentReason.id',
        [component.dataAbsentReason.extension] NVARCHAR(MAX)       '$.dataAbsentReason.extension',
        [component.dataAbsentReason.coding] NVARCHAR(MAX)       '$.dataAbsentReason.coding',
        [component.dataAbsentReason.text] NVARCHAR(4000)      '$.dataAbsentReason.text',
        [component.interpretation]     NVARCHAR(MAX)       '$.interpretation' AS JSON,
        [component.referenceRange]     NVARCHAR(MAX)       '$.referenceRange' AS JSON,
        [component.value.Quantity.id]  NVARCHAR(4000)      '$.value.Quantity.id',
        [component.value.Quantity.extension] NVARCHAR(MAX)       '$.value.Quantity.extension',
        [component.value.Quantity.value] float               '$.value.Quantity.value',
        [component.value.Quantity.comparator] NVARCHAR(64)        '$.value.Quantity.comparator',
        [component.value.Quantity.unit] NVARCHAR(4000)      '$.value.Quantity.unit',
        [component.value.Quantity.system] VARCHAR(256)        '$.value.Quantity.system',
        [component.value.Quantity.code] NVARCHAR(4000)      '$.value.Quantity.code',
        [component.value.CodeableConcept.id] NVARCHAR(4000)      '$.value.CodeableConcept.id',
        [component.value.CodeableConcept.extension] NVARCHAR(MAX)       '$.value.CodeableConcept.extension',
        [component.value.CodeableConcept.coding] NVARCHAR(MAX)       '$.value.CodeableConcept.coding',
        [component.value.CodeableConcept.text] NVARCHAR(4000)      '$.value.CodeableConcept.text',
        [component.value.string]       NVARCHAR(4000)      '$.value.string',
        [component.value.boolean]      bit                 '$.value.boolean',
        [component.value.integer]      bigint              '$.value.integer',
        [component.value.Range.id]     NVARCHAR(4000)      '$.value.Range.id',
        [component.value.Range.extension] NVARCHAR(MAX)       '$.value.Range.extension',
        [component.value.Range.low]    NVARCHAR(MAX)       '$.value.Range.low',
        [component.value.Range.high]   NVARCHAR(MAX)       '$.value.Range.high',
        [component.value.Ratio.id]     NVARCHAR(4000)      '$.value.Ratio.id',
        [component.value.Ratio.extension] NVARCHAR(MAX)       '$.value.Ratio.extension',
        [component.value.Ratio.numerator] NVARCHAR(MAX)       '$.value.Ratio.numerator',
        [component.value.Ratio.denominator] NVARCHAR(MAX)       '$.value.Ratio.denominator',
        [component.value.SampledData.id] NVARCHAR(4000)      '$.value.SampledData.id',
        [component.value.SampledData.extension] NVARCHAR(MAX)       '$.value.SampledData.extension',
        [component.value.SampledData.origin] NVARCHAR(MAX)       '$.value.SampledData.origin',
        [component.value.SampledData.period] float               '$.value.SampledData.period',
        [component.value.SampledData.factor] float               '$.value.SampledData.factor',
        [component.value.SampledData.lowerLimit] float               '$.value.SampledData.lowerLimit',
        [component.value.SampledData.upperLimit] float               '$.value.SampledData.upperLimit',
        [component.value.SampledData.dimensions] bigint              '$.value.SampledData.dimensions',
        [component.value.SampledData.data] NVARCHAR(4000)      '$.value.SampledData.data',
        [component.value.time]         NVARCHAR(MAX)       '$.value.time',
        [component.value.dateTime]     VARCHAR(30)         '$.value.dateTime',
        [component.value.Period.id]    NVARCHAR(4000)      '$.value.Period.id',
        [component.value.Period.extension] NVARCHAR(MAX)       '$.value.Period.extension',
        [component.value.Period.start] VARCHAR(30)         '$.value.Period.start',
        [component.value.Period.end]   VARCHAR(30)         '$.value.Period.end'
    ) j
