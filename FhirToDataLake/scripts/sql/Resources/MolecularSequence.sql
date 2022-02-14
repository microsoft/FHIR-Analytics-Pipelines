CREATE EXTERNAL TABLE [fhir].[MolecularSequence] (
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
    [type] NVARCHAR(64),
    [coordinateSystem] bigint,
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
    [specimen.id] NVARCHAR(100),
    [specimen.extension] NVARCHAR(MAX),
    [specimen.reference] NVARCHAR(4000),
    [specimen.type] VARCHAR(256),
    [specimen.identifier.id] NVARCHAR(100),
    [specimen.identifier.extension] NVARCHAR(MAX),
    [specimen.identifier.use] NVARCHAR(64),
    [specimen.identifier.type] NVARCHAR(MAX),
    [specimen.identifier.system] VARCHAR(256),
    [specimen.identifier.value] NVARCHAR(4000),
    [specimen.identifier.period] NVARCHAR(MAX),
    [specimen.identifier.assigner] NVARCHAR(MAX),
    [specimen.display] NVARCHAR(4000),
    [device.id] NVARCHAR(100),
    [device.extension] NVARCHAR(MAX),
    [device.reference] NVARCHAR(4000),
    [device.type] VARCHAR(256),
    [device.identifier.id] NVARCHAR(100),
    [device.identifier.extension] NVARCHAR(MAX),
    [device.identifier.use] NVARCHAR(64),
    [device.identifier.type] NVARCHAR(MAX),
    [device.identifier.system] VARCHAR(256),
    [device.identifier.value] NVARCHAR(4000),
    [device.identifier.period] NVARCHAR(MAX),
    [device.identifier.assigner] NVARCHAR(MAX),
    [device.display] NVARCHAR(4000),
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
    [quantity.id] NVARCHAR(100),
    [quantity.extension] NVARCHAR(MAX),
    [quantity.value] float,
    [quantity.comparator] NVARCHAR(64),
    [quantity.unit] NVARCHAR(100),
    [quantity.system] VARCHAR(256),
    [quantity.code] NVARCHAR(4000),
    [referenceSeq.id] NVARCHAR(100),
    [referenceSeq.extension] NVARCHAR(MAX),
    [referenceSeq.modifierExtension] NVARCHAR(MAX),
    [referenceSeq.chromosome.id] NVARCHAR(100),
    [referenceSeq.chromosome.extension] NVARCHAR(MAX),
    [referenceSeq.chromosome.coding] NVARCHAR(MAX),
    [referenceSeq.chromosome.text] NVARCHAR(4000),
    [referenceSeq.genomeBuild] NVARCHAR(4000),
    [referenceSeq.orientation] NVARCHAR(64),
    [referenceSeq.referenceSeqId.id] NVARCHAR(100),
    [referenceSeq.referenceSeqId.extension] NVARCHAR(MAX),
    [referenceSeq.referenceSeqId.coding] NVARCHAR(MAX),
    [referenceSeq.referenceSeqId.text] NVARCHAR(4000),
    [referenceSeq.referenceSeqPointer.id] NVARCHAR(100),
    [referenceSeq.referenceSeqPointer.extension] NVARCHAR(MAX),
    [referenceSeq.referenceSeqPointer.reference] NVARCHAR(4000),
    [referenceSeq.referenceSeqPointer.type] VARCHAR(256),
    [referenceSeq.referenceSeqPointer.identifier] NVARCHAR(MAX),
    [referenceSeq.referenceSeqPointer.display] NVARCHAR(4000),
    [referenceSeq.referenceSeqString] NVARCHAR(4000),
    [referenceSeq.strand] NVARCHAR(64),
    [referenceSeq.windowStart] bigint,
    [referenceSeq.windowEnd] bigint,
    [variant] VARCHAR(MAX),
    [observedSeq] NVARCHAR(4000),
    [quality] VARCHAR(MAX),
    [readCoverage] bigint,
    [repository] VARCHAR(MAX),
    [pointer] VARCHAR(MAX),
    [structureVariant] VARCHAR(MAX),
) WITH (
    LOCATION='/MolecularSequence/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MolecularSequenceIdentifier AS
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
        BULK 'MolecularSequence/**',
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

CREATE VIEW fhir.MolecularSequenceVariant AS
SELECT
    [id],
    [variant.JSON],
    [variant.id],
    [variant.extension],
    [variant.modifierExtension],
    [variant.start],
    [variant.end],
    [variant.observedAllele],
    [variant.referenceAllele],
    [variant.cigar],
    [variant.variantPointer.id],
    [variant.variantPointer.extension],
    [variant.variantPointer.reference],
    [variant.variantPointer.type],
    [variant.variantPointer.identifier],
    [variant.variantPointer.display]
FROM openrowset (
        BULK 'MolecularSequence/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [variant.JSON]  VARCHAR(MAX) '$.variant'
    ) AS rowset
    CROSS APPLY openjson (rowset.[variant.JSON]) with (
        [variant.id]                   NVARCHAR(100)       '$.id',
        [variant.extension]            NVARCHAR(MAX)       '$.extension',
        [variant.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [variant.start]                bigint              '$.start',
        [variant.end]                  bigint              '$.end',
        [variant.observedAllele]       NVARCHAR(4000)      '$.observedAllele',
        [variant.referenceAllele]      NVARCHAR(4000)      '$.referenceAllele',
        [variant.cigar]                NVARCHAR(4000)      '$.cigar',
        [variant.variantPointer.id]    NVARCHAR(100)       '$.variantPointer.id',
        [variant.variantPointer.extension] NVARCHAR(MAX)       '$.variantPointer.extension',
        [variant.variantPointer.reference] NVARCHAR(4000)      '$.variantPointer.reference',
        [variant.variantPointer.type]  VARCHAR(256)        '$.variantPointer.type',
        [variant.variantPointer.identifier] NVARCHAR(MAX)       '$.variantPointer.identifier',
        [variant.variantPointer.display] NVARCHAR(4000)      '$.variantPointer.display'
    ) j

GO

CREATE VIEW fhir.MolecularSequenceQuality AS
SELECT
    [id],
    [quality.JSON],
    [quality.id],
    [quality.extension],
    [quality.modifierExtension],
    [quality.type],
    [quality.standardSequence.id],
    [quality.standardSequence.extension],
    [quality.standardSequence.coding],
    [quality.standardSequence.text],
    [quality.start],
    [quality.end],
    [quality.score.id],
    [quality.score.extension],
    [quality.score.value],
    [quality.score.comparator],
    [quality.score.unit],
    [quality.score.system],
    [quality.score.code],
    [quality.method.id],
    [quality.method.extension],
    [quality.method.coding],
    [quality.method.text],
    [quality.truthTP],
    [quality.queryTP],
    [quality.truthFN],
    [quality.queryFP],
    [quality.gtFP],
    [quality.precision],
    [quality.recall],
    [quality.fScore],
    [quality.roc.id],
    [quality.roc.extension],
    [quality.roc.modifierExtension],
    [quality.roc.score],
    [quality.roc.numTP],
    [quality.roc.numFP],
    [quality.roc.numFN],
    [quality.roc.precision],
    [quality.roc.sensitivity],
    [quality.roc.fMeasure]
FROM openrowset (
        BULK 'MolecularSequence/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [quality.JSON]  VARCHAR(MAX) '$.quality'
    ) AS rowset
    CROSS APPLY openjson (rowset.[quality.JSON]) with (
        [quality.id]                   NVARCHAR(100)       '$.id',
        [quality.extension]            NVARCHAR(MAX)       '$.extension',
        [quality.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [quality.type]                 NVARCHAR(64)        '$.type',
        [quality.standardSequence.id]  NVARCHAR(100)       '$.standardSequence.id',
        [quality.standardSequence.extension] NVARCHAR(MAX)       '$.standardSequence.extension',
        [quality.standardSequence.coding] NVARCHAR(MAX)       '$.standardSequence.coding',
        [quality.standardSequence.text] NVARCHAR(4000)      '$.standardSequence.text',
        [quality.start]                bigint              '$.start',
        [quality.end]                  bigint              '$.end',
        [quality.score.id]             NVARCHAR(100)       '$.score.id',
        [quality.score.extension]      NVARCHAR(MAX)       '$.score.extension',
        [quality.score.value]          float               '$.score.value',
        [quality.score.comparator]     NVARCHAR(64)        '$.score.comparator',
        [quality.score.unit]           NVARCHAR(100)       '$.score.unit',
        [quality.score.system]         VARCHAR(256)        '$.score.system',
        [quality.score.code]           NVARCHAR(4000)      '$.score.code',
        [quality.method.id]            NVARCHAR(100)       '$.method.id',
        [quality.method.extension]     NVARCHAR(MAX)       '$.method.extension',
        [quality.method.coding]        NVARCHAR(MAX)       '$.method.coding',
        [quality.method.text]          NVARCHAR(4000)      '$.method.text',
        [quality.truthTP]              float               '$.truthTP',
        [quality.queryTP]              float               '$.queryTP',
        [quality.truthFN]              float               '$.truthFN',
        [quality.queryFP]              float               '$.queryFP',
        [quality.gtFP]                 float               '$.gtFP',
        [quality.precision]            float               '$.precision',
        [quality.recall]               float               '$.recall',
        [quality.fScore]               float               '$.fScore',
        [quality.roc.id]               NVARCHAR(100)       '$.roc.id',
        [quality.roc.extension]        NVARCHAR(MAX)       '$.roc.extension',
        [quality.roc.modifierExtension] NVARCHAR(MAX)       '$.roc.modifierExtension',
        [quality.roc.score]            NVARCHAR(MAX)       '$.roc.score',
        [quality.roc.numTP]            NVARCHAR(MAX)       '$.roc.numTP',
        [quality.roc.numFP]            NVARCHAR(MAX)       '$.roc.numFP',
        [quality.roc.numFN]            NVARCHAR(MAX)       '$.roc.numFN',
        [quality.roc.precision]        NVARCHAR(MAX)       '$.roc.precision',
        [quality.roc.sensitivity]      NVARCHAR(MAX)       '$.roc.sensitivity',
        [quality.roc.fMeasure]         NVARCHAR(MAX)       '$.roc.fMeasure'
    ) j

GO

CREATE VIEW fhir.MolecularSequenceRepository AS
SELECT
    [id],
    [repository.JSON],
    [repository.id],
    [repository.extension],
    [repository.modifierExtension],
    [repository.type],
    [repository.url],
    [repository.name],
    [repository.datasetId],
    [repository.variantsetId],
    [repository.readsetId]
FROM openrowset (
        BULK 'MolecularSequence/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [repository.JSON]  VARCHAR(MAX) '$.repository'
    ) AS rowset
    CROSS APPLY openjson (rowset.[repository.JSON]) with (
        [repository.id]                NVARCHAR(100)       '$.id',
        [repository.extension]         NVARCHAR(MAX)       '$.extension',
        [repository.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [repository.type]              NVARCHAR(64)        '$.type',
        [repository.url]               VARCHAR(256)        '$.url',
        [repository.name]              NVARCHAR(500)       '$.name',
        [repository.datasetId]         NVARCHAR(100)       '$.datasetId',
        [repository.variantsetId]      NVARCHAR(100)       '$.variantsetId',
        [repository.readsetId]         NVARCHAR(100)       '$.readsetId'
    ) j

GO

CREATE VIEW fhir.MolecularSequencePointer AS
SELECT
    [id],
    [pointer.JSON],
    [pointer.id],
    [pointer.extension],
    [pointer.reference],
    [pointer.type],
    [pointer.identifier.id],
    [pointer.identifier.extension],
    [pointer.identifier.use],
    [pointer.identifier.type],
    [pointer.identifier.system],
    [pointer.identifier.value],
    [pointer.identifier.period],
    [pointer.identifier.assigner],
    [pointer.display]
FROM openrowset (
        BULK 'MolecularSequence/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [pointer.JSON]  VARCHAR(MAX) '$.pointer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[pointer.JSON]) with (
        [pointer.id]                   NVARCHAR(100)       '$.id',
        [pointer.extension]            NVARCHAR(MAX)       '$.extension',
        [pointer.reference]            NVARCHAR(4000)      '$.reference',
        [pointer.type]                 VARCHAR(256)        '$.type',
        [pointer.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [pointer.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [pointer.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [pointer.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [pointer.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [pointer.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [pointer.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [pointer.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [pointer.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MolecularSequenceStructureVariant AS
SELECT
    [id],
    [structureVariant.JSON],
    [structureVariant.id],
    [structureVariant.extension],
    [structureVariant.modifierExtension],
    [structureVariant.variantType.id],
    [structureVariant.variantType.extension],
    [structureVariant.variantType.coding],
    [structureVariant.variantType.text],
    [structureVariant.exact],
    [structureVariant.length],
    [structureVariant.outer.id],
    [structureVariant.outer.extension],
    [structureVariant.outer.modifierExtension],
    [structureVariant.outer.start],
    [structureVariant.outer.end],
    [structureVariant.inner.id],
    [structureVariant.inner.extension],
    [structureVariant.inner.modifierExtension],
    [structureVariant.inner.start],
    [structureVariant.inner.end]
FROM openrowset (
        BULK 'MolecularSequence/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [structureVariant.JSON]  VARCHAR(MAX) '$.structureVariant'
    ) AS rowset
    CROSS APPLY openjson (rowset.[structureVariant.JSON]) with (
        [structureVariant.id]          NVARCHAR(100)       '$.id',
        [structureVariant.extension]   NVARCHAR(MAX)       '$.extension',
        [structureVariant.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [structureVariant.variantType.id] NVARCHAR(100)       '$.variantType.id',
        [structureVariant.variantType.extension] NVARCHAR(MAX)       '$.variantType.extension',
        [structureVariant.variantType.coding] NVARCHAR(MAX)       '$.variantType.coding',
        [structureVariant.variantType.text] NVARCHAR(4000)      '$.variantType.text',
        [structureVariant.exact]       bit                 '$.exact',
        [structureVariant.length]      bigint              '$.length',
        [structureVariant.outer.id]    NVARCHAR(100)       '$.outer.id',
        [structureVariant.outer.extension] NVARCHAR(MAX)       '$.outer.extension',
        [structureVariant.outer.modifierExtension] NVARCHAR(MAX)       '$.outer.modifierExtension',
        [structureVariant.outer.start] bigint              '$.outer.start',
        [structureVariant.outer.end]   bigint              '$.outer.end',
        [structureVariant.inner.id]    NVARCHAR(100)       '$.inner.id',
        [structureVariant.inner.extension] NVARCHAR(MAX)       '$.inner.extension',
        [structureVariant.inner.modifierExtension] NVARCHAR(MAX)       '$.inner.modifierExtension',
        [structureVariant.inner.start] bigint              '$.inner.start',
        [structureVariant.inner.end]   bigint              '$.inner.end'
    ) j
