CREATE EXTERNAL TABLE [fhir].[SubstanceNucleicAcid] (
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
    [sequenceType.id] NVARCHAR(100),
    [sequenceType.extension] NVARCHAR(MAX),
    [sequenceType.coding] VARCHAR(MAX),
    [sequenceType.text] NVARCHAR(4000),
    [numberOfSubunits] bigint,
    [areaOfHybridisation] NVARCHAR(4000),
    [oligoNucleotideType.id] NVARCHAR(100),
    [oligoNucleotideType.extension] NVARCHAR(MAX),
    [oligoNucleotideType.coding] VARCHAR(MAX),
    [oligoNucleotideType.text] NVARCHAR(4000),
    [subunit] VARCHAR(MAX),
) WITH (
    LOCATION='/SubstanceNucleicAcid/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SubstanceNucleicAcidSubunit AS
SELECT
    [id],
    [subunit.JSON],
    [subunit.id],
    [subunit.extension],
    [subunit.modifierExtension],
    [subunit.subunit],
    [subunit.sequence],
    [subunit.length],
    [subunit.sequenceAttachment.id],
    [subunit.sequenceAttachment.extension],
    [subunit.sequenceAttachment.contentType],
    [subunit.sequenceAttachment.language],
    [subunit.sequenceAttachment.data],
    [subunit.sequenceAttachment.url],
    [subunit.sequenceAttachment.size],
    [subunit.sequenceAttachment.hash],
    [subunit.sequenceAttachment.title],
    [subunit.sequenceAttachment.creation],
    [subunit.fivePrime.id],
    [subunit.fivePrime.extension],
    [subunit.fivePrime.coding],
    [subunit.fivePrime.text],
    [subunit.threePrime.id],
    [subunit.threePrime.extension],
    [subunit.threePrime.coding],
    [subunit.threePrime.text],
    [subunit.linkage],
    [subunit.sugar]
FROM openrowset (
        BULK 'SubstanceNucleicAcid/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subunit.JSON]  VARCHAR(MAX) '$.subunit'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subunit.JSON]) with (
        [subunit.id]                   NVARCHAR(100)       '$.id',
        [subunit.extension]            NVARCHAR(MAX)       '$.extension',
        [subunit.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [subunit.subunit]              bigint              '$.subunit',
        [subunit.sequence]             NVARCHAR(4000)      '$.sequence',
        [subunit.length]               bigint              '$.length',
        [subunit.sequenceAttachment.id] NVARCHAR(100)       '$.sequenceAttachment.id',
        [subunit.sequenceAttachment.extension] NVARCHAR(MAX)       '$.sequenceAttachment.extension',
        [subunit.sequenceAttachment.contentType] NVARCHAR(100)       '$.sequenceAttachment.contentType',
        [subunit.sequenceAttachment.language] NVARCHAR(100)       '$.sequenceAttachment.language',
        [subunit.sequenceAttachment.data] NVARCHAR(MAX)       '$.sequenceAttachment.data',
        [subunit.sequenceAttachment.url] VARCHAR(256)        '$.sequenceAttachment.url',
        [subunit.sequenceAttachment.size] bigint              '$.sequenceAttachment.size',
        [subunit.sequenceAttachment.hash] NVARCHAR(MAX)       '$.sequenceAttachment.hash',
        [subunit.sequenceAttachment.title] NVARCHAR(4000)      '$.sequenceAttachment.title',
        [subunit.sequenceAttachment.creation] VARCHAR(64)         '$.sequenceAttachment.creation',
<<<<<<< HEAD
        [subunit.fivePrime.id]         NVARCHAR(100)       '$.fivePrime.id',
=======
        [subunit.fivePrime.id]         NVARCHAR(4000)      '$.fivePrime.id',
>>>>>>> origin/main
        [subunit.fivePrime.extension]  NVARCHAR(MAX)       '$.fivePrime.extension',
        [subunit.fivePrime.coding]     NVARCHAR(MAX)       '$.fivePrime.coding',
        [subunit.fivePrime.text]       NVARCHAR(4000)      '$.fivePrime.text',
        [subunit.threePrime.id]        NVARCHAR(100)       '$.threePrime.id',
        [subunit.threePrime.extension] NVARCHAR(MAX)       '$.threePrime.extension',
        [subunit.threePrime.coding]    NVARCHAR(MAX)       '$.threePrime.coding',
        [subunit.threePrime.text]      NVARCHAR(4000)      '$.threePrime.text',
        [subunit.linkage]              NVARCHAR(MAX)       '$.linkage' AS JSON,
        [subunit.sugar]                NVARCHAR(MAX)       '$.sugar' AS JSON
    ) j
