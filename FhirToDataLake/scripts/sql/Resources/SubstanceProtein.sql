CREATE EXTERNAL TABLE [fhir].[SubstanceProtein] (
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
    [disulfideLinkage] VARCHAR(MAX),
    [subunit] VARCHAR(MAX),
) WITH (
    LOCATION='/SubstanceProtein/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SubstanceProteinDisulfideLinkage AS
SELECT
    [id],
    [disulfideLinkage.JSON],
    [disulfideLinkage]
FROM openrowset (
        BULK 'SubstanceProtein/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [disulfideLinkage.JSON]  VARCHAR(MAX) '$.disulfideLinkage'
    ) AS rowset
    CROSS APPLY openjson (rowset.[disulfideLinkage.JSON]) with (
        [disulfideLinkage]             NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.SubstanceProteinSubunit AS
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
    [subunit.nTerminalModificationId.id],
    [subunit.nTerminalModificationId.extension],
    [subunit.nTerminalModificationId.use],
    [subunit.nTerminalModificationId.type],
    [subunit.nTerminalModificationId.system],
    [subunit.nTerminalModificationId.value],
    [subunit.nTerminalModificationId.period],
    [subunit.nTerminalModificationId.assigner],
    [subunit.nTerminalModification],
    [subunit.cTerminalModificationId.id],
    [subunit.cTerminalModificationId.extension],
    [subunit.cTerminalModificationId.use],
    [subunit.cTerminalModificationId.type],
    [subunit.cTerminalModificationId.system],
    [subunit.cTerminalModificationId.value],
    [subunit.cTerminalModificationId.period],
    [subunit.cTerminalModificationId.assigner],
    [subunit.cTerminalModification]
FROM openrowset (
        BULK 'SubstanceProtein/**',
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
        [subunit.nTerminalModificationId.id] NVARCHAR(100)       '$.nTerminalModificationId.id',
        [subunit.nTerminalModificationId.extension] NVARCHAR(MAX)       '$.nTerminalModificationId.extension',
        [subunit.nTerminalModificationId.use] NVARCHAR(64)        '$.nTerminalModificationId.use',
        [subunit.nTerminalModificationId.type] NVARCHAR(MAX)       '$.nTerminalModificationId.type',
        [subunit.nTerminalModificationId.system] VARCHAR(256)        '$.nTerminalModificationId.system',
        [subunit.nTerminalModificationId.value] NVARCHAR(4000)      '$.nTerminalModificationId.value',
        [subunit.nTerminalModificationId.period] NVARCHAR(MAX)       '$.nTerminalModificationId.period',
        [subunit.nTerminalModificationId.assigner] NVARCHAR(MAX)       '$.nTerminalModificationId.assigner',
        [subunit.nTerminalModification] NVARCHAR(500)       '$.nTerminalModification',
        [subunit.cTerminalModificationId.id] NVARCHAR(100)       '$.cTerminalModificationId.id',
        [subunit.cTerminalModificationId.extension] NVARCHAR(MAX)       '$.cTerminalModificationId.extension',
        [subunit.cTerminalModificationId.use] NVARCHAR(64)        '$.cTerminalModificationId.use',
        [subunit.cTerminalModificationId.type] NVARCHAR(MAX)       '$.cTerminalModificationId.type',
        [subunit.cTerminalModificationId.system] VARCHAR(256)        '$.cTerminalModificationId.system',
        [subunit.cTerminalModificationId.value] NVARCHAR(4000)      '$.cTerminalModificationId.value',
        [subunit.cTerminalModificationId.period] NVARCHAR(MAX)       '$.cTerminalModificationId.period',
        [subunit.cTerminalModificationId.assigner] NVARCHAR(MAX)       '$.cTerminalModificationId.assigner',
        [subunit.cTerminalModification] NVARCHAR(500)       '$.cTerminalModification'
    ) j
