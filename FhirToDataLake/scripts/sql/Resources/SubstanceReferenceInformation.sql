CREATE EXTERNAL TABLE [fhir].[SubstanceReferenceInformation] (
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
    [comment] NVARCHAR(4000),
    [gene] VARCHAR(MAX),
    [geneElement] VARCHAR(MAX),
    [classification] VARCHAR(MAX),
    [target] VARCHAR(MAX),
) WITH (
    LOCATION='/SubstanceReferenceInformation/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SubstanceReferenceInformationGene AS
SELECT
    [id],
    [gene.JSON],
    [gene.id],
    [gene.extension],
    [gene.modifierExtension],
    [gene.geneSequenceOrigin.id],
    [gene.geneSequenceOrigin.extension],
    [gene.geneSequenceOrigin.coding],
    [gene.geneSequenceOrigin.text],
    [gene.gene.id],
    [gene.gene.extension],
    [gene.gene.coding],
    [gene.gene.text],
    [gene.source]
FROM openrowset (
        BULK 'SubstanceReferenceInformation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [gene.JSON]  VARCHAR(MAX) '$.gene'
    ) AS rowset
    CROSS APPLY openjson (rowset.[gene.JSON]) with (
        [gene.id]                      NVARCHAR(100)       '$.id',
        [gene.extension]               NVARCHAR(MAX)       '$.extension',
        [gene.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [gene.geneSequenceOrigin.id]   NVARCHAR(100)       '$.geneSequenceOrigin.id',
        [gene.geneSequenceOrigin.extension] NVARCHAR(MAX)       '$.geneSequenceOrigin.extension',
        [gene.geneSequenceOrigin.coding] NVARCHAR(MAX)       '$.geneSequenceOrigin.coding',
        [gene.geneSequenceOrigin.text] NVARCHAR(4000)      '$.geneSequenceOrigin.text',
        [gene.gene.id]                 NVARCHAR(100)       '$.gene.id',
        [gene.gene.extension]          NVARCHAR(MAX)       '$.gene.extension',
        [gene.gene.coding]             NVARCHAR(MAX)       '$.gene.coding',
        [gene.gene.text]               NVARCHAR(4000)      '$.gene.text',
        [gene.source]                  NVARCHAR(MAX)       '$.source' AS JSON
    ) j

GO

CREATE VIEW fhir.SubstanceReferenceInformationGeneElement AS
SELECT
    [id],
    [geneElement.JSON],
    [geneElement.id],
    [geneElement.extension],
    [geneElement.modifierExtension],
    [geneElement.type.id],
    [geneElement.type.extension],
    [geneElement.type.coding],
    [geneElement.type.text],
    [geneElement.element.id],
    [geneElement.element.extension],
    [geneElement.element.use],
    [geneElement.element.type],
    [geneElement.element.system],
    [geneElement.element.value],
    [geneElement.element.period],
    [geneElement.element.assigner],
    [geneElement.source]
FROM openrowset (
        BULK 'SubstanceReferenceInformation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [geneElement.JSON]  VARCHAR(MAX) '$.geneElement'
    ) AS rowset
    CROSS APPLY openjson (rowset.[geneElement.JSON]) with (
        [geneElement.id]               NVARCHAR(100)       '$.id',
        [geneElement.extension]        NVARCHAR(MAX)       '$.extension',
        [geneElement.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [geneElement.type.id]          NVARCHAR(100)       '$.type.id',
        [geneElement.type.extension]   NVARCHAR(MAX)       '$.type.extension',
        [geneElement.type.coding]      NVARCHAR(MAX)       '$.type.coding',
        [geneElement.type.text]        NVARCHAR(4000)      '$.type.text',
        [geneElement.element.id]       NVARCHAR(100)       '$.element.id',
        [geneElement.element.extension] NVARCHAR(MAX)       '$.element.extension',
        [geneElement.element.use]      NVARCHAR(64)        '$.element.use',
        [geneElement.element.type]     NVARCHAR(MAX)       '$.element.type',
        [geneElement.element.system]   VARCHAR(256)        '$.element.system',
        [geneElement.element.value]    NVARCHAR(4000)      '$.element.value',
        [geneElement.element.period]   NVARCHAR(MAX)       '$.element.period',
        [geneElement.element.assigner] NVARCHAR(MAX)       '$.element.assigner',
        [geneElement.source]           NVARCHAR(MAX)       '$.source' AS JSON
    ) j

GO

CREATE VIEW fhir.SubstanceReferenceInformationClassification AS
SELECT
    [id],
    [classification.JSON],
    [classification.id],
    [classification.extension],
    [classification.modifierExtension],
    [classification.domain.id],
    [classification.domain.extension],
    [classification.domain.coding],
    [classification.domain.text],
    [classification.classification.id],
    [classification.classification.extension],
    [classification.classification.coding],
    [classification.classification.text],
    [classification.subtype],
    [classification.source]
FROM openrowset (
        BULK 'SubstanceReferenceInformation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [classification.JSON]  VARCHAR(MAX) '$.classification'
    ) AS rowset
    CROSS APPLY openjson (rowset.[classification.JSON]) with (
        [classification.id]            NVARCHAR(100)       '$.id',
        [classification.extension]     NVARCHAR(MAX)       '$.extension',
        [classification.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [classification.domain.id]     NVARCHAR(100)       '$.domain.id',
        [classification.domain.extension] NVARCHAR(MAX)       '$.domain.extension',
        [classification.domain.coding] NVARCHAR(MAX)       '$.domain.coding',
        [classification.domain.text]   NVARCHAR(4000)      '$.domain.text',
        [classification.classification.id] NVARCHAR(100)       '$.classification.id',
        [classification.classification.extension] NVARCHAR(MAX)       '$.classification.extension',
        [classification.classification.coding] NVARCHAR(MAX)       '$.classification.coding',
        [classification.classification.text] NVARCHAR(4000)      '$.classification.text',
        [classification.subtype]       NVARCHAR(MAX)       '$.subtype' AS JSON,
        [classification.source]        NVARCHAR(MAX)       '$.source' AS JSON
    ) j

GO

CREATE VIEW fhir.SubstanceReferenceInformationTarget AS
SELECT
    [id],
    [target.JSON],
    [target.id],
    [target.extension],
    [target.modifierExtension],
    [target.target.id],
    [target.target.extension],
    [target.target.use],
    [target.target.type],
    [target.target.system],
    [target.target.value],
    [target.target.period],
    [target.target.assigner],
    [target.type.id],
    [target.type.extension],
    [target.type.coding],
    [target.type.text],
    [target.interaction.id],
    [target.interaction.extension],
    [target.interaction.coding],
    [target.interaction.text],
    [target.organism.id],
    [target.organism.extension],
    [target.organism.coding],
    [target.organism.text],
    [target.organismType.id],
    [target.organismType.extension],
    [target.organismType.coding],
    [target.organismType.text],
    [target.amountType.id],
    [target.amountType.extension],
    [target.amountType.coding],
    [target.amountType.text],
    [target.source],
    [target.amount.quantity.id],
    [target.amount.quantity.extension],
    [target.amount.quantity.value],
    [target.amount.quantity.comparator],
    [target.amount.quantity.unit],
    [target.amount.quantity.system],
    [target.amount.quantity.code],
    [target.amount.range.id],
    [target.amount.range.extension],
    [target.amount.range.low],
    [target.amount.range.high],
    [target.amount.string]
FROM openrowset (
        BULK 'SubstanceReferenceInformation/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [target.JSON]  VARCHAR(MAX) '$.target'
    ) AS rowset
    CROSS APPLY openjson (rowset.[target.JSON]) with (
        [target.id]                    NVARCHAR(100)       '$.id',
        [target.extension]             NVARCHAR(MAX)       '$.extension',
        [target.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [target.target.id]             NVARCHAR(100)       '$.target.id',
        [target.target.extension]      NVARCHAR(MAX)       '$.target.extension',
        [target.target.use]            NVARCHAR(64)        '$.target.use',
        [target.target.type]           NVARCHAR(MAX)       '$.target.type',
        [target.target.system]         VARCHAR(256)        '$.target.system',
        [target.target.value]          NVARCHAR(4000)      '$.target.value',
        [target.target.period]         NVARCHAR(MAX)       '$.target.period',
        [target.target.assigner]       NVARCHAR(MAX)       '$.target.assigner',
        [target.type.id]               NVARCHAR(100)       '$.type.id',
        [target.type.extension]        NVARCHAR(MAX)       '$.type.extension',
        [target.type.coding]           NVARCHAR(MAX)       '$.type.coding',
        [target.type.text]             NVARCHAR(4000)      '$.type.text',
        [target.interaction.id]        NVARCHAR(100)       '$.interaction.id',
        [target.interaction.extension] NVARCHAR(MAX)       '$.interaction.extension',
        [target.interaction.coding]    NVARCHAR(MAX)       '$.interaction.coding',
        [target.interaction.text]      NVARCHAR(4000)      '$.interaction.text',
        [target.organism.id]           NVARCHAR(100)       '$.organism.id',
        [target.organism.extension]    NVARCHAR(MAX)       '$.organism.extension',
        [target.organism.coding]       NVARCHAR(MAX)       '$.organism.coding',
        [target.organism.text]         NVARCHAR(4000)      '$.organism.text',
        [target.organismType.id]       NVARCHAR(100)       '$.organismType.id',
        [target.organismType.extension] NVARCHAR(MAX)       '$.organismType.extension',
        [target.organismType.coding]   NVARCHAR(MAX)       '$.organismType.coding',
        [target.organismType.text]     NVARCHAR(4000)      '$.organismType.text',
        [target.amountType.id]         NVARCHAR(100)       '$.amountType.id',
        [target.amountType.extension]  NVARCHAR(MAX)       '$.amountType.extension',
        [target.amountType.coding]     NVARCHAR(MAX)       '$.amountType.coding',
        [target.amountType.text]       NVARCHAR(4000)      '$.amountType.text',
        [target.source]                NVARCHAR(MAX)       '$.source' AS JSON,
        [target.amount.quantity.id]    NVARCHAR(100)       '$.amount.quantity.id',
        [target.amount.quantity.extension] NVARCHAR(MAX)       '$.amount.quantity.extension',
        [target.amount.quantity.value] float               '$.amount.quantity.value',
        [target.amount.quantity.comparator] NVARCHAR(64)        '$.amount.quantity.comparator',
        [target.amount.quantity.unit]  NVARCHAR(100)       '$.amount.quantity.unit',
        [target.amount.quantity.system] VARCHAR(256)        '$.amount.quantity.system',
        [target.amount.quantity.code]  NVARCHAR(4000)      '$.amount.quantity.code',
        [target.amount.range.id]       NVARCHAR(100)       '$.amount.range.id',
        [target.amount.range.extension] NVARCHAR(MAX)       '$.amount.range.extension',
        [target.amount.range.low]      NVARCHAR(MAX)       '$.amount.range.low',
        [target.amount.range.high]     NVARCHAR(MAX)       '$.amount.range.high',
        [target.amount.string]         NVARCHAR(4000)      '$.amount.string'
    ) j
