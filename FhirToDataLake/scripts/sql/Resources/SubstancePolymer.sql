CREATE EXTERNAL TABLE [fhir].[SubstancePolymer] (
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
    [class.id] NVARCHAR(100),
    [class.extension] NVARCHAR(MAX),
    [class.coding] VARCHAR(MAX),
    [class.text] NVARCHAR(4000),
    [geometry.id] NVARCHAR(100),
    [geometry.extension] NVARCHAR(MAX),
    [geometry.coding] VARCHAR(MAX),
    [geometry.text] NVARCHAR(4000),
    [copolymerConnectivity] VARCHAR(MAX),
    [modification] VARCHAR(MAX),
    [monomerSet] VARCHAR(MAX),
    [repeat] VARCHAR(MAX),
) WITH (
    LOCATION='/SubstancePolymer/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SubstancePolymerCopolymerConnectivity AS
SELECT
    [id],
    [copolymerConnectivity.JSON],
    [copolymerConnectivity.id],
    [copolymerConnectivity.extension],
    [copolymerConnectivity.coding],
    [copolymerConnectivity.text]
FROM openrowset (
        BULK 'SubstancePolymer/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [copolymerConnectivity.JSON]  VARCHAR(MAX) '$.copolymerConnectivity'
    ) AS rowset
    CROSS APPLY openjson (rowset.[copolymerConnectivity.JSON]) with (
        [copolymerConnectivity.id]     NVARCHAR(100)       '$.id',
        [copolymerConnectivity.extension] NVARCHAR(MAX)       '$.extension',
        [copolymerConnectivity.coding] NVARCHAR(MAX)       '$.coding' AS JSON,
        [copolymerConnectivity.text]   NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.SubstancePolymerModification AS
SELECT
    [id],
    [modification.JSON],
    [modification]
FROM openrowset (
        BULK 'SubstancePolymer/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [modification.JSON]  VARCHAR(MAX) '$.modification'
    ) AS rowset
    CROSS APPLY openjson (rowset.[modification.JSON]) with (
        [modification]                 NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.SubstancePolymerMonomerSet AS
SELECT
    [id],
    [monomerSet.JSON],
    [monomerSet.id],
    [monomerSet.extension],
    [monomerSet.modifierExtension],
    [monomerSet.ratioType.id],
    [monomerSet.ratioType.extension],
    [monomerSet.ratioType.coding],
    [monomerSet.ratioType.text],
    [monomerSet.startingMaterial]
FROM openrowset (
        BULK 'SubstancePolymer/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [monomerSet.JSON]  VARCHAR(MAX) '$.monomerSet'
    ) AS rowset
    CROSS APPLY openjson (rowset.[monomerSet.JSON]) with (
        [monomerSet.id]                NVARCHAR(100)       '$.id',
        [monomerSet.extension]         NVARCHAR(MAX)       '$.extension',
        [monomerSet.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [monomerSet.ratioType.id]      NVARCHAR(100)       '$.ratioType.id',
        [monomerSet.ratioType.extension] NVARCHAR(MAX)       '$.ratioType.extension',
        [monomerSet.ratioType.coding]  NVARCHAR(MAX)       '$.ratioType.coding',
        [monomerSet.ratioType.text]    NVARCHAR(4000)      '$.ratioType.text',
        [monomerSet.startingMaterial]  NVARCHAR(MAX)       '$.startingMaterial' AS JSON
    ) j

GO

CREATE VIEW fhir.SubstancePolymerRepeat AS
SELECT
    [id],
    [repeat.JSON],
    [repeat.id],
    [repeat.extension],
    [repeat.modifierExtension],
    [repeat.numberOfUnits],
    [repeat.averageMolecularFormula],
    [repeat.repeatUnitAmountType.id],
    [repeat.repeatUnitAmountType.extension],
    [repeat.repeatUnitAmountType.coding],
    [repeat.repeatUnitAmountType.text],
    [repeat.repeatUnit]
FROM openrowset (
        BULK 'SubstancePolymer/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [repeat.JSON]  VARCHAR(MAX) '$.repeat'
    ) AS rowset
    CROSS APPLY openjson (rowset.[repeat.JSON]) with (
        [repeat.id]                    NVARCHAR(100)       '$.id',
        [repeat.extension]             NVARCHAR(MAX)       '$.extension',
        [repeat.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [repeat.numberOfUnits]         bigint              '$.numberOfUnits',
        [repeat.averageMolecularFormula] NVARCHAR(500)       '$.averageMolecularFormula',
        [repeat.repeatUnitAmountType.id] NVARCHAR(100)       '$.repeatUnitAmountType.id',
        [repeat.repeatUnitAmountType.extension] NVARCHAR(MAX)       '$.repeatUnitAmountType.extension',
        [repeat.repeatUnitAmountType.coding] NVARCHAR(MAX)       '$.repeatUnitAmountType.coding',
        [repeat.repeatUnitAmountType.text] NVARCHAR(4000)      '$.repeatUnitAmountType.text',
        [repeat.repeatUnit]            NVARCHAR(MAX)       '$.repeatUnit' AS JSON
    ) j
