CREATE EXTERNAL TABLE [fhir].[SubstanceSourceMaterial] (
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
    [sourceMaterialClass.id] NVARCHAR(100),
    [sourceMaterialClass.extension] NVARCHAR(MAX),
    [sourceMaterialClass.coding] VARCHAR(MAX),
    [sourceMaterialClass.text] NVARCHAR(4000),
    [sourceMaterialType.id] NVARCHAR(100),
    [sourceMaterialType.extension] NVARCHAR(MAX),
    [sourceMaterialType.coding] VARCHAR(MAX),
    [sourceMaterialType.text] NVARCHAR(4000),
    [sourceMaterialState.id] NVARCHAR(100),
    [sourceMaterialState.extension] NVARCHAR(MAX),
    [sourceMaterialState.coding] VARCHAR(MAX),
    [sourceMaterialState.text] NVARCHAR(4000),
    [organismId.id] NVARCHAR(100),
    [organismId.extension] NVARCHAR(MAX),
    [organismId.use] NVARCHAR(64),
    [organismId.type.id] NVARCHAR(100),
    [organismId.type.extension] NVARCHAR(MAX),
    [organismId.type.coding] NVARCHAR(MAX),
    [organismId.type.text] NVARCHAR(4000),
    [organismId.system] VARCHAR(256),
    [organismId.value] NVARCHAR(4000),
    [organismId.period.id] NVARCHAR(100),
    [organismId.period.extension] NVARCHAR(MAX),
    [organismId.period.start] VARCHAR(64),
    [organismId.period.end] VARCHAR(64),
    [organismId.assigner.id] NVARCHAR(100),
    [organismId.assigner.extension] NVARCHAR(MAX),
    [organismId.assigner.reference] NVARCHAR(4000),
    [organismId.assigner.type] VARCHAR(256),
    [organismId.assigner.identifier] NVARCHAR(MAX),
    [organismId.assigner.display] NVARCHAR(4000),
    [organismName] NVARCHAR(500),
    [parentSubstanceId] VARCHAR(MAX),
    [parentSubstanceName] VARCHAR(MAX),
    [countryOfOrigin] VARCHAR(MAX),
    [geographicalLocation] VARCHAR(MAX),
    [developmentStage.id] NVARCHAR(100),
    [developmentStage.extension] NVARCHAR(MAX),
    [developmentStage.coding] VARCHAR(MAX),
    [developmentStage.text] NVARCHAR(4000),
    [fractionDescription] VARCHAR(MAX),
    [organism.id] NVARCHAR(100),
    [organism.extension] NVARCHAR(MAX),
    [organism.modifierExtension] NVARCHAR(MAX),
    [organism.family.id] NVARCHAR(100),
    [organism.family.extension] NVARCHAR(MAX),
    [organism.family.coding] NVARCHAR(MAX),
    [organism.family.text] NVARCHAR(4000),
    [organism.genus.id] NVARCHAR(100),
    [organism.genus.extension] NVARCHAR(MAX),
    [organism.genus.coding] NVARCHAR(MAX),
    [organism.genus.text] NVARCHAR(4000),
    [organism.species.id] NVARCHAR(100),
    [organism.species.extension] NVARCHAR(MAX),
    [organism.species.coding] NVARCHAR(MAX),
    [organism.species.text] NVARCHAR(4000),
    [organism.intraspecificType.id] NVARCHAR(100),
    [organism.intraspecificType.extension] NVARCHAR(MAX),
    [organism.intraspecificType.coding] NVARCHAR(MAX),
    [organism.intraspecificType.text] NVARCHAR(4000),
    [organism.intraspecificDescription] NVARCHAR(4000),
    [organism.author] VARCHAR(MAX),
    [organism.hybrid.id] NVARCHAR(100),
    [organism.hybrid.extension] NVARCHAR(MAX),
    [organism.hybrid.modifierExtension] NVARCHAR(MAX),
    [organism.hybrid.maternalOrganismId] NVARCHAR(100),
    [organism.hybrid.maternalOrganismName] NVARCHAR(500),
    [organism.hybrid.paternalOrganismId] NVARCHAR(100),
    [organism.hybrid.paternalOrganismName] NVARCHAR(500),
    [organism.hybrid.hybridType] NVARCHAR(MAX),
    [organism.organismGeneral.id] NVARCHAR(100),
    [organism.organismGeneral.extension] NVARCHAR(MAX),
    [organism.organismGeneral.modifierExtension] NVARCHAR(MAX),
    [organism.organismGeneral.kingdom] NVARCHAR(MAX),
    [organism.organismGeneral.phylum] NVARCHAR(MAX),
    [organism.organismGeneral.class] NVARCHAR(MAX),
    [organism.organismGeneral.order] NVARCHAR(MAX),
    [partDescription] VARCHAR(MAX),
) WITH (
    LOCATION='/SubstanceSourceMaterial/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SubstanceSourceMaterialParentSubstanceId AS
SELECT
    [id],
    [parentSubstanceId.JSON],
    [parentSubstanceId.id],
    [parentSubstanceId.extension],
    [parentSubstanceId.use],
    [parentSubstanceId.type.id],
    [parentSubstanceId.type.extension],
    [parentSubstanceId.type.coding],
    [parentSubstanceId.type.text],
    [parentSubstanceId.system],
    [parentSubstanceId.value],
    [parentSubstanceId.period.id],
    [parentSubstanceId.period.extension],
    [parentSubstanceId.period.start],
    [parentSubstanceId.period.end],
    [parentSubstanceId.assigner.id],
    [parentSubstanceId.assigner.extension],
    [parentSubstanceId.assigner.reference],
    [parentSubstanceId.assigner.type],
    [parentSubstanceId.assigner.identifier],
    [parentSubstanceId.assigner.display]
FROM openrowset (
        BULK 'SubstanceSourceMaterial/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [parentSubstanceId.JSON]  VARCHAR(MAX) '$.parentSubstanceId'
    ) AS rowset
    CROSS APPLY openjson (rowset.[parentSubstanceId.JSON]) with (
        [parentSubstanceId.id]         NVARCHAR(100)       '$.id',
        [parentSubstanceId.extension]  NVARCHAR(MAX)       '$.extension',
        [parentSubstanceId.use]        NVARCHAR(64)        '$.use',
        [parentSubstanceId.type.id]    NVARCHAR(100)       '$.type.id',
        [parentSubstanceId.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [parentSubstanceId.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [parentSubstanceId.type.text]  NVARCHAR(4000)      '$.type.text',
        [parentSubstanceId.system]     VARCHAR(256)        '$.system',
        [parentSubstanceId.value]      NVARCHAR(4000)      '$.value',
        [parentSubstanceId.period.id]  NVARCHAR(100)       '$.period.id',
        [parentSubstanceId.period.extension] NVARCHAR(MAX)       '$.period.extension',
        [parentSubstanceId.period.start] VARCHAR(64)         '$.period.start',
        [parentSubstanceId.period.end] VARCHAR(64)         '$.period.end',
        [parentSubstanceId.assigner.id] NVARCHAR(100)       '$.assigner.id',
        [parentSubstanceId.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [parentSubstanceId.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [parentSubstanceId.assigner.type] VARCHAR(256)        '$.assigner.type',
        [parentSubstanceId.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [parentSubstanceId.assigner.display] NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.SubstanceSourceMaterialParentSubstanceName AS
SELECT
    [id],
    [parentSubstanceName.JSON],
    [parentSubstanceName]
FROM openrowset (
        BULK 'SubstanceSourceMaterial/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [parentSubstanceName.JSON]  VARCHAR(MAX) '$.parentSubstanceName'
    ) AS rowset
    CROSS APPLY openjson (rowset.[parentSubstanceName.JSON]) with (
        [parentSubstanceName]          NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.SubstanceSourceMaterialCountryOfOrigin AS
SELECT
    [id],
    [countryOfOrigin.JSON],
    [countryOfOrigin.id],
    [countryOfOrigin.extension],
    [countryOfOrigin.coding],
    [countryOfOrigin.text]
FROM openrowset (
        BULK 'SubstanceSourceMaterial/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [countryOfOrigin.JSON]  VARCHAR(MAX) '$.countryOfOrigin'
    ) AS rowset
    CROSS APPLY openjson (rowset.[countryOfOrigin.JSON]) with (
        [countryOfOrigin.id]           NVARCHAR(100)       '$.id',
        [countryOfOrigin.extension]    NVARCHAR(MAX)       '$.extension',
        [countryOfOrigin.coding]       NVARCHAR(MAX)       '$.coding' AS JSON,
        [countryOfOrigin.text]         NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.SubstanceSourceMaterialGeographicalLocation AS
SELECT
    [id],
    [geographicalLocation.JSON],
    [geographicalLocation]
FROM openrowset (
        BULK 'SubstanceSourceMaterial/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [geographicalLocation.JSON]  VARCHAR(MAX) '$.geographicalLocation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[geographicalLocation.JSON]) with (
        [geographicalLocation]         NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.SubstanceSourceMaterialFractionDescription AS
SELECT
    [id],
    [fractionDescription.JSON],
    [fractionDescription.id],
    [fractionDescription.extension],
    [fractionDescription.modifierExtension],
    [fractionDescription.fraction],
    [fractionDescription.materialType.id],
    [fractionDescription.materialType.extension],
    [fractionDescription.materialType.coding],
    [fractionDescription.materialType.text]
FROM openrowset (
        BULK 'SubstanceSourceMaterial/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [fractionDescription.JSON]  VARCHAR(MAX) '$.fractionDescription'
    ) AS rowset
    CROSS APPLY openjson (rowset.[fractionDescription.JSON]) with (
        [fractionDescription.id]       NVARCHAR(100)       '$.id',
        [fractionDescription.extension] NVARCHAR(MAX)       '$.extension',
        [fractionDescription.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [fractionDescription.fraction] NVARCHAR(100)       '$.fraction',
        [fractionDescription.materialType.id] NVARCHAR(100)       '$.materialType.id',
        [fractionDescription.materialType.extension] NVARCHAR(MAX)       '$.materialType.extension',
        [fractionDescription.materialType.coding] NVARCHAR(MAX)       '$.materialType.coding',
        [fractionDescription.materialType.text] NVARCHAR(4000)      '$.materialType.text'
    ) j

GO

CREATE VIEW fhir.SubstanceSourceMaterialPartDescription AS
SELECT
    [id],
    [partDescription.JSON],
    [partDescription.id],
    [partDescription.extension],
    [partDescription.modifierExtension],
    [partDescription.part.id],
    [partDescription.part.extension],
    [partDescription.part.coding],
    [partDescription.part.text],
    [partDescription.partLocation.id],
    [partDescription.partLocation.extension],
    [partDescription.partLocation.coding],
    [partDescription.partLocation.text]
FROM openrowset (
        BULK 'SubstanceSourceMaterial/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [partDescription.JSON]  VARCHAR(MAX) '$.partDescription'
    ) AS rowset
    CROSS APPLY openjson (rowset.[partDescription.JSON]) with (
        [partDescription.id]           NVARCHAR(100)       '$.id',
        [partDescription.extension]    NVARCHAR(MAX)       '$.extension',
        [partDescription.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [partDescription.part.id]      NVARCHAR(100)       '$.part.id',
        [partDescription.part.extension] NVARCHAR(MAX)       '$.part.extension',
        [partDescription.part.coding]  NVARCHAR(MAX)       '$.part.coding',
        [partDescription.part.text]    NVARCHAR(4000)      '$.part.text',
        [partDescription.partLocation.id] NVARCHAR(100)       '$.partLocation.id',
        [partDescription.partLocation.extension] NVARCHAR(MAX)       '$.partLocation.extension',
        [partDescription.partLocation.coding] NVARCHAR(MAX)       '$.partLocation.coding',
        [partDescription.partLocation.text] NVARCHAR(4000)      '$.partLocation.text'
    ) j
