CREATE EXTERNAL TABLE [fhir].[MedicinalProductIngredient] (
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
    [identifier.id] NVARCHAR(100),
    [identifier.extension] NVARCHAR(MAX),
    [identifier.use] NVARCHAR(64),
    [identifier.type.id] NVARCHAR(100),
    [identifier.type.extension] NVARCHAR(MAX),
    [identifier.type.coding] NVARCHAR(MAX),
    [identifier.type.text] NVARCHAR(4000),
    [identifier.system] VARCHAR(256),
    [identifier.value] NVARCHAR(4000),
    [identifier.period.id] NVARCHAR(100),
    [identifier.period.extension] NVARCHAR(MAX),
    [identifier.period.start] VARCHAR(64),
    [identifier.period.end] VARCHAR(64),
    [identifier.assigner.id] NVARCHAR(100),
    [identifier.assigner.extension] NVARCHAR(MAX),
    [identifier.assigner.reference] NVARCHAR(4000),
    [identifier.assigner.type] VARCHAR(256),
    [identifier.assigner.identifier] NVARCHAR(MAX),
    [identifier.assigner.display] NVARCHAR(4000),
    [role.id] NVARCHAR(100),
    [role.extension] NVARCHAR(MAX),
    [role.coding] VARCHAR(MAX),
    [role.text] NVARCHAR(4000),
    [allergenicIndicator] bit,
    [manufacturer] VARCHAR(MAX),
    [specifiedSubstance] VARCHAR(MAX),
    [substance.id] NVARCHAR(100),
    [substance.extension] NVARCHAR(MAX),
    [substance.modifierExtension] NVARCHAR(MAX),
    [substance.code.id] NVARCHAR(100),
    [substance.code.extension] NVARCHAR(MAX),
    [substance.code.coding] NVARCHAR(MAX),
    [substance.code.text] NVARCHAR(4000),
    [substance.strength] VARCHAR(MAX),
) WITH (
    LOCATION='/MedicinalProductIngredient/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicinalProductIngredientManufacturer AS
SELECT
    [id],
    [manufacturer.JSON],
    [manufacturer.id],
    [manufacturer.extension],
    [manufacturer.reference],
    [manufacturer.type],
    [manufacturer.identifier.id],
    [manufacturer.identifier.extension],
    [manufacturer.identifier.use],
    [manufacturer.identifier.type],
    [manufacturer.identifier.system],
    [manufacturer.identifier.value],
    [manufacturer.identifier.period],
    [manufacturer.identifier.assigner],
    [manufacturer.display]
FROM openrowset (
        BULK 'MedicinalProductIngredient/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [manufacturer.JSON]  VARCHAR(MAX) '$.manufacturer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[manufacturer.JSON]) with (
        [manufacturer.id]              NVARCHAR(100)       '$.id',
        [manufacturer.extension]       NVARCHAR(MAX)       '$.extension',
        [manufacturer.reference]       NVARCHAR(4000)      '$.reference',
        [manufacturer.type]            VARCHAR(256)        '$.type',
        [manufacturer.identifier.id]   NVARCHAR(100)       '$.identifier.id',
        [manufacturer.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [manufacturer.identifier.use]  NVARCHAR(64)        '$.identifier.use',
        [manufacturer.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [manufacturer.identifier.system] VARCHAR(256)        '$.identifier.system',
        [manufacturer.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [manufacturer.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [manufacturer.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [manufacturer.display]         NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductIngredientSpecifiedSubstance AS
SELECT
    [id],
    [specifiedSubstance.JSON],
    [specifiedSubstance.id],
    [specifiedSubstance.extension],
    [specifiedSubstance.modifierExtension],
    [specifiedSubstance.code.id],
    [specifiedSubstance.code.extension],
    [specifiedSubstance.code.coding],
    [specifiedSubstance.code.text],
    [specifiedSubstance.group.id],
    [specifiedSubstance.group.extension],
    [specifiedSubstance.group.coding],
    [specifiedSubstance.group.text],
    [specifiedSubstance.confidentiality.id],
    [specifiedSubstance.confidentiality.extension],
    [specifiedSubstance.confidentiality.coding],
    [specifiedSubstance.confidentiality.text],
    [specifiedSubstance.strength]
FROM openrowset (
        BULK 'MedicinalProductIngredient/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specifiedSubstance.JSON]  VARCHAR(MAX) '$.specifiedSubstance'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specifiedSubstance.JSON]) with (
        [specifiedSubstance.id]        NVARCHAR(100)       '$.id',
        [specifiedSubstance.extension] NVARCHAR(MAX)       '$.extension',
        [specifiedSubstance.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [specifiedSubstance.code.id]   NVARCHAR(100)       '$.code.id',
        [specifiedSubstance.code.extension] NVARCHAR(MAX)       '$.code.extension',
        [specifiedSubstance.code.coding] NVARCHAR(MAX)       '$.code.coding',
        [specifiedSubstance.code.text] NVARCHAR(4000)      '$.code.text',
        [specifiedSubstance.group.id]  NVARCHAR(100)       '$.group.id',
        [specifiedSubstance.group.extension] NVARCHAR(MAX)       '$.group.extension',
        [specifiedSubstance.group.coding] NVARCHAR(MAX)       '$.group.coding',
        [specifiedSubstance.group.text] NVARCHAR(4000)      '$.group.text',
        [specifiedSubstance.confidentiality.id] NVARCHAR(100)       '$.confidentiality.id',
        [specifiedSubstance.confidentiality.extension] NVARCHAR(MAX)       '$.confidentiality.extension',
        [specifiedSubstance.confidentiality.coding] NVARCHAR(MAX)       '$.confidentiality.coding',
        [specifiedSubstance.confidentiality.text] NVARCHAR(4000)      '$.confidentiality.text',
        [specifiedSubstance.strength]  NVARCHAR(MAX)       '$.strength' AS JSON
    ) j
