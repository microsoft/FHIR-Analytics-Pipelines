CREATE EXTERNAL TABLE [fhir].[Ingredient] (
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
    [status] NVARCHAR(100),
    [for] VARCHAR(MAX),
    [role.id] NVARCHAR(100),
    [role.extension] NVARCHAR(MAX),
    [role.coding] VARCHAR(MAX),
    [role.text] NVARCHAR(4000),
    [function] VARCHAR(MAX),
    [group.id] NVARCHAR(100),
    [group.extension] NVARCHAR(MAX),
    [group.coding] VARCHAR(MAX),
    [group.text] NVARCHAR(4000),
    [allergenicIndicator] bit,
    [manufacturer] VARCHAR(MAX),
    [substance.id] NVARCHAR(100),
    [substance.extension] NVARCHAR(MAX),
    [substance.modifierExtension] NVARCHAR(MAX),
    [substance.code.id] NVARCHAR(100),
    [substance.code.extension] NVARCHAR(MAX),
    [substance.code.concept] NVARCHAR(MAX),
    [substance.code.reference] NVARCHAR(MAX),
    [substance.strength] VARCHAR(MAX),
) WITH (
    LOCATION='/Ingredient/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.IngredientFor AS
SELECT
    [id],
    [for.JSON],
    [for.id],
    [for.extension],
    [for.reference],
    [for.type],
    [for.identifier.id],
    [for.identifier.extension],
    [for.identifier.use],
    [for.identifier.type],
    [for.identifier.system],
    [for.identifier.value],
    [for.identifier.period],
    [for.identifier.assigner],
    [for.display]
FROM openrowset (
        BULK 'Ingredient/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [for.JSON]  VARCHAR(MAX) '$.for'
    ) AS rowset
    CROSS APPLY openjson (rowset.[for.JSON]) with (
        [for.id]                       NVARCHAR(100)       '$.id',
        [for.extension]                NVARCHAR(MAX)       '$.extension',
        [for.reference]                NVARCHAR(4000)      '$.reference',
        [for.type]                     VARCHAR(256)        '$.type',
        [for.identifier.id]            NVARCHAR(100)       '$.identifier.id',
        [for.identifier.extension]     NVARCHAR(MAX)       '$.identifier.extension',
        [for.identifier.use]           NVARCHAR(64)        '$.identifier.use',
        [for.identifier.type]          NVARCHAR(MAX)       '$.identifier.type',
        [for.identifier.system]        VARCHAR(256)        '$.identifier.system',
        [for.identifier.value]         NVARCHAR(4000)      '$.identifier.value',
        [for.identifier.period]        NVARCHAR(MAX)       '$.identifier.period',
        [for.identifier.assigner]      NVARCHAR(MAX)       '$.identifier.assigner',
        [for.display]                  NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.IngredientFunction AS
SELECT
    [id],
    [function.JSON],
    [function.id],
    [function.extension],
    [function.coding],
    [function.text]
FROM openrowset (
        BULK 'Ingredient/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [function.JSON]  VARCHAR(MAX) '$.function'
    ) AS rowset
    CROSS APPLY openjson (rowset.[function.JSON]) with (
        [function.id]                  NVARCHAR(100)       '$.id',
        [function.extension]           NVARCHAR(MAX)       '$.extension',
        [function.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [function.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.IngredientManufacturer AS
SELECT
    [id],
    [manufacturer.JSON],
    [manufacturer.id],
    [manufacturer.extension],
    [manufacturer.modifierExtension],
    [manufacturer.role.id],
    [manufacturer.role.extension],
    [manufacturer.role.system],
    [manufacturer.role.version],
    [manufacturer.role.code],
    [manufacturer.role.display],
    [manufacturer.role.userSelected],
    [manufacturer.manufacturer.id],
    [manufacturer.manufacturer.extension],
    [manufacturer.manufacturer.reference],
    [manufacturer.manufacturer.type],
    [manufacturer.manufacturer.identifier],
    [manufacturer.manufacturer.display]
FROM openrowset (
        BULK 'Ingredient/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [manufacturer.JSON]  VARCHAR(MAX) '$.manufacturer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[manufacturer.JSON]) with (
        [manufacturer.id]              NVARCHAR(100)       '$.id',
        [manufacturer.extension]       NVARCHAR(MAX)       '$.extension',
        [manufacturer.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [manufacturer.role.id]         NVARCHAR(100)       '$.role.id',
        [manufacturer.role.extension]  NVARCHAR(MAX)       '$.role.extension',
        [manufacturer.role.system]     VARCHAR(256)        '$.role.system',
        [manufacturer.role.version]    NVARCHAR(100)       '$.role.version',
        [manufacturer.role.code]       NVARCHAR(4000)      '$.role.code',
        [manufacturer.role.display]    NVARCHAR(4000)      '$.role.display',
        [manufacturer.role.userSelected] bit                 '$.role.userSelected',
        [manufacturer.manufacturer.id] NVARCHAR(100)       '$.manufacturer.id',
        [manufacturer.manufacturer.extension] NVARCHAR(MAX)       '$.manufacturer.extension',
        [manufacturer.manufacturer.reference] NVARCHAR(4000)      '$.manufacturer.reference',
        [manufacturer.manufacturer.type] VARCHAR(256)        '$.manufacturer.type',
        [manufacturer.manufacturer.identifier] NVARCHAR(MAX)       '$.manufacturer.identifier',
        [manufacturer.manufacturer.display] NVARCHAR(4000)      '$.manufacturer.display'
    ) j
