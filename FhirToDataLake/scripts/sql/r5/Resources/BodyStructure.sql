CREATE EXTERNAL TABLE [fhir].[BodyStructure] (
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
    [active] bit,
    [morphology.id] NVARCHAR(100),
    [morphology.extension] NVARCHAR(MAX),
    [morphology.coding] VARCHAR(MAX),
    [morphology.text] NVARCHAR(4000),
    [location.id] NVARCHAR(100),
    [location.extension] NVARCHAR(MAX),
    [location.coding] VARCHAR(MAX),
    [location.text] NVARCHAR(4000),
    [includedStructure] VARCHAR(MAX),
    [excludedStructure] VARCHAR(MAX),
    [description] NVARCHAR(4000),
    [image] VARCHAR(MAX),
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
) WITH (
    LOCATION='/BodyStructure/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.BodyStructureIdentifier AS
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
        BULK 'BodyStructure/**',
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

CREATE VIEW fhir.BodyStructureIncludedStructure AS
SELECT
    [id],
    [includedStructure.JSON],
    [includedStructure.id],
    [includedStructure.extension],
    [includedStructure.modifierExtension],
    [includedStructure.structure.id],
    [includedStructure.structure.extension],
    [includedStructure.structure.coding],
    [includedStructure.structure.text],
    [includedStructure.laterality.id],
    [includedStructure.laterality.extension],
    [includedStructure.laterality.coding],
    [includedStructure.laterality.text],
    [includedStructure.qualifier]
FROM openrowset (
        BULK 'BodyStructure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [includedStructure.JSON]  VARCHAR(MAX) '$.includedStructure'
    ) AS rowset
    CROSS APPLY openjson (rowset.[includedStructure.JSON]) with (
        [includedStructure.id]         NVARCHAR(100)       '$.id',
        [includedStructure.extension]  NVARCHAR(MAX)       '$.extension',
        [includedStructure.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [includedStructure.structure.id] NVARCHAR(100)       '$.structure.id',
        [includedStructure.structure.extension] NVARCHAR(MAX)       '$.structure.extension',
        [includedStructure.structure.coding] NVARCHAR(MAX)       '$.structure.coding',
        [includedStructure.structure.text] NVARCHAR(4000)      '$.structure.text',
        [includedStructure.laterality.id] NVARCHAR(100)       '$.laterality.id',
        [includedStructure.laterality.extension] NVARCHAR(MAX)       '$.laterality.extension',
        [includedStructure.laterality.coding] NVARCHAR(MAX)       '$.laterality.coding',
        [includedStructure.laterality.text] NVARCHAR(4000)      '$.laterality.text',
        [includedStructure.qualifier]  NVARCHAR(MAX)       '$.qualifier' AS JSON
    ) j

GO

CREATE VIEW fhir.BodyStructureExcludedStructure AS
SELECT
    [id],
    [excludedStructure.JSON],
    [excludedStructure.id],
    [excludedStructure.extension],
    [excludedStructure.modifierExtension],
    [excludedStructure.structure.id],
    [excludedStructure.structure.extension],
    [excludedStructure.structure.coding],
    [excludedStructure.structure.text],
    [excludedStructure.laterality.id],
    [excludedStructure.laterality.extension],
    [excludedStructure.laterality.coding],
    [excludedStructure.laterality.text],
    [excludedStructure.qualifier]
FROM openrowset (
        BULK 'BodyStructure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [excludedStructure.JSON]  VARCHAR(MAX) '$.excludedStructure'
    ) AS rowset
    CROSS APPLY openjson (rowset.[excludedStructure.JSON]) with (
        [excludedStructure.id]         NVARCHAR(100)       '$.id',
        [excludedStructure.extension]  NVARCHAR(MAX)       '$.extension',
        [excludedStructure.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [excludedStructure.structure.id] NVARCHAR(100)       '$.structure.id',
        [excludedStructure.structure.extension] NVARCHAR(MAX)       '$.structure.extension',
        [excludedStructure.structure.coding] NVARCHAR(MAX)       '$.structure.coding',
        [excludedStructure.structure.text] NVARCHAR(4000)      '$.structure.text',
        [excludedStructure.laterality.id] NVARCHAR(100)       '$.laterality.id',
        [excludedStructure.laterality.extension] NVARCHAR(MAX)       '$.laterality.extension',
        [excludedStructure.laterality.coding] NVARCHAR(MAX)       '$.laterality.coding',
        [excludedStructure.laterality.text] NVARCHAR(4000)      '$.laterality.text',
        [excludedStructure.qualifier]  NVARCHAR(MAX)       '$.qualifier' AS JSON
    ) j

GO

CREATE VIEW fhir.BodyStructureImage AS
SELECT
    [id],
    [image.JSON],
    [image.id],
    [image.extension],
    [image.contentType],
    [image.language],
    [image.data],
    [image.url],
    [image.size],
    [image.hash],
    [image.title],
    [image.creation],
    [image.height],
    [image.width],
    [image.frames],
    [image.duration],
    [image.pages]
FROM openrowset (
        BULK 'BodyStructure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [image.JSON]  VARCHAR(MAX) '$.image'
    ) AS rowset
    CROSS APPLY openjson (rowset.[image.JSON]) with (
        [image.id]                     NVARCHAR(100)       '$.id',
        [image.extension]              NVARCHAR(MAX)       '$.extension',
        [image.contentType]            NVARCHAR(100)       '$.contentType',
        [image.language]               NVARCHAR(100)       '$.language',
        [image.data]                   NVARCHAR(MAX)       '$.data',
        [image.url]                    VARCHAR(256)        '$.url',
        [image.size]                   NVARCHAR(MAX)       '$.size',
        [image.hash]                   NVARCHAR(MAX)       '$.hash',
        [image.title]                  NVARCHAR(4000)      '$.title',
        [image.creation]               VARCHAR(64)         '$.creation',
        [image.height]                 bigint              '$.height',
        [image.width]                  bigint              '$.width',
        [image.frames]                 bigint              '$.frames',
        [image.duration]               float               '$.duration',
        [image.pages]                  bigint              '$.pages'
    ) j
