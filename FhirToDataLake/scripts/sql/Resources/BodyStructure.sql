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
    [locationQualifier] VARCHAR(MAX),
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

CREATE VIEW fhir.BodyStructureLocationQualifier AS
SELECT
    [id],
    [locationQualifier.JSON],
    [locationQualifier.id],
    [locationQualifier.extension],
    [locationQualifier.coding],
    [locationQualifier.text]
FROM openrowset (
        BULK 'BodyStructure/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [locationQualifier.JSON]  VARCHAR(MAX) '$.locationQualifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[locationQualifier.JSON]) with (
        [locationQualifier.id]         NVARCHAR(100)       '$.id',
        [locationQualifier.extension]  NVARCHAR(MAX)       '$.extension',
        [locationQualifier.coding]     NVARCHAR(MAX)       '$.coding' AS JSON,
        [locationQualifier.text]       NVARCHAR(4000)      '$.text'
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
    [image.creation]
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
        [image.size]                   bigint              '$.size',
        [image.hash]                   NVARCHAR(MAX)       '$.hash',
        [image.title]                  NVARCHAR(4000)      '$.title',
        [image.creation]               VARCHAR(64)         '$.creation'
    ) j
