CREATE EXTERNAL TABLE [fhir].[SpecimenDefinition] (
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
    [typeCollected.id] NVARCHAR(100),
    [typeCollected.extension] NVARCHAR(MAX),
    [typeCollected.coding] VARCHAR(MAX),
    [typeCollected.text] NVARCHAR(4000),
    [patientPreparation] VARCHAR(MAX),
    [timeAspect] NVARCHAR(100),
    [collection] VARCHAR(MAX),
    [typeTested] VARCHAR(MAX),
) WITH (
    LOCATION='/SpecimenDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SpecimenDefinitionPatientPreparation AS
SELECT
    [id],
    [patientPreparation.JSON],
    [patientPreparation.id],
    [patientPreparation.extension],
    [patientPreparation.coding],
    [patientPreparation.text]
FROM openrowset (
        BULK 'SpecimenDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [patientPreparation.JSON]  VARCHAR(MAX) '$.patientPreparation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[patientPreparation.JSON]) with (
        [patientPreparation.id]        NVARCHAR(100)       '$.id',
        [patientPreparation.extension] NVARCHAR(MAX)       '$.extension',
        [patientPreparation.coding]    NVARCHAR(MAX)       '$.coding' AS JSON,
        [patientPreparation.text]      NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.SpecimenDefinitionCollection AS
SELECT
    [id],
    [collection.JSON],
    [collection.id],
    [collection.extension],
    [collection.coding],
    [collection.text]
FROM openrowset (
        BULK 'SpecimenDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [collection.JSON]  VARCHAR(MAX) '$.collection'
    ) AS rowset
    CROSS APPLY openjson (rowset.[collection.JSON]) with (
        [collection.id]                NVARCHAR(100)       '$.id',
        [collection.extension]         NVARCHAR(MAX)       '$.extension',
        [collection.coding]            NVARCHAR(MAX)       '$.coding' AS JSON,
        [collection.text]              NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.SpecimenDefinitionTypeTested AS
SELECT
    [id],
    [typeTested.JSON],
    [typeTested.id],
    [typeTested.extension],
    [typeTested.modifierExtension],
    [typeTested.isDerived],
    [typeTested.type.id],
    [typeTested.type.extension],
    [typeTested.type.coding],
    [typeTested.type.text],
    [typeTested.preference],
    [typeTested.container.id],
    [typeTested.container.extension],
    [typeTested.container.modifierExtension],
    [typeTested.container.material],
    [typeTested.container.type],
    [typeTested.container.cap],
    [typeTested.container.description],
    [typeTested.container.capacity],
    [typeTested.container.minimumVolumeQuantity],
    [typeTested.container.additive],
    [typeTested.container.preparation],
    [typeTested.container.minimumVolume.quantity],
    [typeTested.container.minimumVolume.string],
    [typeTested.requirement],
    [typeTested.retentionTime.id],
    [typeTested.retentionTime.extension],
    [typeTested.retentionTime.value],
    [typeTested.retentionTime.comparator],
    [typeTested.retentionTime.unit],
    [typeTested.retentionTime.system],
    [typeTested.retentionTime.code],
    [typeTested.rejectionCriterion],
    [typeTested.handling]
FROM openrowset (
        BULK 'SpecimenDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [typeTested.JSON]  VARCHAR(MAX) '$.typeTested'
    ) AS rowset
    CROSS APPLY openjson (rowset.[typeTested.JSON]) with (
        [typeTested.id]                NVARCHAR(100)       '$.id',
        [typeTested.extension]         NVARCHAR(MAX)       '$.extension',
        [typeTested.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [typeTested.isDerived]         bit                 '$.isDerived',
        [typeTested.type.id]           NVARCHAR(100)       '$.type.id',
        [typeTested.type.extension]    NVARCHAR(MAX)       '$.type.extension',
        [typeTested.type.coding]       NVARCHAR(MAX)       '$.type.coding',
        [typeTested.type.text]         NVARCHAR(4000)      '$.type.text',
        [typeTested.preference]        NVARCHAR(64)        '$.preference',
        [typeTested.container.id]      NVARCHAR(100)       '$.container.id',
        [typeTested.container.extension] NVARCHAR(MAX)       '$.container.extension',
        [typeTested.container.modifierExtension] NVARCHAR(MAX)       '$.container.modifierExtension',
        [typeTested.container.material] NVARCHAR(MAX)       '$.container.material',
        [typeTested.container.type]    NVARCHAR(MAX)       '$.container.type',
        [typeTested.container.cap]     NVARCHAR(MAX)       '$.container.cap',
        [typeTested.container.description] NVARCHAR(4000)      '$.container.description',
        [typeTested.container.capacity] NVARCHAR(MAX)       '$.container.capacity',
        [typeTested.container.minimumVolumeQuantity] NVARCHAR(MAX)       '$.container.minimumVolumeQuantity',
        [typeTested.container.additive] NVARCHAR(MAX)       '$.container.additive',
        [typeTested.container.preparation] NVARCHAR(4000)      '$.container.preparation',
        [typeTested.container.minimumVolume.quantity] NVARCHAR(MAX)       '$.container.minimumVolume.quantity',
        [typeTested.container.minimumVolume.string] NVARCHAR(4000)      '$.container.minimumVolume.string',
        [typeTested.requirement]       NVARCHAR(500)       '$.requirement',
        [typeTested.retentionTime.id]  NVARCHAR(100)       '$.retentionTime.id',
        [typeTested.retentionTime.extension] NVARCHAR(MAX)       '$.retentionTime.extension',
        [typeTested.retentionTime.value] float               '$.retentionTime.value',
        [typeTested.retentionTime.comparator] NVARCHAR(64)        '$.retentionTime.comparator',
        [typeTested.retentionTime.unit] NVARCHAR(100)       '$.retentionTime.unit',
        [typeTested.retentionTime.system] VARCHAR(256)        '$.retentionTime.system',
        [typeTested.retentionTime.code] NVARCHAR(4000)      '$.retentionTime.code',
        [typeTested.rejectionCriterion] NVARCHAR(MAX)       '$.rejectionCriterion' AS JSON,
        [typeTested.handling]          NVARCHAR(MAX)       '$.handling' AS JSON
    ) j
