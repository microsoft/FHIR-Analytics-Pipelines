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
    [url] VARCHAR(256),
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
    [version] NVARCHAR(100),
    [title] NVARCHAR(4000),
    [derivedFromCanonical] VARCHAR(MAX),
    [derivedFromUri] VARCHAR(MAX),
    [status] NVARCHAR(100),
    [experimental] bit,
    [date] VARCHAR(64),
    [publisher.id] NVARCHAR(100),
    [publisher.extension] NVARCHAR(MAX),
    [publisher.reference] NVARCHAR(4000),
    [publisher.type] VARCHAR(256),
    [publisher.identifier.id] NVARCHAR(100),
    [publisher.identifier.extension] NVARCHAR(MAX),
    [publisher.identifier.use] NVARCHAR(64),
    [publisher.identifier.type] NVARCHAR(MAX),
    [publisher.identifier.system] VARCHAR(256),
    [publisher.identifier.value] NVARCHAR(4000),
    [publisher.identifier.period] NVARCHAR(MAX),
    [publisher.identifier.assigner] NVARCHAR(MAX),
    [publisher.display] NVARCHAR(4000),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [purpose] NVARCHAR(MAX),
    [copyright] NVARCHAR(MAX),
    [approvalDate] VARCHAR(64),
    [lastReviewDate] VARCHAR(64),
    [effectivePeriod.id] NVARCHAR(100),
    [effectivePeriod.extension] NVARCHAR(MAX),
    [effectivePeriod.start] VARCHAR(64),
    [effectivePeriod.end] VARCHAR(64),
    [typeCollected.id] NVARCHAR(100),
    [typeCollected.extension] NVARCHAR(MAX),
    [typeCollected.coding] VARCHAR(MAX),
    [typeCollected.text] NVARCHAR(4000),
    [patientPreparation] VARCHAR(MAX),
    [timeAspect] NVARCHAR(100),
    [collection] VARCHAR(MAX),
    [typeTested] VARCHAR(MAX),
    [subject.codeableConcept.id] NVARCHAR(100),
    [subject.codeableConcept.extension] NVARCHAR(MAX),
    [subject.codeableConcept.coding] VARCHAR(MAX),
    [subject.codeableConcept.text] NVARCHAR(4000),
    [subject.reference.id] NVARCHAR(100),
    [subject.reference.extension] NVARCHAR(MAX),
    [subject.reference.reference] NVARCHAR(4000),
    [subject.reference.type] VARCHAR(256),
    [subject.reference.identifier.id] NVARCHAR(100),
    [subject.reference.identifier.extension] NVARCHAR(MAX),
    [subject.reference.identifier.use] NVARCHAR(64),
    [subject.reference.identifier.type] NVARCHAR(MAX),
    [subject.reference.identifier.system] VARCHAR(256),
    [subject.reference.identifier.value] NVARCHAR(4000),
    [subject.reference.identifier.period] NVARCHAR(MAX),
    [subject.reference.identifier.assigner] NVARCHAR(MAX),
    [subject.reference.display] NVARCHAR(4000),
) WITH (
    LOCATION='/SpecimenDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SpecimenDefinitionDerivedFromCanonical AS
SELECT
    [id],
    [derivedFromCanonical.JSON],
    [derivedFromCanonical]
FROM openrowset (
        BULK 'SpecimenDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [derivedFromCanonical.JSON]  VARCHAR(MAX) '$.derivedFromCanonical'
    ) AS rowset
    CROSS APPLY openjson (rowset.[derivedFromCanonical.JSON]) with (
        [derivedFromCanonical]         NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.SpecimenDefinitionDerivedFromUri AS
SELECT
    [id],
    [derivedFromUri.JSON],
    [derivedFromUri]
FROM openrowset (
        BULK 'SpecimenDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [derivedFromUri.JSON]  VARCHAR(MAX) '$.derivedFromUri'
    ) AS rowset
    CROSS APPLY openjson (rowset.[derivedFromUri.JSON]) with (
        [derivedFromUri]               NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.SpecimenDefinitionContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'SpecimenDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contact.JSON]  VARCHAR(MAX) '$.contact'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contact.JSON]) with (
        [contact.id]                   NVARCHAR(100)       '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.name]                 NVARCHAR(500)       '$.name',
        [contact.telecom]              NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.SpecimenDefinitionUseContext AS
SELECT
    [id],
    [useContext.JSON],
    [useContext.id],
    [useContext.extension],
    [useContext.code.id],
    [useContext.code.extension],
    [useContext.code.system],
    [useContext.code.version],
    [useContext.code.code],
    [useContext.code.display],
    [useContext.code.userSelected],
    [useContext.value.codeableConcept.id],
    [useContext.value.codeableConcept.extension],
    [useContext.value.codeableConcept.coding],
    [useContext.value.codeableConcept.text],
    [useContext.value.quantity.id],
    [useContext.value.quantity.extension],
    [useContext.value.quantity.value],
    [useContext.value.quantity.comparator],
    [useContext.value.quantity.unit],
    [useContext.value.quantity.system],
    [useContext.value.quantity.code],
    [useContext.value.range.id],
    [useContext.value.range.extension],
    [useContext.value.range.low],
    [useContext.value.range.high],
    [useContext.value.reference.id],
    [useContext.value.reference.extension],
    [useContext.value.reference.reference],
    [useContext.value.reference.type],
    [useContext.value.reference.identifier],
    [useContext.value.reference.display]
FROM openrowset (
        BULK 'SpecimenDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [useContext.JSON]  VARCHAR(MAX) '$.useContext'
    ) AS rowset
    CROSS APPLY openjson (rowset.[useContext.JSON]) with (
        [useContext.id]                NVARCHAR(100)       '$.id',
        [useContext.extension]         NVARCHAR(MAX)       '$.extension',
        [useContext.code.id]           NVARCHAR(100)       '$.code.id',
        [useContext.code.extension]    NVARCHAR(MAX)       '$.code.extension',
        [useContext.code.system]       VARCHAR(256)        '$.code.system',
        [useContext.code.version]      NVARCHAR(100)       '$.code.version',
        [useContext.code.code]         NVARCHAR(4000)      '$.code.code',
        [useContext.code.display]      NVARCHAR(4000)      '$.code.display',
        [useContext.code.userSelected] bit                 '$.code.userSelected',
        [useContext.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [useContext.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [useContext.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [useContext.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [useContext.value.quantity.id] NVARCHAR(100)       '$.value.quantity.id',
        [useContext.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [useContext.value.quantity.value] float               '$.value.quantity.value',
        [useContext.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [useContext.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [useContext.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [useContext.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [useContext.value.range.id]    NVARCHAR(100)       '$.value.range.id',
        [useContext.value.range.extension] NVARCHAR(MAX)       '$.value.range.extension',
        [useContext.value.range.low]   NVARCHAR(MAX)       '$.value.range.low',
        [useContext.value.range.high]  NVARCHAR(MAX)       '$.value.range.high',
        [useContext.value.reference.id] NVARCHAR(100)       '$.value.reference.id',
        [useContext.value.reference.extension] NVARCHAR(MAX)       '$.value.reference.extension',
        [useContext.value.reference.reference] NVARCHAR(4000)      '$.value.reference.reference',
        [useContext.value.reference.type] VARCHAR(256)        '$.value.reference.type',
        [useContext.value.reference.identifier] NVARCHAR(MAX)       '$.value.reference.identifier',
        [useContext.value.reference.display] NVARCHAR(4000)      '$.value.reference.display'
    ) j

GO

CREATE VIEW fhir.SpecimenDefinitionJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'SpecimenDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [jurisdiction.JSON]  VARCHAR(MAX) '$.jurisdiction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[jurisdiction.JSON]) with (
        [jurisdiction.id]              NVARCHAR(100)       '$.id',
        [jurisdiction.extension]       NVARCHAR(MAX)       '$.extension',
        [jurisdiction.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [jurisdiction.text]            NVARCHAR(4000)      '$.text'
    ) j

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
    [typeTested.singleUse],
    [typeTested.rejectionCriterion],
    [typeTested.handling],
    [typeTested.testingDestination]
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
        [typeTested.preference]        NVARCHAR(4000)      '$.preference',
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
        [typeTested.singleUse]         bit                 '$.singleUse',
        [typeTested.rejectionCriterion] NVARCHAR(MAX)       '$.rejectionCriterion' AS JSON,
        [typeTested.handling]          NVARCHAR(MAX)       '$.handling' AS JSON,
        [typeTested.testingDestination] NVARCHAR(MAX)       '$.testingDestination' AS JSON
    ) j
