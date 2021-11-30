CREATE EXTERNAL TABLE [fhir].[ChargeItemDefinition] (
    [resourceType] NVARCHAR(4000),
    [id] VARCHAR(64),
    [meta.id] NVARCHAR(4000),
    [meta.extension] NVARCHAR(MAX),
    [meta.versionId] VARCHAR(64),
    [meta.lastUpdated] VARCHAR(30),
    [meta.source] VARCHAR(256),
    [meta.profile] VARCHAR(MAX),
    [meta.security] VARCHAR(MAX),
    [meta.tag] VARCHAR(MAX),
    [implicitRules] VARCHAR(256),
    [language] NVARCHAR(4000),
    [text.id] NVARCHAR(4000),
    [text.extension] NVARCHAR(MAX),
    [text.status] NVARCHAR(64),
    [text.div] NVARCHAR(MAX),
    [extension] NVARCHAR(MAX),
    [modifierExtension] NVARCHAR(MAX),
    [url] VARCHAR(256),
    [identifier] VARCHAR(MAX),
    [version] NVARCHAR(4000),
    [title] NVARCHAR(4000),
    [derivedFromUri] VARCHAR(MAX),
    [partOf] VARCHAR(MAX),
    [replaces] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [experimental] bit,
    [date] VARCHAR(30),
    [publisher] NVARCHAR(4000),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [copyright] NVARCHAR(MAX),
    [approvalDate] VARCHAR(10),
    [lastReviewDate] VARCHAR(10),
    [effectivePeriod.id] NVARCHAR(4000),
    [effectivePeriod.extension] NVARCHAR(MAX),
    [effectivePeriod.start] VARCHAR(30),
    [effectivePeriod.end] VARCHAR(30),
    [code.id] NVARCHAR(4000),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [instance] VARCHAR(MAX),
    [applicability] VARCHAR(MAX),
    [propertyGroup] VARCHAR(MAX),
) WITH (
    LOCATION='/ChargeItemDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ChargeItemDefinitionIdentifier AS
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
        BULK 'ChargeItemDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [identifier.JSON]  VARCHAR(MAX) '$.identifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[identifier.JSON]) with (
        [identifier.id]                NVARCHAR(4000)      '$.id',
        [identifier.extension]         NVARCHAR(MAX)       '$.extension',
        [identifier.use]               NVARCHAR(64)        '$.use',
        [identifier.type.id]           NVARCHAR(4000)      '$.type.id',
        [identifier.type.extension]    NVARCHAR(MAX)       '$.type.extension',
        [identifier.type.coding]       NVARCHAR(MAX)       '$.type.coding',
        [identifier.type.text]         NVARCHAR(4000)      '$.type.text',
        [identifier.system]            VARCHAR(256)        '$.system',
        [identifier.value]             NVARCHAR(4000)      '$.value',
        [identifier.period.id]         NVARCHAR(4000)      '$.period.id',
        [identifier.period.extension]  NVARCHAR(MAX)       '$.period.extension',
        [identifier.period.start]      VARCHAR(30)         '$.period.start',
        [identifier.period.end]        VARCHAR(30)         '$.period.end',
        [identifier.assigner.id]       NVARCHAR(4000)      '$.assigner.id',
        [identifier.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [identifier.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [identifier.assigner.type]     VARCHAR(256)        '$.assigner.type',
        [identifier.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [identifier.assigner.display]  NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.ChargeItemDefinitionDerivedFromUri AS
SELECT
    [id],
    [derivedFromUri.JSON],
    [derivedFromUri]
FROM openrowset (
        BULK 'ChargeItemDefinition/**',
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

CREATE VIEW fhir.ChargeItemDefinitionPartOf AS
SELECT
    [id],
    [partOf.JSON],
    [partOf]
FROM openrowset (
        BULK 'ChargeItemDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [partOf.JSON]  VARCHAR(MAX) '$.partOf'
    ) AS rowset
    CROSS APPLY openjson (rowset.[partOf.JSON]) with (
        [partOf]                       NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ChargeItemDefinitionReplaces AS
SELECT
    [id],
    [replaces.JSON],
    [replaces]
FROM openrowset (
        BULK 'ChargeItemDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [replaces.JSON]  VARCHAR(MAX) '$.replaces'
    ) AS rowset
    CROSS APPLY openjson (rowset.[replaces.JSON]) with (
        [replaces]                     NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ChargeItemDefinitionContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'ChargeItemDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contact.JSON]  VARCHAR(MAX) '$.contact'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contact.JSON]) with (
        [contact.id]                   NVARCHAR(4000)      '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.name]                 NVARCHAR(4000)      '$.name',
        [contact.telecom]              NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.ChargeItemDefinitionUseContext AS
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
    [useContext.value.CodeableConcept.id],
    [useContext.value.CodeableConcept.extension],
    [useContext.value.CodeableConcept.coding],
    [useContext.value.CodeableConcept.text],
    [useContext.value.Quantity.id],
    [useContext.value.Quantity.extension],
    [useContext.value.Quantity.value],
    [useContext.value.Quantity.comparator],
    [useContext.value.Quantity.unit],
    [useContext.value.Quantity.system],
    [useContext.value.Quantity.code],
    [useContext.value.Range.id],
    [useContext.value.Range.extension],
    [useContext.value.Range.low],
    [useContext.value.Range.high],
    [useContext.value.Reference.id],
    [useContext.value.Reference.extension],
    [useContext.value.Reference.reference],
    [useContext.value.Reference.type],
    [useContext.value.Reference.identifier],
    [useContext.value.Reference.display]
FROM openrowset (
        BULK 'ChargeItemDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [useContext.JSON]  VARCHAR(MAX) '$.useContext'
    ) AS rowset
    CROSS APPLY openjson (rowset.[useContext.JSON]) with (
        [useContext.id]                NVARCHAR(4000)      '$.id',
        [useContext.extension]         NVARCHAR(MAX)       '$.extension',
        [useContext.code.id]           NVARCHAR(4000)      '$.code.id',
        [useContext.code.extension]    NVARCHAR(MAX)       '$.code.extension',
        [useContext.code.system]       VARCHAR(256)        '$.code.system',
        [useContext.code.version]      NVARCHAR(4000)      '$.code.version',
        [useContext.code.code]         NVARCHAR(4000)      '$.code.code',
        [useContext.code.display]      NVARCHAR(4000)      '$.code.display',
        [useContext.code.userSelected] bit                 '$.code.userSelected',
        [useContext.value.CodeableConcept.id] NVARCHAR(4000)      '$.value.CodeableConcept.id',
        [useContext.value.CodeableConcept.extension] NVARCHAR(MAX)       '$.value.CodeableConcept.extension',
        [useContext.value.CodeableConcept.coding] NVARCHAR(MAX)       '$.value.CodeableConcept.coding',
        [useContext.value.CodeableConcept.text] NVARCHAR(4000)      '$.value.CodeableConcept.text',
        [useContext.value.Quantity.id] NVARCHAR(4000)      '$.value.Quantity.id',
        [useContext.value.Quantity.extension] NVARCHAR(MAX)       '$.value.Quantity.extension',
        [useContext.value.Quantity.value] float               '$.value.Quantity.value',
        [useContext.value.Quantity.comparator] NVARCHAR(64)        '$.value.Quantity.comparator',
        [useContext.value.Quantity.unit] NVARCHAR(4000)      '$.value.Quantity.unit',
        [useContext.value.Quantity.system] VARCHAR(256)        '$.value.Quantity.system',
        [useContext.value.Quantity.code] NVARCHAR(4000)      '$.value.Quantity.code',
        [useContext.value.Range.id]    NVARCHAR(4000)      '$.value.Range.id',
        [useContext.value.Range.extension] NVARCHAR(MAX)       '$.value.Range.extension',
        [useContext.value.Range.low]   NVARCHAR(MAX)       '$.value.Range.low',
        [useContext.value.Range.high]  NVARCHAR(MAX)       '$.value.Range.high',
        [useContext.value.Reference.id] NVARCHAR(4000)      '$.value.Reference.id',
        [useContext.value.Reference.extension] NVARCHAR(MAX)       '$.value.Reference.extension',
        [useContext.value.Reference.reference] NVARCHAR(4000)      '$.value.Reference.reference',
        [useContext.value.Reference.type] VARCHAR(256)        '$.value.Reference.type',
        [useContext.value.Reference.identifier] NVARCHAR(MAX)       '$.value.Reference.identifier',
        [useContext.value.Reference.display] NVARCHAR(4000)      '$.value.Reference.display'
    ) j

GO

CREATE VIEW fhir.ChargeItemDefinitionJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'ChargeItemDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [jurisdiction.JSON]  VARCHAR(MAX) '$.jurisdiction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[jurisdiction.JSON]) with (
        [jurisdiction.id]              NVARCHAR(4000)      '$.id',
        [jurisdiction.extension]       NVARCHAR(MAX)       '$.extension',
        [jurisdiction.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [jurisdiction.text]            NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ChargeItemDefinitionInstance AS
SELECT
    [id],
    [instance.JSON],
    [instance.id],
    [instance.extension],
    [instance.reference],
    [instance.type],
    [instance.identifier.id],
    [instance.identifier.extension],
    [instance.identifier.use],
    [instance.identifier.type],
    [instance.identifier.system],
    [instance.identifier.value],
    [instance.identifier.period],
    [instance.identifier.assigner],
    [instance.display]
FROM openrowset (
        BULK 'ChargeItemDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instance.JSON]  VARCHAR(MAX) '$.instance'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instance.JSON]) with (
        [instance.id]                  NVARCHAR(4000)      '$.id',
        [instance.extension]           NVARCHAR(MAX)       '$.extension',
        [instance.reference]           NVARCHAR(4000)      '$.reference',
        [instance.type]                VARCHAR(256)        '$.type',
        [instance.identifier.id]       NVARCHAR(4000)      '$.identifier.id',
        [instance.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [instance.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [instance.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [instance.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [instance.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [instance.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [instance.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [instance.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ChargeItemDefinitionApplicability AS
SELECT
    [id],
    [applicability.JSON],
    [applicability.id],
    [applicability.extension],
    [applicability.modifierExtension],
    [applicability.description],
    [applicability.language],
    [applicability.expression]
FROM openrowset (
        BULK 'ChargeItemDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [applicability.JSON]  VARCHAR(MAX) '$.applicability'
    ) AS rowset
    CROSS APPLY openjson (rowset.[applicability.JSON]) with (
        [applicability.id]             NVARCHAR(4000)      '$.id',
        [applicability.extension]      NVARCHAR(MAX)       '$.extension',
        [applicability.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [applicability.description]    NVARCHAR(4000)      '$.description',
        [applicability.language]       NVARCHAR(4000)      '$.language',
        [applicability.expression]     NVARCHAR(4000)      '$.expression'
    ) j

GO

CREATE VIEW fhir.ChargeItemDefinitionPropertyGroup AS
SELECT
    [id],
    [propertyGroup.JSON],
    [propertyGroup.id],
    [propertyGroup.extension],
    [propertyGroup.modifierExtension],
    [propertyGroup.applicability],
    [propertyGroup.priceComponent]
FROM openrowset (
        BULK 'ChargeItemDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [propertyGroup.JSON]  VARCHAR(MAX) '$.propertyGroup'
    ) AS rowset
    CROSS APPLY openjson (rowset.[propertyGroup.JSON]) with (
        [propertyGroup.id]             NVARCHAR(4000)      '$.id',
        [propertyGroup.extension]      NVARCHAR(MAX)       '$.extension',
        [propertyGroup.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [propertyGroup.applicability]  NVARCHAR(MAX)       '$.applicability' AS JSON,
        [propertyGroup.priceComponent] NVARCHAR(MAX)       '$.priceComponent' AS JSON
    ) j
