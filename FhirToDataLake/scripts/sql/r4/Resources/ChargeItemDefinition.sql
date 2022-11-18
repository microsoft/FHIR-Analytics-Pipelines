CREATE EXTERNAL TABLE [fhir].[ChargeItemDefinition] (
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
    [identifier] VARCHAR(MAX),
    [version] NVARCHAR(100),
    [title] NVARCHAR(4000),
    [derivedFromUri] VARCHAR(MAX),
    [partOf] VARCHAR(MAX),
    [replaces] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [experimental] bit,
    [date] VARCHAR(64),
    [publisher] NVARCHAR(500),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [copyright] NVARCHAR(MAX),
    [approvalDate] VARCHAR(64),
    [lastReviewDate] VARCHAR(64),
    [effectivePeriod.id] NVARCHAR(100),
    [effectivePeriod.extension] NVARCHAR(MAX),
    [effectivePeriod.start] VARCHAR(64),
    [effectivePeriod.end] VARCHAR(64),
    [code.id] NVARCHAR(100),
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
        [contact.id]                   NVARCHAR(100)       '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.name]                 NVARCHAR(500)       '$.name',
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
        BULK 'ChargeItemDefinition/**',
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
        [jurisdiction.id]              NVARCHAR(100)       '$.id',
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
        [instance.id]                  NVARCHAR(100)       '$.id',
        [instance.extension]           NVARCHAR(MAX)       '$.extension',
        [instance.reference]           NVARCHAR(4000)      '$.reference',
        [instance.type]                VARCHAR(256)        '$.type',
        [instance.identifier.id]       NVARCHAR(100)       '$.identifier.id',
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
        [applicability.id]             NVARCHAR(100)       '$.id',
        [applicability.extension]      NVARCHAR(MAX)       '$.extension',
        [applicability.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [applicability.description]    NVARCHAR(4000)      '$.description',
        [applicability.language]       NVARCHAR(100)       '$.language',
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
        [propertyGroup.id]             NVARCHAR(100)       '$.id',
        [propertyGroup.extension]      NVARCHAR(MAX)       '$.extension',
        [propertyGroup.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [propertyGroup.applicability]  NVARCHAR(MAX)       '$.applicability' AS JSON,
        [propertyGroup.priceComponent] NVARCHAR(MAX)       '$.priceComponent' AS JSON
    ) j
