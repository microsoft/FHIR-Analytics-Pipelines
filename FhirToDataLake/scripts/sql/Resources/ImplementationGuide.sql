CREATE EXTERNAL TABLE [fhir].[ImplementationGuide] (
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
    [version] NVARCHAR(4000),
    [name] NVARCHAR(4000),
    [title] NVARCHAR(4000),
    [status] NVARCHAR(64),
    [experimental] bit,
    [date] VARCHAR(30),
    [publisher] NVARCHAR(4000),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [copyright] NVARCHAR(MAX),
    [packageId] VARCHAR(64),
    [license] NVARCHAR(64),
    [fhirVersion] VARCHAR(MAX),
    [dependsOn] VARCHAR(MAX),
    [global] VARCHAR(MAX),
    [definition.id] NVARCHAR(4000),
    [definition.extension] NVARCHAR(MAX),
    [definition.modifierExtension] NVARCHAR(MAX),
    [definition.grouping] VARCHAR(MAX),
    [definition.resource] VARCHAR(MAX),
    [definition.page.id] NVARCHAR(4000),
    [definition.page.extension] NVARCHAR(MAX),
    [definition.page.modifierExtension] NVARCHAR(MAX),
    [definition.page.title] NVARCHAR(4000),
    [definition.page.generation] NVARCHAR(64),
    [definition.page.page] NVARCHAR(MAX),
    [definition.page.name.url] VARCHAR(256),
    [definition.page.name.Reference] NVARCHAR(MAX),
    [definition.parameter] VARCHAR(MAX),
    [definition.template] VARCHAR(MAX),
    [manifest.id] NVARCHAR(4000),
    [manifest.extension] NVARCHAR(MAX),
    [manifest.modifierExtension] NVARCHAR(MAX),
    [manifest.rendering] VARCHAR(256),
    [manifest.resource] VARCHAR(MAX),
    [manifest.page] VARCHAR(MAX),
    [manifest.image] VARCHAR(MAX),
    [manifest.other] VARCHAR(MAX),
) WITH (
    LOCATION='/ImplementationGuide/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ImplementationGuideContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'ImplementationGuide/**',
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

CREATE VIEW fhir.ImplementationGuideUseContext AS
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
        BULK 'ImplementationGuide/**',
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

CREATE VIEW fhir.ImplementationGuideJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'ImplementationGuide/**',
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

CREATE VIEW fhir.ImplementationGuideFhirVersion AS
SELECT
    [id],
    [fhirVersion.JSON],
    [fhirVersion]
FROM openrowset (
        BULK 'ImplementationGuide/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [fhirVersion.JSON]  VARCHAR(MAX) '$.fhirVersion'
    ) AS rowset
    CROSS APPLY openjson (rowset.[fhirVersion.JSON]) with (
        [fhirVersion]                  NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ImplementationGuideDependsOn AS
SELECT
    [id],
    [dependsOn.JSON],
    [dependsOn.id],
    [dependsOn.extension],
    [dependsOn.modifierExtension],
    [dependsOn.uri],
    [dependsOn.packageId],
    [dependsOn.version]
FROM openrowset (
        BULK 'ImplementationGuide/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [dependsOn.JSON]  VARCHAR(MAX) '$.dependsOn'
    ) AS rowset
    CROSS APPLY openjson (rowset.[dependsOn.JSON]) with (
        [dependsOn.id]                 NVARCHAR(4000)      '$.id',
        [dependsOn.extension]          NVARCHAR(MAX)       '$.extension',
        [dependsOn.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [dependsOn.uri]                VARCHAR(256)        '$.uri',
        [dependsOn.packageId]          VARCHAR(64)         '$.packageId',
        [dependsOn.version]            NVARCHAR(4000)      '$.version'
    ) j

GO

CREATE VIEW fhir.ImplementationGuideGlobal AS
SELECT
    [id],
    [global.JSON],
    [global.id],
    [global.extension],
    [global.modifierExtension],
    [global.type],
    [global.profile]
FROM openrowset (
        BULK 'ImplementationGuide/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [global.JSON]  VARCHAR(MAX) '$.global'
    ) AS rowset
    CROSS APPLY openjson (rowset.[global.JSON]) with (
        [global.id]                    NVARCHAR(4000)      '$.id',
        [global.extension]             NVARCHAR(MAX)       '$.extension',
        [global.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [global.type]                  NVARCHAR(4000)      '$.type',
        [global.profile]               VARCHAR(256)        '$.profile'
    ) j
