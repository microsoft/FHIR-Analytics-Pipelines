CREATE EXTERNAL TABLE [fhir].[ImplementationGuide] (
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
    [version] NVARCHAR(100),
    [name] NVARCHAR(500),
    [title] NVARCHAR(4000),
    [status] NVARCHAR(64),
    [experimental] bit,
    [date] VARCHAR(64),
    [publisher] NVARCHAR(500),
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
    [definition.id] NVARCHAR(100),
    [definition.extension] NVARCHAR(MAX),
    [definition.modifierExtension] NVARCHAR(MAX),
    [definition.grouping] VARCHAR(MAX),
    [definition.resource] VARCHAR(MAX),
    [definition.page.id] NVARCHAR(100),
    [definition.page.extension] NVARCHAR(MAX),
    [definition.page.modifierExtension] NVARCHAR(MAX),
    [definition.page.title] NVARCHAR(4000),
    [definition.page.generation] NVARCHAR(64),
    [definition.page.page] NVARCHAR(MAX),
    [definition.page.name.url] VARCHAR(256),
    [definition.page.name.reference] NVARCHAR(MAX),
    [definition.parameter] VARCHAR(MAX),
    [definition.template] VARCHAR(MAX),
    [manifest.id] NVARCHAR(100),
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
        [contact.id]                   NVARCHAR(100)       '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.name]                 NVARCHAR(500)       '$.name',
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
        BULK 'ImplementationGuide/**',
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
        [jurisdiction.id]              NVARCHAR(100)       '$.id',
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
        [dependsOn.id]                 NVARCHAR(100)       '$.id',
        [dependsOn.extension]          NVARCHAR(MAX)       '$.extension',
        [dependsOn.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [dependsOn.uri]                VARCHAR(256)        '$.uri',
        [dependsOn.packageId]          VARCHAR(64)         '$.packageId',
        [dependsOn.version]            NVARCHAR(100)       '$.version'
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
        [global.id]                    NVARCHAR(100)       '$.id',
        [global.extension]             NVARCHAR(MAX)       '$.extension',
        [global.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [global.type]                  NVARCHAR(100)       '$.type',
        [global.profile]               VARCHAR(256)        '$.profile'
    ) j
