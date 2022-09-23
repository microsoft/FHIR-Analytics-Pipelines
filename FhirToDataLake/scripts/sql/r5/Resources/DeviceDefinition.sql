CREATE EXTERNAL TABLE [fhir].[DeviceDefinition] (
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
    [description] NVARCHAR(MAX),
    [identifier] VARCHAR(MAX),
    [udiDeviceIdentifier] VARCHAR(MAX),
    [partNumber] NVARCHAR(100),
    [deviceName] VARCHAR(MAX),
    [modelNumber] NVARCHAR(100),
    [classification] VARCHAR(MAX),
    [specialization] VARCHAR(MAX),
    [hasPart] VARCHAR(MAX),
    [packaging] VARCHAR(MAX),
    [version] VARCHAR(MAX),
    [safety] VARCHAR(MAX),
    [shelfLifeStorage] VARCHAR(MAX),
    [languageCode] VARCHAR(MAX),
    [property] VARCHAR(MAX),
    [owner.id] NVARCHAR(100),
    [owner.extension] NVARCHAR(MAX),
    [owner.reference] NVARCHAR(4000),
    [owner.type] VARCHAR(256),
    [owner.identifier.id] NVARCHAR(100),
    [owner.identifier.extension] NVARCHAR(MAX),
    [owner.identifier.use] NVARCHAR(64),
    [owner.identifier.type] NVARCHAR(MAX),
    [owner.identifier.system] VARCHAR(256),
    [owner.identifier.value] NVARCHAR(4000),
    [owner.identifier.period] NVARCHAR(MAX),
    [owner.identifier.assigner] NVARCHAR(MAX),
    [owner.display] NVARCHAR(4000),
    [contact] VARCHAR(MAX),
    [link] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [parentDevice.id] NVARCHAR(100),
    [parentDevice.extension] NVARCHAR(MAX),
    [parentDevice.reference] NVARCHAR(4000),
    [parentDevice.type] VARCHAR(256),
    [parentDevice.identifier.id] NVARCHAR(100),
    [parentDevice.identifier.extension] NVARCHAR(MAX),
    [parentDevice.identifier.use] NVARCHAR(64),
    [parentDevice.identifier.type] NVARCHAR(MAX),
    [parentDevice.identifier.system] VARCHAR(256),
    [parentDevice.identifier.value] NVARCHAR(4000),
    [parentDevice.identifier.period] NVARCHAR(MAX),
    [parentDevice.identifier.assigner] NVARCHAR(MAX),
    [parentDevice.display] NVARCHAR(4000),
    [material] VARCHAR(MAX),
    [productionIdentifierInUDI] VARCHAR(MAX),
    [guideline.id] NVARCHAR(100),
    [guideline.extension] NVARCHAR(MAX),
    [guideline.modifierExtension] NVARCHAR(MAX),
    [guideline.useContext] VARCHAR(MAX),
    [guideline.usageInstruction] NVARCHAR(MAX),
    [guideline.relatedArtifact] VARCHAR(MAX),
    [guideline.indication] VARCHAR(MAX),
    [guideline.contraindication] VARCHAR(MAX),
    [guideline.warning] VARCHAR(MAX),
    [guideline.intendedUse] NVARCHAR(4000),
    [correctiveAction.id] NVARCHAR(100),
    [correctiveAction.extension] NVARCHAR(MAX),
    [correctiveAction.modifierExtension] NVARCHAR(MAX),
    [correctiveAction.recall] bit,
    [correctiveAction.scope] NVARCHAR(4000),
    [correctiveAction.period.id] NVARCHAR(100),
    [correctiveAction.period.extension] NVARCHAR(MAX),
    [correctiveAction.period.start] VARCHAR(64),
    [correctiveAction.period.end] VARCHAR(64),
    [chargeItem] VARCHAR(MAX),
    [manufacturer.string] NVARCHAR(4000),
    [manufacturer.reference.id] NVARCHAR(100),
    [manufacturer.reference.extension] NVARCHAR(MAX),
    [manufacturer.reference.reference] NVARCHAR(4000),
    [manufacturer.reference.type] VARCHAR(256),
    [manufacturer.reference.identifier.id] NVARCHAR(100),
    [manufacturer.reference.identifier.extension] NVARCHAR(MAX),
    [manufacturer.reference.identifier.use] NVARCHAR(64),
    [manufacturer.reference.identifier.type] NVARCHAR(MAX),
    [manufacturer.reference.identifier.system] VARCHAR(256),
    [manufacturer.reference.identifier.value] NVARCHAR(4000),
    [manufacturer.reference.identifier.period] NVARCHAR(MAX),
    [manufacturer.reference.identifier.assigner] NVARCHAR(MAX),
    [manufacturer.reference.display] NVARCHAR(4000),
) WITH (
    LOCATION='/DeviceDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.DeviceDefinitionIdentifier AS
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
        BULK 'DeviceDefinition/**',
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

CREATE VIEW fhir.DeviceDefinitionUdiDeviceIdentifier AS
SELECT
    [id],
    [udiDeviceIdentifier.JSON],
    [udiDeviceIdentifier.id],
    [udiDeviceIdentifier.extension],
    [udiDeviceIdentifier.modifierExtension],
    [udiDeviceIdentifier.deviceIdentifier],
    [udiDeviceIdentifier.issuer],
    [udiDeviceIdentifier.jurisdiction],
    [udiDeviceIdentifier.marketDistribution]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [udiDeviceIdentifier.JSON]  VARCHAR(MAX) '$.udiDeviceIdentifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[udiDeviceIdentifier.JSON]) with (
        [udiDeviceIdentifier.id]       NVARCHAR(100)       '$.id',
        [udiDeviceIdentifier.extension] NVARCHAR(MAX)       '$.extension',
        [udiDeviceIdentifier.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [udiDeviceIdentifier.deviceIdentifier] NVARCHAR(500)       '$.deviceIdentifier',
        [udiDeviceIdentifier.issuer]   VARCHAR(256)        '$.issuer',
        [udiDeviceIdentifier.jurisdiction] VARCHAR(256)        '$.jurisdiction',
        [udiDeviceIdentifier.marketDistribution] NVARCHAR(MAX)       '$.marketDistribution' AS JSON
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionDeviceName AS
SELECT
    [id],
    [deviceName.JSON],
    [deviceName.id],
    [deviceName.extension],
    [deviceName.modifierExtension],
    [deviceName.name],
    [deviceName.type]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [deviceName.JSON]  VARCHAR(MAX) '$.deviceName'
    ) AS rowset
    CROSS APPLY openjson (rowset.[deviceName.JSON]) with (
        [deviceName.id]                NVARCHAR(100)       '$.id',
        [deviceName.extension]         NVARCHAR(MAX)       '$.extension',
        [deviceName.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [deviceName.name]              NVARCHAR(500)       '$.name',
        [deviceName.type]              NVARCHAR(100)       '$.type'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionClassification AS
SELECT
    [id],
    [classification.JSON],
    [classification.id],
    [classification.extension],
    [classification.modifierExtension],
    [classification.type.id],
    [classification.type.extension],
    [classification.type.coding],
    [classification.type.text],
    [classification.justification]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [classification.JSON]  VARCHAR(MAX) '$.classification'
    ) AS rowset
    CROSS APPLY openjson (rowset.[classification.JSON]) with (
        [classification.id]            NVARCHAR(100)       '$.id',
        [classification.extension]     NVARCHAR(MAX)       '$.extension',
        [classification.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [classification.type.id]       NVARCHAR(100)       '$.type.id',
        [classification.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [classification.type.coding]   NVARCHAR(MAX)       '$.type.coding',
        [classification.type.text]     NVARCHAR(4000)      '$.type.text',
        [classification.justification] NVARCHAR(MAX)       '$.justification' AS JSON
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionSpecialization AS
SELECT
    [id],
    [specialization.JSON],
    [specialization.id],
    [specialization.extension],
    [specialization.type],
    [specialization.classifier],
    [specialization.label],
    [specialization.display],
    [specialization.citation],
    [specialization.document.id],
    [specialization.document.extension],
    [specialization.document.contentType],
    [specialization.document.language],
    [specialization.document.data],
    [specialization.document.url],
    [specialization.document.size],
    [specialization.document.hash],
    [specialization.document.title],
    [specialization.document.creation],
    [specialization.document.height],
    [specialization.document.width],
    [specialization.document.frames],
    [specialization.document.duration],
    [specialization.document.pages],
    [specialization.resource],
    [specialization.resourceReference.id],
    [specialization.resourceReference.extension],
    [specialization.resourceReference.reference],
    [specialization.resourceReference.type],
    [specialization.resourceReference.identifier],
    [specialization.resourceReference.display]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specialization.JSON]  VARCHAR(MAX) '$.specialization'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specialization.JSON]) with (
        [specialization.id]            NVARCHAR(100)       '$.id',
        [specialization.extension]     NVARCHAR(MAX)       '$.extension',
        [specialization.type]          NVARCHAR(64)        '$.type',
        [specialization.classifier]    NVARCHAR(MAX)       '$.classifier' AS JSON,
        [specialization.label]         NVARCHAR(100)       '$.label',
        [specialization.display]       NVARCHAR(4000)      '$.display',
        [specialization.citation]      NVARCHAR(MAX)       '$.citation',
        [specialization.document.id]   NVARCHAR(100)       '$.document.id',
        [specialization.document.extension] NVARCHAR(MAX)       '$.document.extension',
        [specialization.document.contentType] NVARCHAR(100)       '$.document.contentType',
        [specialization.document.language] NVARCHAR(100)       '$.document.language',
        [specialization.document.data] NVARCHAR(MAX)       '$.document.data',
        [specialization.document.url]  VARCHAR(256)        '$.document.url',
        [specialization.document.size] NVARCHAR(MAX)       '$.document.size',
        [specialization.document.hash] NVARCHAR(MAX)       '$.document.hash',
        [specialization.document.title] NVARCHAR(4000)      '$.document.title',
        [specialization.document.creation] VARCHAR(64)         '$.document.creation',
        [specialization.document.height] bigint              '$.document.height',
        [specialization.document.width] bigint              '$.document.width',
        [specialization.document.frames] bigint              '$.document.frames',
        [specialization.document.duration] float               '$.document.duration',
        [specialization.document.pages] bigint              '$.document.pages',
        [specialization.resource]      VARCHAR(256)        '$.resource',
        [specialization.resourceReference.id] NVARCHAR(100)       '$.resourceReference.id',
        [specialization.resourceReference.extension] NVARCHAR(MAX)       '$.resourceReference.extension',
        [specialization.resourceReference.reference] NVARCHAR(4000)      '$.resourceReference.reference',
        [specialization.resourceReference.type] VARCHAR(256)        '$.resourceReference.type',
        [specialization.resourceReference.identifier] NVARCHAR(MAX)       '$.resourceReference.identifier',
        [specialization.resourceReference.display] NVARCHAR(4000)      '$.resourceReference.display'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionHasPart AS
SELECT
    [id],
    [hasPart.JSON],
    [hasPart.id],
    [hasPart.extension],
    [hasPart.modifierExtension],
    [hasPart.reference.id],
    [hasPart.reference.extension],
    [hasPart.reference.reference],
    [hasPart.reference.type],
    [hasPart.reference.identifier],
    [hasPart.reference.display],
    [hasPart.count]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [hasPart.JSON]  VARCHAR(MAX) '$.hasPart'
    ) AS rowset
    CROSS APPLY openjson (rowset.[hasPart.JSON]) with (
        [hasPart.id]                   NVARCHAR(100)       '$.id',
        [hasPart.extension]            NVARCHAR(MAX)       '$.extension',
        [hasPart.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [hasPart.reference.id]         NVARCHAR(100)       '$.reference.id',
        [hasPart.reference.extension]  NVARCHAR(MAX)       '$.reference.extension',
        [hasPart.reference.reference]  NVARCHAR(4000)      '$.reference.reference',
        [hasPart.reference.type]       VARCHAR(256)        '$.reference.type',
        [hasPart.reference.identifier] NVARCHAR(MAX)       '$.reference.identifier',
        [hasPart.reference.display]    NVARCHAR(4000)      '$.reference.display',
        [hasPart.count]                bigint              '$.count'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionPackaging AS
SELECT
    [id],
    [packaging.JSON],
    [packaging.id],
    [packaging.extension],
    [packaging.modifierExtension],
    [packaging.identifier.id],
    [packaging.identifier.extension],
    [packaging.identifier.use],
    [packaging.identifier.type],
    [packaging.identifier.system],
    [packaging.identifier.value],
    [packaging.identifier.period],
    [packaging.identifier.assigner],
    [packaging.type.id],
    [packaging.type.extension],
    [packaging.type.coding],
    [packaging.type.text],
    [packaging.count],
    [packaging.distributor],
    [packaging.udiDeviceIdentifier],
    [packaging.packaging]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [packaging.JSON]  VARCHAR(MAX) '$.packaging'
    ) AS rowset
    CROSS APPLY openjson (rowset.[packaging.JSON]) with (
        [packaging.id]                 NVARCHAR(100)       '$.id',
        [packaging.extension]          NVARCHAR(MAX)       '$.extension',
        [packaging.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [packaging.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [packaging.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [packaging.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [packaging.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [packaging.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [packaging.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [packaging.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [packaging.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [packaging.type.id]            NVARCHAR(100)       '$.type.id',
        [packaging.type.extension]     NVARCHAR(MAX)       '$.type.extension',
        [packaging.type.coding]        NVARCHAR(MAX)       '$.type.coding',
        [packaging.type.text]          NVARCHAR(4000)      '$.type.text',
        [packaging.count]              bigint              '$.count',
        [packaging.distributor]        NVARCHAR(MAX)       '$.distributor' AS JSON,
        [packaging.udiDeviceIdentifier] NVARCHAR(MAX)       '$.udiDeviceIdentifier' AS JSON,
        [packaging.packaging]          NVARCHAR(MAX)       '$.packaging' AS JSON
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionVersion AS
SELECT
    [id],
    [version.JSON],
    [version.id],
    [version.extension],
    [version.modifierExtension],
    [version.type.id],
    [version.type.extension],
    [version.type.coding],
    [version.type.text],
    [version.component.id],
    [version.component.extension],
    [version.component.use],
    [version.component.type],
    [version.component.system],
    [version.component.value],
    [version.component.period],
    [version.component.assigner],
    [version.value]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [version.JSON]  VARCHAR(MAX) '$.version'
    ) AS rowset
    CROSS APPLY openjson (rowset.[version.JSON]) with (
        [version.id]                   NVARCHAR(100)       '$.id',
        [version.extension]            NVARCHAR(MAX)       '$.extension',
        [version.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [version.type.id]              NVARCHAR(100)       '$.type.id',
        [version.type.extension]       NVARCHAR(MAX)       '$.type.extension',
        [version.type.coding]          NVARCHAR(MAX)       '$.type.coding',
        [version.type.text]            NVARCHAR(4000)      '$.type.text',
        [version.component.id]         NVARCHAR(100)       '$.component.id',
        [version.component.extension]  NVARCHAR(MAX)       '$.component.extension',
        [version.component.use]        NVARCHAR(64)        '$.component.use',
        [version.component.type]       NVARCHAR(MAX)       '$.component.type',
        [version.component.system]     VARCHAR(256)        '$.component.system',
        [version.component.value]      NVARCHAR(4000)      '$.component.value',
        [version.component.period]     NVARCHAR(MAX)       '$.component.period',
        [version.component.assigner]   NVARCHAR(MAX)       '$.component.assigner',
        [version.value]                NVARCHAR(4000)      '$.value'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionSafety AS
SELECT
    [id],
    [safety.JSON],
    [safety.id],
    [safety.extension],
    [safety.coding],
    [safety.text]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [safety.JSON]  VARCHAR(MAX) '$.safety'
    ) AS rowset
    CROSS APPLY openjson (rowset.[safety.JSON]) with (
        [safety.id]                    NVARCHAR(100)       '$.id',
        [safety.extension]             NVARCHAR(MAX)       '$.extension',
        [safety.coding]                NVARCHAR(MAX)       '$.coding' AS JSON,
        [safety.text]                  NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionShelfLifeStorage AS
SELECT
    [id],
    [shelfLifeStorage.JSON],
    [shelfLifeStorage.id],
    [shelfLifeStorage.extension],
    [shelfLifeStorage.modifierExtension],
    [shelfLifeStorage.type.id],
    [shelfLifeStorage.type.extension],
    [shelfLifeStorage.type.coding],
    [shelfLifeStorage.type.text],
    [shelfLifeStorage.specialPrecautionsForStorage],
    [shelfLifeStorage.period.duration.id],
    [shelfLifeStorage.period.duration.extension],
    [shelfLifeStorage.period.duration.value],
    [shelfLifeStorage.period.duration.comparator],
    [shelfLifeStorage.period.duration.unit],
    [shelfLifeStorage.period.duration.system],
    [shelfLifeStorage.period.duration.code],
    [shelfLifeStorage.period.string]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [shelfLifeStorage.JSON]  VARCHAR(MAX) '$.shelfLifeStorage'
    ) AS rowset
    CROSS APPLY openjson (rowset.[shelfLifeStorage.JSON]) with (
        [shelfLifeStorage.id]          NVARCHAR(100)       '$.id',
        [shelfLifeStorage.extension]   NVARCHAR(MAX)       '$.extension',
        [shelfLifeStorage.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [shelfLifeStorage.type.id]     NVARCHAR(100)       '$.type.id',
        [shelfLifeStorage.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [shelfLifeStorage.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [shelfLifeStorage.type.text]   NVARCHAR(4000)      '$.type.text',
        [shelfLifeStorage.specialPrecautionsForStorage] NVARCHAR(MAX)       '$.specialPrecautionsForStorage' AS JSON,
        [shelfLifeStorage.period.duration.id] NVARCHAR(100)       '$.period.duration.id',
        [shelfLifeStorage.period.duration.extension] NVARCHAR(MAX)       '$.period.duration.extension',
        [shelfLifeStorage.period.duration.value] float               '$.period.duration.value',
        [shelfLifeStorage.period.duration.comparator] NVARCHAR(64)        '$.period.duration.comparator',
        [shelfLifeStorage.period.duration.unit] NVARCHAR(100)       '$.period.duration.unit',
        [shelfLifeStorage.period.duration.system] VARCHAR(256)        '$.period.duration.system',
        [shelfLifeStorage.period.duration.code] NVARCHAR(4000)      '$.period.duration.code',
        [shelfLifeStorage.period.string] NVARCHAR(4000)      '$.period.string'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionLanguageCode AS
SELECT
    [id],
    [languageCode.JSON],
    [languageCode.id],
    [languageCode.extension],
    [languageCode.coding],
    [languageCode.text]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [languageCode.JSON]  VARCHAR(MAX) '$.languageCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[languageCode.JSON]) with (
        [languageCode.id]              NVARCHAR(100)       '$.id',
        [languageCode.extension]       NVARCHAR(MAX)       '$.extension',
        [languageCode.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [languageCode.text]            NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionProperty AS
SELECT
    [id],
    [property.JSON],
    [property.id],
    [property.extension],
    [property.modifierExtension],
    [property.type.id],
    [property.type.extension],
    [property.type.coding],
    [property.type.text],
    [property.value.quantity.id],
    [property.value.quantity.extension],
    [property.value.quantity.value],
    [property.value.quantity.comparator],
    [property.value.quantity.unit],
    [property.value.quantity.system],
    [property.value.quantity.code],
    [property.value.codeableConcept.id],
    [property.value.codeableConcept.extension],
    [property.value.codeableConcept.coding],
    [property.value.codeableConcept.text],
    [property.value.string],
    [property.value.boolean],
    [property.value.integer],
    [property.value.range.id],
    [property.value.range.extension],
    [property.value.range.low],
    [property.value.range.high],
    [property.value.attachment.id],
    [property.value.attachment.extension],
    [property.value.attachment.contentType],
    [property.value.attachment.language],
    [property.value.attachment.data],
    [property.value.attachment.url],
    [property.value.attachment.size],
    [property.value.attachment.hash],
    [property.value.attachment.title],
    [property.value.attachment.creation],
    [property.value.attachment.height],
    [property.value.attachment.width],
    [property.value.attachment.frames],
    [property.value.attachment.duration],
    [property.value.attachment.pages]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [property.JSON]  VARCHAR(MAX) '$.property'
    ) AS rowset
    CROSS APPLY openjson (rowset.[property.JSON]) with (
        [property.id]                  NVARCHAR(100)       '$.id',
        [property.extension]           NVARCHAR(MAX)       '$.extension',
        [property.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [property.type.id]             NVARCHAR(100)       '$.type.id',
        [property.type.extension]      NVARCHAR(MAX)       '$.type.extension',
        [property.type.coding]         NVARCHAR(MAX)       '$.type.coding',
        [property.type.text]           NVARCHAR(4000)      '$.type.text',
        [property.value.quantity.id]   NVARCHAR(100)       '$.value.quantity.id',
        [property.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [property.value.quantity.value] float               '$.value.quantity.value',
        [property.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [property.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [property.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [property.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [property.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [property.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [property.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [property.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [property.value.string]        NVARCHAR(4000)      '$.value.string',
        [property.value.boolean]       bit                 '$.value.boolean',
        [property.value.integer]       bigint              '$.value.integer',
        [property.value.range.id]      NVARCHAR(100)       '$.value.range.id',
        [property.value.range.extension] NVARCHAR(MAX)       '$.value.range.extension',
        [property.value.range.low]     NVARCHAR(MAX)       '$.value.range.low',
        [property.value.range.high]    NVARCHAR(MAX)       '$.value.range.high',
        [property.value.attachment.id] NVARCHAR(100)       '$.value.attachment.id',
        [property.value.attachment.extension] NVARCHAR(MAX)       '$.value.attachment.extension',
        [property.value.attachment.contentType] NVARCHAR(100)       '$.value.attachment.contentType',
        [property.value.attachment.language] NVARCHAR(100)       '$.value.attachment.language',
        [property.value.attachment.data] NVARCHAR(MAX)       '$.value.attachment.data',
        [property.value.attachment.url] VARCHAR(256)        '$.value.attachment.url',
        [property.value.attachment.size] NVARCHAR(MAX)       '$.value.attachment.size',
        [property.value.attachment.hash] NVARCHAR(MAX)       '$.value.attachment.hash',
        [property.value.attachment.title] NVARCHAR(4000)      '$.value.attachment.title',
        [property.value.attachment.creation] VARCHAR(64)         '$.value.attachment.creation',
        [property.value.attachment.height] bigint              '$.value.attachment.height',
        [property.value.attachment.width] bigint              '$.value.attachment.width',
        [property.value.attachment.frames] bigint              '$.value.attachment.frames',
        [property.value.attachment.duration] float               '$.value.attachment.duration',
        [property.value.attachment.pages] bigint              '$.value.attachment.pages'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.system],
    [contact.value],
    [contact.use],
    [contact.rank],
    [contact.period.id],
    [contact.period.extension],
    [contact.period.start],
    [contact.period.end]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contact.JSON]  VARCHAR(MAX) '$.contact'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contact.JSON]) with (
        [contact.id]                   NVARCHAR(100)       '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.system]               NVARCHAR(64)        '$.system',
        [contact.value]                NVARCHAR(4000)      '$.value',
        [contact.use]                  NVARCHAR(64)        '$.use',
        [contact.rank]                 bigint              '$.rank',
        [contact.period.id]            NVARCHAR(100)       '$.period.id',
        [contact.period.extension]     NVARCHAR(MAX)       '$.period.extension',
        [contact.period.start]         VARCHAR(64)         '$.period.start',
        [contact.period.end]           VARCHAR(64)         '$.period.end'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionLink AS
SELECT
    [id],
    [link.JSON],
    [link.id],
    [link.extension],
    [link.modifierExtension],
    [link.relation.id],
    [link.relation.extension],
    [link.relation.system],
    [link.relation.version],
    [link.relation.code],
    [link.relation.display],
    [link.relation.userSelected],
    [link.relatedDevice.id],
    [link.relatedDevice.extension],
    [link.relatedDevice.concept],
    [link.relatedDevice.reference]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [link.JSON]  VARCHAR(MAX) '$.link'
    ) AS rowset
    CROSS APPLY openjson (rowset.[link.JSON]) with (
        [link.id]                      NVARCHAR(100)       '$.id',
        [link.extension]               NVARCHAR(MAX)       '$.extension',
        [link.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [link.relation.id]             NVARCHAR(100)       '$.relation.id',
        [link.relation.extension]      NVARCHAR(MAX)       '$.relation.extension',
        [link.relation.system]         VARCHAR(256)        '$.relation.system',
        [link.relation.version]        NVARCHAR(100)       '$.relation.version',
        [link.relation.code]           NVARCHAR(4000)      '$.relation.code',
        [link.relation.display]        NVARCHAR(4000)      '$.relation.display',
        [link.relation.userSelected]   bit                 '$.relation.userSelected',
        [link.relatedDevice.id]        NVARCHAR(100)       '$.relatedDevice.id',
        [link.relatedDevice.extension] NVARCHAR(MAX)       '$.relatedDevice.extension',
        [link.relatedDevice.concept]   NVARCHAR(MAX)       '$.relatedDevice.concept',
        [link.relatedDevice.reference] NVARCHAR(MAX)       '$.relatedDevice.reference'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionNote AS
SELECT
    [id],
    [note.JSON],
    [note.id],
    [note.extension],
    [note.time],
    [note.text],
    [note.author.reference.id],
    [note.author.reference.extension],
    [note.author.reference.reference],
    [note.author.reference.type],
    [note.author.reference.identifier],
    [note.author.reference.display],
    [note.author.string]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [note.JSON]  VARCHAR(MAX) '$.note'
    ) AS rowset
    CROSS APPLY openjson (rowset.[note.JSON]) with (
        [note.id]                      NVARCHAR(100)       '$.id',
        [note.extension]               NVARCHAR(MAX)       '$.extension',
        [note.time]                    VARCHAR(64)         '$.time',
        [note.text]                    NVARCHAR(MAX)       '$.text',
        [note.author.reference.id]     NVARCHAR(100)       '$.author.reference.id',
        [note.author.reference.extension] NVARCHAR(MAX)       '$.author.reference.extension',
        [note.author.reference.reference] NVARCHAR(4000)      '$.author.reference.reference',
        [note.author.reference.type]   VARCHAR(256)        '$.author.reference.type',
        [note.author.reference.identifier] NVARCHAR(MAX)       '$.author.reference.identifier',
        [note.author.reference.display] NVARCHAR(4000)      '$.author.reference.display',
        [note.author.string]           NVARCHAR(4000)      '$.author.string'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionMaterial AS
SELECT
    [id],
    [material.JSON],
    [material.id],
    [material.extension],
    [material.modifierExtension],
    [material.substance.id],
    [material.substance.extension],
    [material.substance.coding],
    [material.substance.text],
    [material.alternate],
    [material.allergenicIndicator]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [material.JSON]  VARCHAR(MAX) '$.material'
    ) AS rowset
    CROSS APPLY openjson (rowset.[material.JSON]) with (
        [material.id]                  NVARCHAR(100)       '$.id',
        [material.extension]           NVARCHAR(MAX)       '$.extension',
        [material.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [material.substance.id]        NVARCHAR(100)       '$.substance.id',
        [material.substance.extension] NVARCHAR(MAX)       '$.substance.extension',
        [material.substance.coding]    NVARCHAR(MAX)       '$.substance.coding',
        [material.substance.text]      NVARCHAR(4000)      '$.substance.text',
        [material.alternate]           bit                 '$.alternate',
        [material.allergenicIndicator] bit                 '$.allergenicIndicator'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionProductionIdentifierInUDI AS
SELECT
    [id],
    [productionIdentifierInUDI.JSON],
    [productionIdentifierInUDI]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [productionIdentifierInUDI.JSON]  VARCHAR(MAX) '$.productionIdentifierInUDI'
    ) AS rowset
    CROSS APPLY openjson (rowset.[productionIdentifierInUDI.JSON]) with (
        [productionIdentifierInUDI]    NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionChargeItem AS
SELECT
    [id],
    [chargeItem.JSON],
    [chargeItem.id],
    [chargeItem.extension],
    [chargeItem.modifierExtension],
    [chargeItem.chargeItemCode.id],
    [chargeItem.chargeItemCode.extension],
    [chargeItem.chargeItemCode.concept],
    [chargeItem.chargeItemCode.reference],
    [chargeItem.count.id],
    [chargeItem.count.extension],
    [chargeItem.count.value],
    [chargeItem.count.comparator],
    [chargeItem.count.unit],
    [chargeItem.count.system],
    [chargeItem.count.code],
    [chargeItem.effectivePeriod.id],
    [chargeItem.effectivePeriod.extension],
    [chargeItem.effectivePeriod.start],
    [chargeItem.effectivePeriod.end],
    [chargeItem.useContext]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [chargeItem.JSON]  VARCHAR(MAX) '$.chargeItem'
    ) AS rowset
    CROSS APPLY openjson (rowset.[chargeItem.JSON]) with (
        [chargeItem.id]                NVARCHAR(100)       '$.id',
        [chargeItem.extension]         NVARCHAR(MAX)       '$.extension',
        [chargeItem.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [chargeItem.chargeItemCode.id] NVARCHAR(100)       '$.chargeItemCode.id',
        [chargeItem.chargeItemCode.extension] NVARCHAR(MAX)       '$.chargeItemCode.extension',
        [chargeItem.chargeItemCode.concept] NVARCHAR(MAX)       '$.chargeItemCode.concept',
        [chargeItem.chargeItemCode.reference] NVARCHAR(MAX)       '$.chargeItemCode.reference',
        [chargeItem.count.id]          NVARCHAR(100)       '$.count.id',
        [chargeItem.count.extension]   NVARCHAR(MAX)       '$.count.extension',
        [chargeItem.count.value]       float               '$.count.value',
        [chargeItem.count.comparator]  NVARCHAR(64)        '$.count.comparator',
        [chargeItem.count.unit]        NVARCHAR(100)       '$.count.unit',
        [chargeItem.count.system]      VARCHAR(256)        '$.count.system',
        [chargeItem.count.code]        NVARCHAR(4000)      '$.count.code',
        [chargeItem.effectivePeriod.id] NVARCHAR(100)       '$.effectivePeriod.id',
        [chargeItem.effectivePeriod.extension] NVARCHAR(MAX)       '$.effectivePeriod.extension',
        [chargeItem.effectivePeriod.start] VARCHAR(64)         '$.effectivePeriod.start',
        [chargeItem.effectivePeriod.end] VARCHAR(64)         '$.effectivePeriod.end',
        [chargeItem.useContext]        NVARCHAR(MAX)       '$.useContext' AS JSON
    ) j
