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
    [identifier] VARCHAR(MAX),
    [udiDeviceIdentifier] VARCHAR(MAX),
    [deviceName] VARCHAR(MAX),
    [modelNumber] NVARCHAR(100),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [specialization] VARCHAR(MAX),
    [version] VARCHAR(MAX),
    [safety] VARCHAR(MAX),
    [shelfLifeStorage] VARCHAR(MAX),
    [physicalCharacteristics.id] NVARCHAR(100),
    [physicalCharacteristics.extension] NVARCHAR(MAX),
    [physicalCharacteristics.modifierExtension] NVARCHAR(MAX),
    [physicalCharacteristics.height.id] NVARCHAR(100),
    [physicalCharacteristics.height.extension] NVARCHAR(MAX),
    [physicalCharacteristics.height.value] float,
    [physicalCharacteristics.height.comparator] NVARCHAR(64),
    [physicalCharacteristics.height.unit] NVARCHAR(100),
    [physicalCharacteristics.height.system] VARCHAR(256),
    [physicalCharacteristics.height.code] NVARCHAR(4000),
    [physicalCharacteristics.width.id] NVARCHAR(100),
    [physicalCharacteristics.width.extension] NVARCHAR(MAX),
    [physicalCharacteristics.width.value] float,
    [physicalCharacteristics.width.comparator] NVARCHAR(64),
    [physicalCharacteristics.width.unit] NVARCHAR(100),
    [physicalCharacteristics.width.system] VARCHAR(256),
    [physicalCharacteristics.width.code] NVARCHAR(4000),
    [physicalCharacteristics.depth.id] NVARCHAR(100),
    [physicalCharacteristics.depth.extension] NVARCHAR(MAX),
    [physicalCharacteristics.depth.value] float,
    [physicalCharacteristics.depth.comparator] NVARCHAR(64),
    [physicalCharacteristics.depth.unit] NVARCHAR(100),
    [physicalCharacteristics.depth.system] VARCHAR(256),
    [physicalCharacteristics.depth.code] NVARCHAR(4000),
    [physicalCharacteristics.weight.id] NVARCHAR(100),
    [physicalCharacteristics.weight.extension] NVARCHAR(MAX),
    [physicalCharacteristics.weight.value] float,
    [physicalCharacteristics.weight.comparator] NVARCHAR(64),
    [physicalCharacteristics.weight.unit] NVARCHAR(100),
    [physicalCharacteristics.weight.system] VARCHAR(256),
    [physicalCharacteristics.weight.code] NVARCHAR(4000),
    [physicalCharacteristics.nominalVolume.id] NVARCHAR(100),
    [physicalCharacteristics.nominalVolume.extension] NVARCHAR(MAX),
    [physicalCharacteristics.nominalVolume.value] float,
    [physicalCharacteristics.nominalVolume.comparator] NVARCHAR(64),
    [physicalCharacteristics.nominalVolume.unit] NVARCHAR(100),
    [physicalCharacteristics.nominalVolume.system] VARCHAR(256),
    [physicalCharacteristics.nominalVolume.code] NVARCHAR(4000),
    [physicalCharacteristics.externalDiameter.id] NVARCHAR(100),
    [physicalCharacteristics.externalDiameter.extension] NVARCHAR(MAX),
    [physicalCharacteristics.externalDiameter.value] float,
    [physicalCharacteristics.externalDiameter.comparator] NVARCHAR(64),
    [physicalCharacteristics.externalDiameter.unit] NVARCHAR(100),
    [physicalCharacteristics.externalDiameter.system] VARCHAR(256),
    [physicalCharacteristics.externalDiameter.code] NVARCHAR(4000),
    [physicalCharacteristics.shape] NVARCHAR(100),
    [physicalCharacteristics.color] VARCHAR(MAX),
    [physicalCharacteristics.imprint] VARCHAR(MAX),
    [physicalCharacteristics.image] VARCHAR(MAX),
    [physicalCharacteristics.scoring.id] NVARCHAR(100),
    [physicalCharacteristics.scoring.extension] NVARCHAR(MAX),
    [physicalCharacteristics.scoring.coding] NVARCHAR(MAX),
    [physicalCharacteristics.scoring.text] NVARCHAR(4000),
    [languageCode] VARCHAR(MAX),
    [capability] VARCHAR(MAX),
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
    [url] VARCHAR(256),
    [onlineInformation] VARCHAR(256),
    [note] VARCHAR(MAX),
    [quantity.id] NVARCHAR(100),
    [quantity.extension] NVARCHAR(MAX),
    [quantity.value] float,
    [quantity.comparator] NVARCHAR(64),
    [quantity.unit] NVARCHAR(100),
    [quantity.system] VARCHAR(256),
    [quantity.code] NVARCHAR(4000),
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
    [udiDeviceIdentifier.jurisdiction]
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
        [udiDeviceIdentifier.jurisdiction] VARCHAR(256)        '$.jurisdiction'
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
        [deviceName.type]              NVARCHAR(64)        '$.type'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionSpecialization AS
SELECT
    [id],
    [specialization.JSON],
    [specialization.id],
    [specialization.extension],
    [specialization.modifierExtension],
    [specialization.systemType],
    [specialization.version]
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
        [specialization.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [specialization.systemType]    NVARCHAR(100)       '$.systemType',
        [specialization.version]       NVARCHAR(100)       '$.version'
    ) j

GO

CREATE VIEW fhir.DeviceDefinitionVersion AS
SELECT
    [id],
    [version.JSON],
    [version]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [version.JSON]  VARCHAR(MAX) '$.version'
    ) AS rowset
    CROSS APPLY openjson (rowset.[version.JSON]) with (
        [version]                      NVARCHAR(MAX)       '$'
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
    [shelfLifeStorage.identifier.id],
    [shelfLifeStorage.identifier.extension],
    [shelfLifeStorage.identifier.use],
    [shelfLifeStorage.identifier.type],
    [shelfLifeStorage.identifier.system],
    [shelfLifeStorage.identifier.value],
    [shelfLifeStorage.identifier.period],
    [shelfLifeStorage.identifier.assigner],
    [shelfLifeStorage.type.id],
    [shelfLifeStorage.type.extension],
    [shelfLifeStorage.type.coding],
    [shelfLifeStorage.type.text],
    [shelfLifeStorage.period.id],
    [shelfLifeStorage.period.extension],
    [shelfLifeStorage.period.value],
    [shelfLifeStorage.period.comparator],
    [shelfLifeStorage.period.unit],
    [shelfLifeStorage.period.system],
    [shelfLifeStorage.period.code],
    [shelfLifeStorage.specialPrecautionsForStorage]
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
        [shelfLifeStorage.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [shelfLifeStorage.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [shelfLifeStorage.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [shelfLifeStorage.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [shelfLifeStorage.identifier.system] VARCHAR(256)        '$.identifier.system',
        [shelfLifeStorage.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [shelfLifeStorage.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [shelfLifeStorage.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [shelfLifeStorage.type.id]     NVARCHAR(100)       '$.type.id',
        [shelfLifeStorage.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [shelfLifeStorage.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [shelfLifeStorage.type.text]   NVARCHAR(4000)      '$.type.text',
        [shelfLifeStorage.period.id]   NVARCHAR(100)       '$.period.id',
        [shelfLifeStorage.period.extension] NVARCHAR(MAX)       '$.period.extension',
        [shelfLifeStorage.period.value] float               '$.period.value',
        [shelfLifeStorage.period.comparator] NVARCHAR(64)        '$.period.comparator',
        [shelfLifeStorage.period.unit] NVARCHAR(100)       '$.period.unit',
        [shelfLifeStorage.period.system] VARCHAR(256)        '$.period.system',
        [shelfLifeStorage.period.code] NVARCHAR(4000)      '$.period.code',
        [shelfLifeStorage.specialPrecautionsForStorage] NVARCHAR(MAX)       '$.specialPrecautionsForStorage' AS JSON
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

CREATE VIEW fhir.DeviceDefinitionCapability AS
SELECT
    [id],
    [capability.JSON],
    [capability.id],
    [capability.extension],
    [capability.modifierExtension],
    [capability.type.id],
    [capability.type.extension],
    [capability.type.coding],
    [capability.type.text],
    [capability.description]
FROM openrowset (
        BULK 'DeviceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [capability.JSON]  VARCHAR(MAX) '$.capability'
    ) AS rowset
    CROSS APPLY openjson (rowset.[capability.JSON]) with (
        [capability.id]                NVARCHAR(100)       '$.id',
        [capability.extension]         NVARCHAR(MAX)       '$.extension',
        [capability.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [capability.type.id]           NVARCHAR(100)       '$.type.id',
        [capability.type.extension]    NVARCHAR(MAX)       '$.type.extension',
        [capability.type.coding]       NVARCHAR(MAX)       '$.type.coding',
        [capability.type.text]         NVARCHAR(4000)      '$.type.text',
        [capability.description]       NVARCHAR(MAX)       '$.description' AS JSON
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
    [property.valueQuantity],
    [property.valueCode]
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
        [property.valueQuantity]       NVARCHAR(MAX)       '$.valueQuantity' AS JSON,
        [property.valueCode]           NVARCHAR(MAX)       '$.valueCode' AS JSON
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
