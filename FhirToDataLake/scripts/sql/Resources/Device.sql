CREATE EXTERNAL TABLE [fhir].[Device] (
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
    [definition.id] NVARCHAR(100),
    [definition.extension] NVARCHAR(MAX),
    [definition.reference] NVARCHAR(4000),
    [definition.type] VARCHAR(256),
    [definition.identifier.id] NVARCHAR(100),
    [definition.identifier.extension] NVARCHAR(MAX),
    [definition.identifier.use] NVARCHAR(64),
    [definition.identifier.type] NVARCHAR(MAX),
    [definition.identifier.system] VARCHAR(256),
    [definition.identifier.value] NVARCHAR(4000),
    [definition.identifier.period] NVARCHAR(MAX),
    [definition.identifier.assigner] NVARCHAR(MAX),
    [definition.display] NVARCHAR(4000),
    [udiCarrier] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [statusReason] VARCHAR(MAX),
    [distinctIdentifier] NVARCHAR(500),
    [manufacturer] NVARCHAR(500),
    [manufactureDate] VARCHAR(64),
    [expirationDate] VARCHAR(64),
    [lotNumber] NVARCHAR(100),
    [serialNumber] NVARCHAR(100),
    [deviceName] VARCHAR(MAX),
    [modelNumber] NVARCHAR(100),
    [partNumber] NVARCHAR(100),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [specialization] VARCHAR(MAX),
    [version] VARCHAR(MAX),
    [property] VARCHAR(MAX),
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
    [location.id] NVARCHAR(100),
    [location.extension] NVARCHAR(MAX),
    [location.reference] NVARCHAR(4000),
    [location.type] VARCHAR(256),
    [location.identifier.id] NVARCHAR(100),
    [location.identifier.extension] NVARCHAR(MAX),
    [location.identifier.use] NVARCHAR(64),
    [location.identifier.type] NVARCHAR(MAX),
    [location.identifier.system] VARCHAR(256),
    [location.identifier.value] NVARCHAR(4000),
    [location.identifier.period] NVARCHAR(MAX),
    [location.identifier.assigner] NVARCHAR(MAX),
    [location.display] NVARCHAR(4000),
    [url] VARCHAR(256),
    [note] VARCHAR(MAX),
    [safety] VARCHAR(MAX),
    [parent.id] NVARCHAR(100),
    [parent.extension] NVARCHAR(MAX),
    [parent.reference] NVARCHAR(4000),
    [parent.type] VARCHAR(256),
    [parent.identifier.id] NVARCHAR(100),
    [parent.identifier.extension] NVARCHAR(MAX),
    [parent.identifier.use] NVARCHAR(64),
    [parent.identifier.type] NVARCHAR(MAX),
    [parent.identifier.system] VARCHAR(256),
    [parent.identifier.value] NVARCHAR(4000),
    [parent.identifier.period] NVARCHAR(MAX),
    [parent.identifier.assigner] NVARCHAR(MAX),
    [parent.display] NVARCHAR(4000),
) WITH (
    LOCATION='/Device/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.DeviceIdentifier AS
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
        BULK 'Device/**',
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

CREATE VIEW fhir.DeviceUdiCarrier AS
SELECT
    [id],
    [udiCarrier.JSON],
    [udiCarrier.id],
    [udiCarrier.extension],
    [udiCarrier.modifierExtension],
    [udiCarrier.deviceIdentifier],
    [udiCarrier.issuer],
    [udiCarrier.jurisdiction],
    [udiCarrier.carrierAIDC],
    [udiCarrier.carrierHRF],
    [udiCarrier.entryType]
FROM openrowset (
        BULK 'Device/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [udiCarrier.JSON]  VARCHAR(MAX) '$.udiCarrier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[udiCarrier.JSON]) with (
        [udiCarrier.id]                NVARCHAR(100)       '$.id',
        [udiCarrier.extension]         NVARCHAR(MAX)       '$.extension',
        [udiCarrier.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [udiCarrier.deviceIdentifier]  NVARCHAR(500)       '$.deviceIdentifier',
        [udiCarrier.issuer]            VARCHAR(256)        '$.issuer',
        [udiCarrier.jurisdiction]      VARCHAR(256)        '$.jurisdiction',
        [udiCarrier.carrierAIDC]       NVARCHAR(MAX)       '$.carrierAIDC',
        [udiCarrier.carrierHRF]        NVARCHAR(500)       '$.carrierHRF',
        [udiCarrier.entryType]         NVARCHAR(64)        '$.entryType'
    ) j

GO

CREATE VIEW fhir.DeviceStatusReason AS
SELECT
    [id],
    [statusReason.JSON],
    [statusReason.id],
    [statusReason.extension],
    [statusReason.coding],
    [statusReason.text]
FROM openrowset (
        BULK 'Device/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [statusReason.JSON]  VARCHAR(MAX) '$.statusReason'
    ) AS rowset
    CROSS APPLY openjson (rowset.[statusReason.JSON]) with (
        [statusReason.id]              NVARCHAR(100)       '$.id',
        [statusReason.extension]       NVARCHAR(MAX)       '$.extension',
        [statusReason.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [statusReason.text]            NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.DeviceDeviceName AS
SELECT
    [id],
    [deviceName.JSON],
    [deviceName.id],
    [deviceName.extension],
    [deviceName.modifierExtension],
    [deviceName.name],
    [deviceName.type]
FROM openrowset (
        BULK 'Device/**',
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

CREATE VIEW fhir.DeviceSpecialization AS
SELECT
    [id],
    [specialization.JSON],
    [specialization.id],
    [specialization.extension],
    [specialization.modifierExtension],
    [specialization.systemType.id],
    [specialization.systemType.extension],
    [specialization.systemType.coding],
    [specialization.systemType.text],
    [specialization.version]
FROM openrowset (
        BULK 'Device/**',
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
        [specialization.systemType.id] NVARCHAR(100)       '$.systemType.id',
        [specialization.systemType.extension] NVARCHAR(MAX)       '$.systemType.extension',
        [specialization.systemType.coding] NVARCHAR(MAX)       '$.systemType.coding',
        [specialization.systemType.text] NVARCHAR(4000)      '$.systemType.text',
        [specialization.version]       NVARCHAR(100)       '$.version'
    ) j

GO

CREATE VIEW fhir.DeviceVersion AS
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
        BULK 'Device/**',
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

CREATE VIEW fhir.DeviceProperty AS
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
        BULK 'Device/**',
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

CREATE VIEW fhir.DeviceContact AS
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
        BULK 'Device/**',
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

CREATE VIEW fhir.DeviceNote AS
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
        BULK 'Device/**',
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

CREATE VIEW fhir.DeviceSafety AS
SELECT
    [id],
    [safety.JSON],
    [safety.id],
    [safety.extension],
    [safety.coding],
    [safety.text]
FROM openrowset (
        BULK 'Device/**',
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
