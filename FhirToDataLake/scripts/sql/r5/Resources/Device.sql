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
    [displayName] NVARCHAR(4000),
    [definition.id] NVARCHAR(100),
    [definition.extension] NVARCHAR(MAX),
    [definition.concept.id] NVARCHAR(100),
    [definition.concept.extension] NVARCHAR(MAX),
    [definition.concept.coding] NVARCHAR(MAX),
    [definition.concept.text] NVARCHAR(4000),
    [definition.reference.id] NVARCHAR(100),
    [definition.reference.extension] NVARCHAR(MAX),
    [definition.reference.reference] NVARCHAR(4000),
    [definition.reference.type] VARCHAR(256),
    [definition.reference.identifier] NVARCHAR(MAX),
    [definition.reference.display] NVARCHAR(4000),
    [udiCarrier] VARCHAR(MAX),
    [status] NVARCHAR(100),
    [statusReason] VARCHAR(MAX),
    [biologicalSource.id] NVARCHAR(100),
    [biologicalSource.extension] NVARCHAR(MAX),
    [biologicalSource.use] NVARCHAR(64),
    [biologicalSource.type.id] NVARCHAR(100),
    [biologicalSource.type.extension] NVARCHAR(MAX),
    [biologicalSource.type.coding] NVARCHAR(MAX),
    [biologicalSource.type.text] NVARCHAR(4000),
    [biologicalSource.system] VARCHAR(256),
    [biologicalSource.value] NVARCHAR(4000),
    [biologicalSource.period.id] NVARCHAR(100),
    [biologicalSource.period.extension] NVARCHAR(MAX),
    [biologicalSource.period.start] VARCHAR(64),
    [biologicalSource.period.end] VARCHAR(64),
    [biologicalSource.assigner.id] NVARCHAR(100),
    [biologicalSource.assigner.extension] NVARCHAR(MAX),
    [biologicalSource.assigner.reference] NVARCHAR(4000),
    [biologicalSource.assigner.type] VARCHAR(256),
    [biologicalSource.assigner.identifier] NVARCHAR(MAX),
    [biologicalSource.assigner.display] NVARCHAR(4000),
    [manufacturer] NVARCHAR(500),
    [manufactureDate] VARCHAR(64),
    [expirationDate] VARCHAR(64),
    [lotNumber] NVARCHAR(100),
    [serialNumber] NVARCHAR(100),
    [deviceName] VARCHAR(MAX),
    [modelNumber] NVARCHAR(100),
    [partNumber] NVARCHAR(100),
    [type] VARCHAR(MAX),
    [version] VARCHAR(MAX),
    [property] VARCHAR(MAX),
    [subject.id] NVARCHAR(100),
    [subject.extension] NVARCHAR(MAX),
    [subject.reference] NVARCHAR(4000),
    [subject.type] VARCHAR(256),
    [subject.identifier.id] NVARCHAR(100),
    [subject.identifier.extension] NVARCHAR(MAX),
    [subject.identifier.use] NVARCHAR(64),
    [subject.identifier.type] NVARCHAR(MAX),
    [subject.identifier.system] VARCHAR(256),
    [subject.identifier.value] NVARCHAR(4000),
    [subject.identifier.period] NVARCHAR(MAX),
    [subject.identifier.assigner] NVARCHAR(MAX),
    [subject.display] NVARCHAR(4000),
    [operationalStatus.id] NVARCHAR(100),
    [operationalStatus.extension] NVARCHAR(MAX),
    [operationalStatus.modifierExtension] NVARCHAR(MAX),
    [operationalStatus.value.id] NVARCHAR(100),
    [operationalStatus.value.extension] NVARCHAR(MAX),
    [operationalStatus.value.coding] NVARCHAR(MAX),
    [operationalStatus.value.text] NVARCHAR(4000),
    [operationalStatus.reason] VARCHAR(MAX),
    [associationStatus.id] NVARCHAR(100),
    [associationStatus.extension] NVARCHAR(MAX),
    [associationStatus.modifierExtension] NVARCHAR(MAX),
    [associationStatus.value.id] NVARCHAR(100),
    [associationStatus.value.extension] NVARCHAR(MAX),
    [associationStatus.value.coding] NVARCHAR(MAX),
    [associationStatus.value.text] NVARCHAR(4000),
    [associationStatus.reason] VARCHAR(MAX),
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
    [endpoint] VARCHAR(MAX),
    [link] VARCHAR(MAX),
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
        [udiCarrier.entryType]         NVARCHAR(4000)      '$.entryType'
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
        [deviceName.type]              NVARCHAR(100)       '$.type'
    ) j

GO

CREATE VIEW fhir.DeviceType AS
SELECT
    [id],
    [type.JSON],
    [type.id],
    [type.extension],
    [type.coding],
    [type.text]
FROM openrowset (
        BULK 'Device/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [type.JSON]  VARCHAR(MAX) '$.type'
    ) AS rowset
    CROSS APPLY openjson (rowset.[type.JSON]) with (
        [type.id]                      NVARCHAR(100)       '$.id',
        [type.extension]               NVARCHAR(MAX)       '$.extension',
        [type.coding]                  NVARCHAR(MAX)       '$.coding' AS JSON,
        [type.text]                    NVARCHAR(4000)      '$.text'
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

CREATE VIEW fhir.DeviceEndpoint AS
SELECT
    [id],
    [endpoint.JSON],
    [endpoint.id],
    [endpoint.extension],
    [endpoint.reference],
    [endpoint.type],
    [endpoint.identifier.id],
    [endpoint.identifier.extension],
    [endpoint.identifier.use],
    [endpoint.identifier.type],
    [endpoint.identifier.system],
    [endpoint.identifier.value],
    [endpoint.identifier.period],
    [endpoint.identifier.assigner],
    [endpoint.display]
FROM openrowset (
        BULK 'Device/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [endpoint.JSON]  VARCHAR(MAX) '$.endpoint'
    ) AS rowset
    CROSS APPLY openjson (rowset.[endpoint.JSON]) with (
        [endpoint.id]                  NVARCHAR(100)       '$.id',
        [endpoint.extension]           NVARCHAR(MAX)       '$.extension',
        [endpoint.reference]           NVARCHAR(4000)      '$.reference',
        [endpoint.type]                VARCHAR(256)        '$.type',
        [endpoint.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [endpoint.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [endpoint.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [endpoint.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [endpoint.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [endpoint.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [endpoint.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [endpoint.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [endpoint.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DeviceLink AS
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
        BULK 'Device/**',
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
