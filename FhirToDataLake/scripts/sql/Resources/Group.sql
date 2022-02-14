CREATE EXTERNAL TABLE [fhir].[Group] (
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
    [active] bit,
    [type] NVARCHAR(64),
    [actual] bit,
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [name] NVARCHAR(500),
    [quantity] bigint,
    [managingEntity.id] NVARCHAR(100),
    [managingEntity.extension] NVARCHAR(MAX),
    [managingEntity.reference] NVARCHAR(4000),
    [managingEntity.type] VARCHAR(256),
    [managingEntity.identifier.id] NVARCHAR(100),
    [managingEntity.identifier.extension] NVARCHAR(MAX),
    [managingEntity.identifier.use] NVARCHAR(64),
    [managingEntity.identifier.type] NVARCHAR(MAX),
    [managingEntity.identifier.system] VARCHAR(256),
    [managingEntity.identifier.value] NVARCHAR(4000),
    [managingEntity.identifier.period] NVARCHAR(MAX),
    [managingEntity.identifier.assigner] NVARCHAR(MAX),
    [managingEntity.display] NVARCHAR(4000),
    [characteristic] VARCHAR(MAX),
    [member] VARCHAR(MAX),
) WITH (
    LOCATION='/Group/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.GroupIdentifier AS
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
        BULK 'Group/**',
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

CREATE VIEW fhir.GroupCharacteristic AS
SELECT
    [id],
    [characteristic.JSON],
    [characteristic.id],
    [characteristic.extension],
    [characteristic.modifierExtension],
    [characteristic.code.id],
    [characteristic.code.extension],
    [characteristic.code.coding],
    [characteristic.code.text],
    [characteristic.exclude],
    [characteristic.period.id],
    [characteristic.period.extension],
    [characteristic.period.start],
    [characteristic.period.end],
    [characteristic.value.codeableConcept.id],
    [characteristic.value.codeableConcept.extension],
    [characteristic.value.codeableConcept.coding],
    [characteristic.value.codeableConcept.text],
    [characteristic.value.boolean],
    [characteristic.value.quantity.id],
    [characteristic.value.quantity.extension],
    [characteristic.value.quantity.value],
    [characteristic.value.quantity.comparator],
    [characteristic.value.quantity.unit],
    [characteristic.value.quantity.system],
    [characteristic.value.quantity.code],
    [characteristic.value.range.id],
    [characteristic.value.range.extension],
    [characteristic.value.range.low],
    [characteristic.value.range.high],
    [characteristic.value.reference.id],
    [characteristic.value.reference.extension],
    [characteristic.value.reference.reference],
    [characteristic.value.reference.type],
    [characteristic.value.reference.identifier],
    [characteristic.value.reference.display]
FROM openrowset (
        BULK 'Group/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [characteristic.JSON]  VARCHAR(MAX) '$.characteristic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[characteristic.JSON]) with (
        [characteristic.id]            NVARCHAR(100)       '$.id',
        [characteristic.extension]     NVARCHAR(MAX)       '$.extension',
        [characteristic.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [characteristic.code.id]       NVARCHAR(100)       '$.code.id',
        [characteristic.code.extension] NVARCHAR(MAX)       '$.code.extension',
        [characteristic.code.coding]   NVARCHAR(MAX)       '$.code.coding',
        [characteristic.code.text]     NVARCHAR(4000)      '$.code.text',
        [characteristic.exclude]       bit                 '$.exclude',
        [characteristic.period.id]     NVARCHAR(100)       '$.period.id',
        [characteristic.period.extension] NVARCHAR(MAX)       '$.period.extension',
        [characteristic.period.start]  VARCHAR(64)         '$.period.start',
        [characteristic.period.end]    VARCHAR(64)         '$.period.end',
        [characteristic.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [characteristic.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [characteristic.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [characteristic.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [characteristic.value.boolean] bit                 '$.value.boolean',
        [characteristic.value.quantity.id] NVARCHAR(100)       '$.value.quantity.id',
        [characteristic.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [characteristic.value.quantity.value] float               '$.value.quantity.value',
        [characteristic.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [characteristic.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [characteristic.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [characteristic.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [characteristic.value.range.id] NVARCHAR(100)       '$.value.range.id',
        [characteristic.value.range.extension] NVARCHAR(MAX)       '$.value.range.extension',
        [characteristic.value.range.low] NVARCHAR(MAX)       '$.value.range.low',
        [characteristic.value.range.high] NVARCHAR(MAX)       '$.value.range.high',
        [characteristic.value.reference.id] NVARCHAR(100)       '$.value.reference.id',
        [characteristic.value.reference.extension] NVARCHAR(MAX)       '$.value.reference.extension',
        [characteristic.value.reference.reference] NVARCHAR(4000)      '$.value.reference.reference',
        [characteristic.value.reference.type] VARCHAR(256)        '$.value.reference.type',
        [characteristic.value.reference.identifier] NVARCHAR(MAX)       '$.value.reference.identifier',
        [characteristic.value.reference.display] NVARCHAR(4000)      '$.value.reference.display'
    ) j

GO

CREATE VIEW fhir.GroupMember AS
SELECT
    [id],
    [member.JSON],
    [member.id],
    [member.extension],
    [member.modifierExtension],
    [member.entity.id],
    [member.entity.extension],
    [member.entity.reference],
    [member.entity.type],
    [member.entity.identifier],
    [member.entity.display],
    [member.period.id],
    [member.period.extension],
    [member.period.start],
    [member.period.end],
    [member.inactive]
FROM openrowset (
        BULK 'Group/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [member.JSON]  VARCHAR(MAX) '$.member'
    ) AS rowset
    CROSS APPLY openjson (rowset.[member.JSON]) with (
        [member.id]                    NVARCHAR(100)       '$.id',
        [member.extension]             NVARCHAR(MAX)       '$.extension',
        [member.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [member.entity.id]             NVARCHAR(100)       '$.entity.id',
        [member.entity.extension]      NVARCHAR(MAX)       '$.entity.extension',
        [member.entity.reference]      NVARCHAR(4000)      '$.entity.reference',
        [member.entity.type]           VARCHAR(256)        '$.entity.type',
        [member.entity.identifier]     NVARCHAR(MAX)       '$.entity.identifier',
        [member.entity.display]        NVARCHAR(4000)      '$.entity.display',
        [member.period.id]             NVARCHAR(100)       '$.period.id',
        [member.period.extension]      NVARCHAR(MAX)       '$.period.extension',
        [member.period.start]          VARCHAR(64)         '$.period.start',
        [member.period.end]            VARCHAR(64)         '$.period.end',
        [member.inactive]              bit                 '$.inactive'
    ) j
