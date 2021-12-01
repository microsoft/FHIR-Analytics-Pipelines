CREATE EXTERNAL TABLE [fhir].[Group] (
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
    [identifier] VARCHAR(MAX),
    [active] bit,
    [type] NVARCHAR(64),
    [actual] bit,
    [code.id] NVARCHAR(4000),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [name] NVARCHAR(4000),
    [quantity] bigint,
    [managingEntity.id] NVARCHAR(4000),
    [managingEntity.extension] NVARCHAR(MAX),
    [managingEntity.reference] NVARCHAR(4000),
    [managingEntity.type] VARCHAR(256),
    [managingEntity.identifier.id] NVARCHAR(4000),
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
    [characteristic.value.CodeableConcept.id],
    [characteristic.value.CodeableConcept.extension],
    [characteristic.value.CodeableConcept.coding],
    [characteristic.value.CodeableConcept.text],
    [characteristic.value.boolean],
    [characteristic.value.Quantity.id],
    [characteristic.value.Quantity.extension],
    [characteristic.value.Quantity.value],
    [characteristic.value.Quantity.comparator],
    [characteristic.value.Quantity.unit],
    [characteristic.value.Quantity.system],
    [characteristic.value.Quantity.code],
    [characteristic.value.Range.id],
    [characteristic.value.Range.extension],
    [characteristic.value.Range.low],
    [characteristic.value.Range.high],
    [characteristic.value.Reference.id],
    [characteristic.value.Reference.extension],
    [characteristic.value.Reference.reference],
    [characteristic.value.Reference.type],
    [characteristic.value.Reference.identifier],
    [characteristic.value.Reference.display]
FROM openrowset (
        BULK 'Group/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [characteristic.JSON]  VARCHAR(MAX) '$.characteristic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[characteristic.JSON]) with (
        [characteristic.id]            NVARCHAR(4000)      '$.id',
        [characteristic.extension]     NVARCHAR(MAX)       '$.extension',
        [characteristic.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [characteristic.code.id]       NVARCHAR(4000)      '$.code.id',
        [characteristic.code.extension] NVARCHAR(MAX)       '$.code.extension',
        [characteristic.code.coding]   NVARCHAR(MAX)       '$.code.coding',
        [characteristic.code.text]     NVARCHAR(4000)      '$.code.text',
        [characteristic.exclude]       bit                 '$.exclude',
        [characteristic.period.id]     NVARCHAR(4000)      '$.period.id',
        [characteristic.period.extension] NVARCHAR(MAX)       '$.period.extension',
        [characteristic.period.start]  VARCHAR(30)         '$.period.start',
        [characteristic.period.end]    VARCHAR(30)         '$.period.end',
        [characteristic.value.CodeableConcept.id] NVARCHAR(4000)      '$.value.CodeableConcept.id',
        [characteristic.value.CodeableConcept.extension] NVARCHAR(MAX)       '$.value.CodeableConcept.extension',
        [characteristic.value.CodeableConcept.coding] NVARCHAR(MAX)       '$.value.CodeableConcept.coding',
        [characteristic.value.CodeableConcept.text] NVARCHAR(4000)      '$.value.CodeableConcept.text',
        [characteristic.value.boolean] bit                 '$.value.boolean',
        [characteristic.value.Quantity.id] NVARCHAR(4000)      '$.value.Quantity.id',
        [characteristic.value.Quantity.extension] NVARCHAR(MAX)       '$.value.Quantity.extension',
        [characteristic.value.Quantity.value] float               '$.value.Quantity.value',
        [characteristic.value.Quantity.comparator] NVARCHAR(64)        '$.value.Quantity.comparator',
        [characteristic.value.Quantity.unit] NVARCHAR(4000)      '$.value.Quantity.unit',
        [characteristic.value.Quantity.system] VARCHAR(256)        '$.value.Quantity.system',
        [characteristic.value.Quantity.code] NVARCHAR(4000)      '$.value.Quantity.code',
        [characteristic.value.Range.id] NVARCHAR(4000)      '$.value.Range.id',
        [characteristic.value.Range.extension] NVARCHAR(MAX)       '$.value.Range.extension',
        [characteristic.value.Range.low] NVARCHAR(MAX)       '$.value.Range.low',
        [characteristic.value.Range.high] NVARCHAR(MAX)       '$.value.Range.high',
        [characteristic.value.Reference.id] NVARCHAR(4000)      '$.value.Reference.id',
        [characteristic.value.Reference.extension] NVARCHAR(MAX)       '$.value.Reference.extension',
        [characteristic.value.Reference.reference] NVARCHAR(4000)      '$.value.Reference.reference',
        [characteristic.value.Reference.type] VARCHAR(256)        '$.value.Reference.type',
        [characteristic.value.Reference.identifier] NVARCHAR(MAX)       '$.value.Reference.identifier',
        [characteristic.value.Reference.display] NVARCHAR(4000)      '$.value.Reference.display'
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
        [member.id]                    NVARCHAR(4000)      '$.id',
        [member.extension]             NVARCHAR(MAX)       '$.extension',
        [member.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [member.entity.id]             NVARCHAR(4000)      '$.entity.id',
        [member.entity.extension]      NVARCHAR(MAX)       '$.entity.extension',
        [member.entity.reference]      NVARCHAR(4000)      '$.entity.reference',
        [member.entity.type]           VARCHAR(256)        '$.entity.type',
        [member.entity.identifier]     NVARCHAR(MAX)       '$.entity.identifier',
        [member.entity.display]        NVARCHAR(4000)      '$.entity.display',
        [member.period.id]             NVARCHAR(4000)      '$.period.id',
        [member.period.extension]      NVARCHAR(MAX)       '$.period.extension',
        [member.period.start]          VARCHAR(30)         '$.period.start',
        [member.period.end]            VARCHAR(30)         '$.period.end',
        [member.inactive]              bit                 '$.inactive'
    ) j
