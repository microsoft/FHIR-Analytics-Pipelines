CREATE EXTERNAL TABLE [fhir].[SupplyRequest] (
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
    [status] NVARCHAR(100),
    [basedOn] VARCHAR(MAX),
    [category.id] NVARCHAR(100),
    [category.extension] NVARCHAR(MAX),
    [category.coding] VARCHAR(MAX),
    [category.text] NVARCHAR(4000),
    [priority] NVARCHAR(100),
    [item.id] NVARCHAR(100),
    [item.extension] NVARCHAR(MAX),
    [item.concept.id] NVARCHAR(100),
    [item.concept.extension] NVARCHAR(MAX),
    [item.concept.coding] NVARCHAR(MAX),
    [item.concept.text] NVARCHAR(4000),
    [item.reference.id] NVARCHAR(100),
    [item.reference.extension] NVARCHAR(MAX),
    [item.reference.reference] NVARCHAR(4000),
    [item.reference.type] VARCHAR(256),
    [item.reference.identifier] NVARCHAR(MAX),
    [item.reference.display] NVARCHAR(4000),
    [quantity.id] NVARCHAR(100),
    [quantity.extension] NVARCHAR(MAX),
    [quantity.value] float,
    [quantity.comparator] NVARCHAR(64),
    [quantity.unit] NVARCHAR(100),
    [quantity.system] VARCHAR(256),
    [quantity.code] NVARCHAR(4000),
    [parameter] VARCHAR(MAX),
    [authoredOn] VARCHAR(64),
    [requester.id] NVARCHAR(100),
    [requester.extension] NVARCHAR(MAX),
    [requester.reference] NVARCHAR(4000),
    [requester.type] VARCHAR(256),
    [requester.identifier.id] NVARCHAR(100),
    [requester.identifier.extension] NVARCHAR(MAX),
    [requester.identifier.use] NVARCHAR(64),
    [requester.identifier.type] NVARCHAR(MAX),
    [requester.identifier.system] VARCHAR(256),
    [requester.identifier.value] NVARCHAR(4000),
    [requester.identifier.period] NVARCHAR(MAX),
    [requester.identifier.assigner] NVARCHAR(MAX),
    [requester.display] NVARCHAR(4000),
    [supplier] VARCHAR(MAX),
    [reason] VARCHAR(MAX),
    [deliverFrom.id] NVARCHAR(100),
    [deliverFrom.extension] NVARCHAR(MAX),
    [deliverFrom.reference] NVARCHAR(4000),
    [deliverFrom.type] VARCHAR(256),
    [deliverFrom.identifier.id] NVARCHAR(100),
    [deliverFrom.identifier.extension] NVARCHAR(MAX),
    [deliverFrom.identifier.use] NVARCHAR(64),
    [deliverFrom.identifier.type] NVARCHAR(MAX),
    [deliverFrom.identifier.system] VARCHAR(256),
    [deliverFrom.identifier.value] NVARCHAR(4000),
    [deliverFrom.identifier.period] NVARCHAR(MAX),
    [deliverFrom.identifier.assigner] NVARCHAR(MAX),
    [deliverFrom.display] NVARCHAR(4000),
    [deliverTo.id] NVARCHAR(100),
    [deliverTo.extension] NVARCHAR(MAX),
    [deliverTo.reference] NVARCHAR(4000),
    [deliverTo.type] VARCHAR(256),
    [deliverTo.identifier.id] NVARCHAR(100),
    [deliverTo.identifier.extension] NVARCHAR(MAX),
    [deliverTo.identifier.use] NVARCHAR(64),
    [deliverTo.identifier.type] NVARCHAR(MAX),
    [deliverTo.identifier.system] VARCHAR(256),
    [deliverTo.identifier.value] NVARCHAR(4000),
    [deliverTo.identifier.period] NVARCHAR(MAX),
    [deliverTo.identifier.assigner] NVARCHAR(MAX),
    [deliverTo.display] NVARCHAR(4000),
    [occurrence.dateTime] VARCHAR(64),
    [occurrence.period.id] NVARCHAR(100),
    [occurrence.period.extension] NVARCHAR(MAX),
    [occurrence.period.start] VARCHAR(64),
    [occurrence.period.end] VARCHAR(64),
    [occurrence.timing.id] NVARCHAR(100),
    [occurrence.timing.extension] NVARCHAR(MAX),
    [occurrence.timing.modifierExtension] NVARCHAR(MAX),
    [occurrence.timing.event] VARCHAR(MAX),
    [occurrence.timing.repeat.id] NVARCHAR(100),
    [occurrence.timing.repeat.extension] NVARCHAR(MAX),
    [occurrence.timing.repeat.modifierExtension] NVARCHAR(MAX),
    [occurrence.timing.repeat.count] bigint,
    [occurrence.timing.repeat.countMax] bigint,
    [occurrence.timing.repeat.duration] float,
    [occurrence.timing.repeat.durationMax] float,
    [occurrence.timing.repeat.durationUnit] NVARCHAR(64),
    [occurrence.timing.repeat.frequency] bigint,
    [occurrence.timing.repeat.frequencyMax] bigint,
    [occurrence.timing.repeat.period] float,
    [occurrence.timing.repeat.periodMax] float,
    [occurrence.timing.repeat.periodUnit] NVARCHAR(64),
    [occurrence.timing.repeat.dayOfWeek] NVARCHAR(MAX),
    [occurrence.timing.repeat.timeOfDay] NVARCHAR(MAX),
    [occurrence.timing.repeat.when] NVARCHAR(MAX),
    [occurrence.timing.repeat.offset] bigint,
    [occurrence.timing.repeat.bounds.duration] NVARCHAR(MAX),
    [occurrence.timing.repeat.bounds.range] NVARCHAR(MAX),
    [occurrence.timing.repeat.bounds.period] NVARCHAR(MAX),
    [occurrence.timing.code.id] NVARCHAR(100),
    [occurrence.timing.code.extension] NVARCHAR(MAX),
    [occurrence.timing.code.coding] NVARCHAR(MAX),
    [occurrence.timing.code.text] NVARCHAR(4000),
) WITH (
    LOCATION='/SupplyRequest/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SupplyRequestIdentifier AS
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
        BULK 'SupplyRequest/**',
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

CREATE VIEW fhir.SupplyRequestBasedOn AS
SELECT
    [id],
    [basedOn.JSON],
    [basedOn.id],
    [basedOn.extension],
    [basedOn.reference],
    [basedOn.type],
    [basedOn.identifier.id],
    [basedOn.identifier.extension],
    [basedOn.identifier.use],
    [basedOn.identifier.type],
    [basedOn.identifier.system],
    [basedOn.identifier.value],
    [basedOn.identifier.period],
    [basedOn.identifier.assigner],
    [basedOn.display]
FROM openrowset (
        BULK 'SupplyRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [basedOn.JSON]  VARCHAR(MAX) '$.basedOn'
    ) AS rowset
    CROSS APPLY openjson (rowset.[basedOn.JSON]) with (
        [basedOn.id]                   NVARCHAR(100)       '$.id',
        [basedOn.extension]            NVARCHAR(MAX)       '$.extension',
        [basedOn.reference]            NVARCHAR(4000)      '$.reference',
        [basedOn.type]                 VARCHAR(256)        '$.type',
        [basedOn.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [basedOn.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [basedOn.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [basedOn.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [basedOn.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [basedOn.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [basedOn.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [basedOn.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [basedOn.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.SupplyRequestParameter AS
SELECT
    [id],
    [parameter.JSON],
    [parameter.id],
    [parameter.extension],
    [parameter.modifierExtension],
    [parameter.code.id],
    [parameter.code.extension],
    [parameter.code.coding],
    [parameter.code.text],
    [parameter.value.codeableConcept.id],
    [parameter.value.codeableConcept.extension],
    [parameter.value.codeableConcept.coding],
    [parameter.value.codeableConcept.text],
    [parameter.value.quantity.id],
    [parameter.value.quantity.extension],
    [parameter.value.quantity.value],
    [parameter.value.quantity.comparator],
    [parameter.value.quantity.unit],
    [parameter.value.quantity.system],
    [parameter.value.quantity.code],
    [parameter.value.range.id],
    [parameter.value.range.extension],
    [parameter.value.range.low],
    [parameter.value.range.high],
    [parameter.value.boolean]
FROM openrowset (
        BULK 'SupplyRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [parameter.JSON]  VARCHAR(MAX) '$.parameter'
    ) AS rowset
    CROSS APPLY openjson (rowset.[parameter.JSON]) with (
        [parameter.id]                 NVARCHAR(100)       '$.id',
        [parameter.extension]          NVARCHAR(MAX)       '$.extension',
        [parameter.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [parameter.code.id]            NVARCHAR(100)       '$.code.id',
        [parameter.code.extension]     NVARCHAR(MAX)       '$.code.extension',
        [parameter.code.coding]        NVARCHAR(MAX)       '$.code.coding',
        [parameter.code.text]          NVARCHAR(4000)      '$.code.text',
        [parameter.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [parameter.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [parameter.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [parameter.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [parameter.value.quantity.id]  NVARCHAR(100)       '$.value.quantity.id',
        [parameter.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [parameter.value.quantity.value] float               '$.value.quantity.value',
        [parameter.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [parameter.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [parameter.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [parameter.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [parameter.value.range.id]     NVARCHAR(100)       '$.value.range.id',
        [parameter.value.range.extension] NVARCHAR(MAX)       '$.value.range.extension',
        [parameter.value.range.low]    NVARCHAR(MAX)       '$.value.range.low',
        [parameter.value.range.high]   NVARCHAR(MAX)       '$.value.range.high',
        [parameter.value.boolean]      bit                 '$.value.boolean'
    ) j

GO

CREATE VIEW fhir.SupplyRequestSupplier AS
SELECT
    [id],
    [supplier.JSON],
    [supplier.id],
    [supplier.extension],
    [supplier.reference],
    [supplier.type],
    [supplier.identifier.id],
    [supplier.identifier.extension],
    [supplier.identifier.use],
    [supplier.identifier.type],
    [supplier.identifier.system],
    [supplier.identifier.value],
    [supplier.identifier.period],
    [supplier.identifier.assigner],
    [supplier.display]
FROM openrowset (
        BULK 'SupplyRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [supplier.JSON]  VARCHAR(MAX) '$.supplier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[supplier.JSON]) with (
        [supplier.id]                  NVARCHAR(100)       '$.id',
        [supplier.extension]           NVARCHAR(MAX)       '$.extension',
        [supplier.reference]           NVARCHAR(4000)      '$.reference',
        [supplier.type]                VARCHAR(256)        '$.type',
        [supplier.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [supplier.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [supplier.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [supplier.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [supplier.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [supplier.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [supplier.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [supplier.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [supplier.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.SupplyRequestReason AS
SELECT
    [id],
    [reason.JSON],
    [reason.id],
    [reason.extension],
    [reason.concept.id],
    [reason.concept.extension],
    [reason.concept.coding],
    [reason.concept.text],
    [reason.reference.id],
    [reason.reference.extension],
    [reason.reference.reference],
    [reason.reference.type],
    [reason.reference.identifier],
    [reason.reference.display]
FROM openrowset (
        BULK 'SupplyRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reason.JSON]  VARCHAR(MAX) '$.reason'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reason.JSON]) with (
        [reason.id]                    NVARCHAR(100)       '$.id',
        [reason.extension]             NVARCHAR(MAX)       '$.extension',
        [reason.concept.id]            NVARCHAR(100)       '$.concept.id',
        [reason.concept.extension]     NVARCHAR(MAX)       '$.concept.extension',
        [reason.concept.coding]        NVARCHAR(MAX)       '$.concept.coding',
        [reason.concept.text]          NVARCHAR(4000)      '$.concept.text',
        [reason.reference.id]          NVARCHAR(100)       '$.reference.id',
        [reason.reference.extension]   NVARCHAR(MAX)       '$.reference.extension',
        [reason.reference.reference]   NVARCHAR(4000)      '$.reference.reference',
        [reason.reference.type]        VARCHAR(256)        '$.reference.type',
        [reason.reference.identifier]  NVARCHAR(MAX)       '$.reference.identifier',
        [reason.reference.display]     NVARCHAR(4000)      '$.reference.display'
    ) j
