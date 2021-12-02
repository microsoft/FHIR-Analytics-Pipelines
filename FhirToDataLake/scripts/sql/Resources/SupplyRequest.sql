CREATE EXTERNAL TABLE [fhir].[SupplyRequest] (
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
    [status] NVARCHAR(64),
    [category.id] NVARCHAR(4000),
    [category.extension] NVARCHAR(MAX),
    [category.coding] VARCHAR(MAX),
    [category.text] NVARCHAR(4000),
    [priority] NVARCHAR(4000),
    [quantity.id] NVARCHAR(4000),
    [quantity.extension] NVARCHAR(MAX),
    [quantity.value] float,
    [quantity.comparator] NVARCHAR(64),
    [quantity.unit] NVARCHAR(4000),
    [quantity.system] VARCHAR(256),
    [quantity.code] NVARCHAR(4000),
    [parameter] VARCHAR(MAX),
    [authoredOn] VARCHAR(30),
    [requester.id] NVARCHAR(4000),
    [requester.extension] NVARCHAR(MAX),
    [requester.reference] NVARCHAR(4000),
    [requester.type] VARCHAR(256),
    [requester.identifier.id] NVARCHAR(4000),
    [requester.identifier.extension] NVARCHAR(MAX),
    [requester.identifier.use] NVARCHAR(64),
    [requester.identifier.type] NVARCHAR(MAX),
    [requester.identifier.system] VARCHAR(256),
    [requester.identifier.value] NVARCHAR(4000),
    [requester.identifier.period] NVARCHAR(MAX),
    [requester.identifier.assigner] NVARCHAR(MAX),
    [requester.display] NVARCHAR(4000),
    [supplier] VARCHAR(MAX),
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [deliverFrom.id] NVARCHAR(4000),
    [deliverFrom.extension] NVARCHAR(MAX),
    [deliverFrom.reference] NVARCHAR(4000),
    [deliverFrom.type] VARCHAR(256),
    [deliverFrom.identifier.id] NVARCHAR(4000),
    [deliverFrom.identifier.extension] NVARCHAR(MAX),
    [deliverFrom.identifier.use] NVARCHAR(64),
    [deliverFrom.identifier.type] NVARCHAR(MAX),
    [deliverFrom.identifier.system] VARCHAR(256),
    [deliverFrom.identifier.value] NVARCHAR(4000),
    [deliverFrom.identifier.period] NVARCHAR(MAX),
    [deliverFrom.identifier.assigner] NVARCHAR(MAX),
    [deliverFrom.display] NVARCHAR(4000),
    [deliverTo.id] NVARCHAR(4000),
    [deliverTo.extension] NVARCHAR(MAX),
    [deliverTo.reference] NVARCHAR(4000),
    [deliverTo.type] VARCHAR(256),
    [deliverTo.identifier.id] NVARCHAR(4000),
    [deliverTo.identifier.extension] NVARCHAR(MAX),
    [deliverTo.identifier.use] NVARCHAR(64),
    [deliverTo.identifier.type] NVARCHAR(MAX),
    [deliverTo.identifier.system] VARCHAR(256),
    [deliverTo.identifier.value] NVARCHAR(4000),
    [deliverTo.identifier.period] NVARCHAR(MAX),
    [deliverTo.identifier.assigner] NVARCHAR(MAX),
    [deliverTo.display] NVARCHAR(4000),
    [item.CodeableConcept.id] NVARCHAR(4000),
    [item.CodeableConcept.extension] NVARCHAR(MAX),
    [item.CodeableConcept.coding] VARCHAR(MAX),
    [item.CodeableConcept.text] NVARCHAR(4000),
    [item.Reference.id] NVARCHAR(4000),
    [item.Reference.extension] NVARCHAR(MAX),
    [item.Reference.reference] NVARCHAR(4000),
    [item.Reference.type] VARCHAR(256),
    [item.Reference.identifier.id] NVARCHAR(4000),
    [item.Reference.identifier.extension] NVARCHAR(MAX),
    [item.Reference.identifier.use] NVARCHAR(64),
    [item.Reference.identifier.type] NVARCHAR(MAX),
    [item.Reference.identifier.system] VARCHAR(256),
    [item.Reference.identifier.value] NVARCHAR(4000),
    [item.Reference.identifier.period] NVARCHAR(MAX),
    [item.Reference.identifier.assigner] NVARCHAR(MAX),
    [item.Reference.display] NVARCHAR(4000),
    [occurrence.dateTime] VARCHAR(30),
    [occurrence.Period.id] NVARCHAR(4000),
    [occurrence.Period.extension] NVARCHAR(MAX),
    [occurrence.Period.start] VARCHAR(30),
    [occurrence.Period.end] VARCHAR(30),
    [occurrence.Timing.id] NVARCHAR(4000),
    [occurrence.Timing.extension] NVARCHAR(MAX),
    [occurrence.Timing.modifierExtension] NVARCHAR(MAX),
    [occurrence.Timing.event] VARCHAR(MAX),
    [occurrence.Timing.repeat.id] NVARCHAR(4000),
    [occurrence.Timing.repeat.extension] NVARCHAR(MAX),
    [occurrence.Timing.repeat.modifierExtension] NVARCHAR(MAX),
    [occurrence.Timing.repeat.count] bigint,
    [occurrence.Timing.repeat.countMax] bigint,
    [occurrence.Timing.repeat.duration] float,
    [occurrence.Timing.repeat.durationMax] float,
    [occurrence.Timing.repeat.durationUnit] NVARCHAR(64),
    [occurrence.Timing.repeat.frequency] bigint,
    [occurrence.Timing.repeat.frequencyMax] bigint,
    [occurrence.Timing.repeat.period] float,
    [occurrence.Timing.repeat.periodMax] float,
    [occurrence.Timing.repeat.periodUnit] NVARCHAR(64),
    [occurrence.Timing.repeat.dayOfWeek] NVARCHAR(MAX),
    [occurrence.Timing.repeat.timeOfDay] NVARCHAR(MAX),
    [occurrence.Timing.repeat.when] NVARCHAR(MAX),
    [occurrence.Timing.repeat.offset] bigint,
    [occurrence.Timing.repeat.bounds.Duration] NVARCHAR(MAX),
    [occurrence.Timing.repeat.bounds.Range] NVARCHAR(MAX),
    [occurrence.Timing.repeat.bounds.Period] NVARCHAR(MAX),
    [occurrence.Timing.code.id] NVARCHAR(4000),
    [occurrence.Timing.code.extension] NVARCHAR(MAX),
    [occurrence.Timing.code.coding] NVARCHAR(MAX),
    [occurrence.Timing.code.text] NVARCHAR(4000),
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
    [parameter.value.CodeableConcept.id],
    [parameter.value.CodeableConcept.extension],
    [parameter.value.CodeableConcept.coding],
    [parameter.value.CodeableConcept.text],
    [parameter.value.Quantity.id],
    [parameter.value.Quantity.extension],
    [parameter.value.Quantity.value],
    [parameter.value.Quantity.comparator],
    [parameter.value.Quantity.unit],
    [parameter.value.Quantity.system],
    [parameter.value.Quantity.code],
    [parameter.value.Range.id],
    [parameter.value.Range.extension],
    [parameter.value.Range.low],
    [parameter.value.Range.high],
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
        [parameter.id]                 NVARCHAR(4000)      '$.id',
        [parameter.extension]          NVARCHAR(MAX)       '$.extension',
        [parameter.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [parameter.code.id]            NVARCHAR(4000)      '$.code.id',
        [parameter.code.extension]     NVARCHAR(MAX)       '$.code.extension',
        [parameter.code.coding]        NVARCHAR(MAX)       '$.code.coding',
        [parameter.code.text]          NVARCHAR(4000)      '$.code.text',
        [parameter.value.CodeableConcept.id] NVARCHAR(4000)      '$.value.CodeableConcept.id',
        [parameter.value.CodeableConcept.extension] NVARCHAR(MAX)       '$.value.CodeableConcept.extension',
        [parameter.value.CodeableConcept.coding] NVARCHAR(MAX)       '$.value.CodeableConcept.coding',
        [parameter.value.CodeableConcept.text] NVARCHAR(4000)      '$.value.CodeableConcept.text',
        [parameter.value.Quantity.id]  NVARCHAR(4000)      '$.value.Quantity.id',
        [parameter.value.Quantity.extension] NVARCHAR(MAX)       '$.value.Quantity.extension',
        [parameter.value.Quantity.value] float               '$.value.Quantity.value',
        [parameter.value.Quantity.comparator] NVARCHAR(64)        '$.value.Quantity.comparator',
        [parameter.value.Quantity.unit] NVARCHAR(4000)      '$.value.Quantity.unit',
        [parameter.value.Quantity.system] VARCHAR(256)        '$.value.Quantity.system',
        [parameter.value.Quantity.code] NVARCHAR(4000)      '$.value.Quantity.code',
        [parameter.value.Range.id]     NVARCHAR(4000)      '$.value.Range.id',
        [parameter.value.Range.extension] NVARCHAR(MAX)       '$.value.Range.extension',
        [parameter.value.Range.low]    NVARCHAR(MAX)       '$.value.Range.low',
        [parameter.value.Range.high]   NVARCHAR(MAX)       '$.value.Range.high',
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
        [supplier.id]                  NVARCHAR(4000)      '$.id',
        [supplier.extension]           NVARCHAR(MAX)       '$.extension',
        [supplier.reference]           NVARCHAR(4000)      '$.reference',
        [supplier.type]                VARCHAR(256)        '$.type',
        [supplier.identifier.id]       NVARCHAR(4000)      '$.identifier.id',
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

CREATE VIEW fhir.SupplyRequestReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'SupplyRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonCode.JSON]  VARCHAR(MAX) '$.reasonCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonCode.JSON]) with (
        [reasonCode.id]                NVARCHAR(4000)      '$.id',
        [reasonCode.extension]         NVARCHAR(MAX)       '$.extension',
        [reasonCode.coding]            NVARCHAR(MAX)       '$.coding' AS JSON,
        [reasonCode.text]              NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.SupplyRequestReasonReference AS
SELECT
    [id],
    [reasonReference.JSON],
    [reasonReference.id],
    [reasonReference.extension],
    [reasonReference.reference],
    [reasonReference.type],
    [reasonReference.identifier.id],
    [reasonReference.identifier.extension],
    [reasonReference.identifier.use],
    [reasonReference.identifier.type],
    [reasonReference.identifier.system],
    [reasonReference.identifier.value],
    [reasonReference.identifier.period],
    [reasonReference.identifier.assigner],
    [reasonReference.display]
FROM openrowset (
        BULK 'SupplyRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonReference.JSON]  VARCHAR(MAX) '$.reasonReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonReference.JSON]) with (
        [reasonReference.id]           NVARCHAR(4000)      '$.id',
        [reasonReference.extension]    NVARCHAR(MAX)       '$.extension',
        [reasonReference.reference]    NVARCHAR(4000)      '$.reference',
        [reasonReference.type]         VARCHAR(256)        '$.type',
        [reasonReference.identifier.id] NVARCHAR(4000)      '$.identifier.id',
        [reasonReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [reasonReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [reasonReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [reasonReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [reasonReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [reasonReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [reasonReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [reasonReference.display]      NVARCHAR(4000)      '$.display'
    ) j
