CREATE EXTERNAL TABLE [fhir].[ServiceRequest] (
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
    [instantiatesCanonical] VARCHAR(MAX),
    [instantiatesUri] VARCHAR(MAX),
    [basedOn] VARCHAR(MAX),
    [replaces] VARCHAR(MAX),
    [requisition.id] NVARCHAR(100),
    [requisition.extension] NVARCHAR(MAX),
    [requisition.use] NVARCHAR(64),
    [requisition.type.id] NVARCHAR(100),
    [requisition.type.extension] NVARCHAR(MAX),
    [requisition.type.coding] NVARCHAR(MAX),
    [requisition.type.text] NVARCHAR(4000),
    [requisition.system] VARCHAR(256),
    [requisition.value] NVARCHAR(4000),
    [requisition.period.id] NVARCHAR(100),
    [requisition.period.extension] NVARCHAR(MAX),
    [requisition.period.start] VARCHAR(64),
    [requisition.period.end] VARCHAR(64),
    [requisition.assigner.id] NVARCHAR(100),
    [requisition.assigner.extension] NVARCHAR(MAX),
    [requisition.assigner.reference] NVARCHAR(4000),
    [requisition.assigner.type] VARCHAR(256),
    [requisition.assigner.identifier] NVARCHAR(MAX),
    [requisition.assigner.display] NVARCHAR(4000),
    [status] NVARCHAR(100),
    [intent] NVARCHAR(100),
    [category] VARCHAR(MAX),
    [priority] NVARCHAR(100),
    [doNotPerform] bit,
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [orderDetail] VARCHAR(MAX),
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
    [encounter.id] NVARCHAR(100),
    [encounter.extension] NVARCHAR(MAX),
    [encounter.reference] NVARCHAR(4000),
    [encounter.type] VARCHAR(256),
    [encounter.identifier.id] NVARCHAR(100),
    [encounter.identifier.extension] NVARCHAR(MAX),
    [encounter.identifier.use] NVARCHAR(64),
    [encounter.identifier.type] NVARCHAR(MAX),
    [encounter.identifier.system] VARCHAR(256),
    [encounter.identifier.value] NVARCHAR(4000),
    [encounter.identifier.period] NVARCHAR(MAX),
    [encounter.identifier.assigner] NVARCHAR(MAX),
    [encounter.display] NVARCHAR(4000),
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
    [performerType.id] NVARCHAR(100),
    [performerType.extension] NVARCHAR(MAX),
    [performerType.coding] VARCHAR(MAX),
    [performerType.text] NVARCHAR(4000),
    [performer] VARCHAR(MAX),
    [locationCode] VARCHAR(MAX),
    [locationReference] VARCHAR(MAX),
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [insurance] VARCHAR(MAX),
    [supportingInfo] VARCHAR(MAX),
    [specimen] VARCHAR(MAX),
    [bodySite] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [patientInstruction] NVARCHAR(4000),
    [relevantHistory] VARCHAR(MAX),
    [quantity.quantity.id] NVARCHAR(100),
    [quantity.quantity.extension] NVARCHAR(MAX),
    [quantity.quantity.value] float,
    [quantity.quantity.comparator] NVARCHAR(64),
    [quantity.quantity.unit] NVARCHAR(100),
    [quantity.quantity.system] VARCHAR(256),
    [quantity.quantity.code] NVARCHAR(4000),
    [quantity.ratio.id] NVARCHAR(100),
    [quantity.ratio.extension] NVARCHAR(MAX),
    [quantity.ratio.numerator.id] NVARCHAR(100),
    [quantity.ratio.numerator.extension] NVARCHAR(MAX),
    [quantity.ratio.numerator.value] float,
    [quantity.ratio.numerator.comparator] NVARCHAR(64),
    [quantity.ratio.numerator.unit] NVARCHAR(100),
    [quantity.ratio.numerator.system] VARCHAR(256),
    [quantity.ratio.numerator.code] NVARCHAR(4000),
    [quantity.ratio.denominator.id] NVARCHAR(100),
    [quantity.ratio.denominator.extension] NVARCHAR(MAX),
    [quantity.ratio.denominator.value] float,
    [quantity.ratio.denominator.comparator] NVARCHAR(64),
    [quantity.ratio.denominator.unit] NVARCHAR(100),
    [quantity.ratio.denominator.system] VARCHAR(256),
    [quantity.ratio.denominator.code] NVARCHAR(4000),
    [quantity.range.id] NVARCHAR(100),
    [quantity.range.extension] NVARCHAR(MAX),
    [quantity.range.low.id] NVARCHAR(100),
    [quantity.range.low.extension] NVARCHAR(MAX),
    [quantity.range.low.value] float,
    [quantity.range.low.comparator] NVARCHAR(64),
    [quantity.range.low.unit] NVARCHAR(100),
    [quantity.range.low.system] VARCHAR(256),
    [quantity.range.low.code] NVARCHAR(4000),
    [quantity.range.high.id] NVARCHAR(100),
    [quantity.range.high.extension] NVARCHAR(MAX),
    [quantity.range.high.value] float,
    [quantity.range.high.comparator] NVARCHAR(64),
    [quantity.range.high.unit] NVARCHAR(100),
    [quantity.range.high.system] VARCHAR(256),
    [quantity.range.high.code] NVARCHAR(4000),
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
    [asNeeded.boolean] bit,
    [asNeeded.codeableConcept.id] NVARCHAR(100),
    [asNeeded.codeableConcept.extension] NVARCHAR(MAX),
    [asNeeded.codeableConcept.coding] VARCHAR(MAX),
    [asNeeded.codeableConcept.text] NVARCHAR(4000),
) WITH (
    LOCATION='/ServiceRequest/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ServiceRequestIdentifier AS
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
        BULK 'ServiceRequest/**',
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

CREATE VIEW fhir.ServiceRequestInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesCanonical.JSON]  VARCHAR(MAX) '$.instantiatesCanonical'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesCanonical.JSON]) with (
        [instantiatesCanonical]        NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ServiceRequestInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesUri.JSON]  VARCHAR(MAX) '$.instantiatesUri'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesUri.JSON]) with (
        [instantiatesUri]              NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ServiceRequestBasedOn AS
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
        BULK 'ServiceRequest/**',
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

CREATE VIEW fhir.ServiceRequestReplaces AS
SELECT
    [id],
    [replaces.JSON],
    [replaces.id],
    [replaces.extension],
    [replaces.reference],
    [replaces.type],
    [replaces.identifier.id],
    [replaces.identifier.extension],
    [replaces.identifier.use],
    [replaces.identifier.type],
    [replaces.identifier.system],
    [replaces.identifier.value],
    [replaces.identifier.period],
    [replaces.identifier.assigner],
    [replaces.display]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [replaces.JSON]  VARCHAR(MAX) '$.replaces'
    ) AS rowset
    CROSS APPLY openjson (rowset.[replaces.JSON]) with (
        [replaces.id]                  NVARCHAR(100)       '$.id',
        [replaces.extension]           NVARCHAR(MAX)       '$.extension',
        [replaces.reference]           NVARCHAR(4000)      '$.reference',
        [replaces.type]                VARCHAR(256)        '$.type',
        [replaces.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [replaces.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [replaces.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [replaces.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [replaces.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [replaces.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [replaces.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [replaces.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [replaces.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ServiceRequestCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [category.JSON]  VARCHAR(MAX) '$.category'
    ) AS rowset
    CROSS APPLY openjson (rowset.[category.JSON]) with (
        [category.id]                  NVARCHAR(100)       '$.id',
        [category.extension]           NVARCHAR(MAX)       '$.extension',
        [category.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [category.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ServiceRequestOrderDetail AS
SELECT
    [id],
    [orderDetail.JSON],
    [orderDetail.id],
    [orderDetail.extension],
    [orderDetail.coding],
    [orderDetail.text]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [orderDetail.JSON]  VARCHAR(MAX) '$.orderDetail'
    ) AS rowset
    CROSS APPLY openjson (rowset.[orderDetail.JSON]) with (
        [orderDetail.id]               NVARCHAR(100)       '$.id',
        [orderDetail.extension]        NVARCHAR(MAX)       '$.extension',
        [orderDetail.coding]           NVARCHAR(MAX)       '$.coding' AS JSON,
        [orderDetail.text]             NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ServiceRequestPerformer AS
SELECT
    [id],
    [performer.JSON],
    [performer.id],
    [performer.extension],
    [performer.reference],
    [performer.type],
    [performer.identifier.id],
    [performer.identifier.extension],
    [performer.identifier.use],
    [performer.identifier.type],
    [performer.identifier.system],
    [performer.identifier.value],
    [performer.identifier.period],
    [performer.identifier.assigner],
    [performer.display]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [performer.JSON]  VARCHAR(MAX) '$.performer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[performer.JSON]) with (
        [performer.id]                 NVARCHAR(100)       '$.id',
        [performer.extension]          NVARCHAR(MAX)       '$.extension',
        [performer.reference]          NVARCHAR(4000)      '$.reference',
        [performer.type]               VARCHAR(256)        '$.type',
        [performer.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [performer.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [performer.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [performer.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [performer.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [performer.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [performer.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [performer.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [performer.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ServiceRequestLocationCode AS
SELECT
    [id],
    [locationCode.JSON],
    [locationCode.id],
    [locationCode.extension],
    [locationCode.coding],
    [locationCode.text]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [locationCode.JSON]  VARCHAR(MAX) '$.locationCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[locationCode.JSON]) with (
        [locationCode.id]              NVARCHAR(100)       '$.id',
        [locationCode.extension]       NVARCHAR(MAX)       '$.extension',
        [locationCode.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [locationCode.text]            NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ServiceRequestLocationReference AS
SELECT
    [id],
    [locationReference.JSON],
    [locationReference.id],
    [locationReference.extension],
    [locationReference.reference],
    [locationReference.type],
    [locationReference.identifier.id],
    [locationReference.identifier.extension],
    [locationReference.identifier.use],
    [locationReference.identifier.type],
    [locationReference.identifier.system],
    [locationReference.identifier.value],
    [locationReference.identifier.period],
    [locationReference.identifier.assigner],
    [locationReference.display]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [locationReference.JSON]  VARCHAR(MAX) '$.locationReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[locationReference.JSON]) with (
        [locationReference.id]         NVARCHAR(100)       '$.id',
        [locationReference.extension]  NVARCHAR(MAX)       '$.extension',
        [locationReference.reference]  NVARCHAR(4000)      '$.reference',
        [locationReference.type]       VARCHAR(256)        '$.type',
        [locationReference.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [locationReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [locationReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [locationReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [locationReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [locationReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [locationReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [locationReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [locationReference.display]    NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ServiceRequestReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonCode.JSON]  VARCHAR(MAX) '$.reasonCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonCode.JSON]) with (
        [reasonCode.id]                NVARCHAR(100)       '$.id',
        [reasonCode.extension]         NVARCHAR(MAX)       '$.extension',
        [reasonCode.coding]            NVARCHAR(MAX)       '$.coding' AS JSON,
        [reasonCode.text]              NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ServiceRequestReasonReference AS
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
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonReference.JSON]  VARCHAR(MAX) '$.reasonReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonReference.JSON]) with (
        [reasonReference.id]           NVARCHAR(100)       '$.id',
        [reasonReference.extension]    NVARCHAR(MAX)       '$.extension',
        [reasonReference.reference]    NVARCHAR(4000)      '$.reference',
        [reasonReference.type]         VARCHAR(256)        '$.type',
        [reasonReference.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [reasonReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [reasonReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [reasonReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [reasonReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [reasonReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [reasonReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [reasonReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [reasonReference.display]      NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ServiceRequestInsurance AS
SELECT
    [id],
    [insurance.JSON],
    [insurance.id],
    [insurance.extension],
    [insurance.reference],
    [insurance.type],
    [insurance.identifier.id],
    [insurance.identifier.extension],
    [insurance.identifier.use],
    [insurance.identifier.type],
    [insurance.identifier.system],
    [insurance.identifier.value],
    [insurance.identifier.period],
    [insurance.identifier.assigner],
    [insurance.display]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [insurance.JSON]  VARCHAR(MAX) '$.insurance'
    ) AS rowset
    CROSS APPLY openjson (rowset.[insurance.JSON]) with (
        [insurance.id]                 NVARCHAR(100)       '$.id',
        [insurance.extension]          NVARCHAR(MAX)       '$.extension',
        [insurance.reference]          NVARCHAR(4000)      '$.reference',
        [insurance.type]               VARCHAR(256)        '$.type',
        [insurance.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [insurance.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [insurance.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [insurance.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [insurance.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [insurance.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [insurance.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [insurance.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [insurance.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ServiceRequestSupportingInfo AS
SELECT
    [id],
    [supportingInfo.JSON],
    [supportingInfo.id],
    [supportingInfo.extension],
    [supportingInfo.reference],
    [supportingInfo.type],
    [supportingInfo.identifier.id],
    [supportingInfo.identifier.extension],
    [supportingInfo.identifier.use],
    [supportingInfo.identifier.type],
    [supportingInfo.identifier.system],
    [supportingInfo.identifier.value],
    [supportingInfo.identifier.period],
    [supportingInfo.identifier.assigner],
    [supportingInfo.display]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [supportingInfo.JSON]  VARCHAR(MAX) '$.supportingInfo'
    ) AS rowset
    CROSS APPLY openjson (rowset.[supportingInfo.JSON]) with (
        [supportingInfo.id]            NVARCHAR(100)       '$.id',
        [supportingInfo.extension]     NVARCHAR(MAX)       '$.extension',
        [supportingInfo.reference]     NVARCHAR(4000)      '$.reference',
        [supportingInfo.type]          VARCHAR(256)        '$.type',
        [supportingInfo.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [supportingInfo.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [supportingInfo.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [supportingInfo.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [supportingInfo.identifier.system] VARCHAR(256)        '$.identifier.system',
        [supportingInfo.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [supportingInfo.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [supportingInfo.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [supportingInfo.display]       NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ServiceRequestSpecimen AS
SELECT
    [id],
    [specimen.JSON],
    [specimen.id],
    [specimen.extension],
    [specimen.reference],
    [specimen.type],
    [specimen.identifier.id],
    [specimen.identifier.extension],
    [specimen.identifier.use],
    [specimen.identifier.type],
    [specimen.identifier.system],
    [specimen.identifier.value],
    [specimen.identifier.period],
    [specimen.identifier.assigner],
    [specimen.display]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specimen.JSON]  VARCHAR(MAX) '$.specimen'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specimen.JSON]) with (
        [specimen.id]                  NVARCHAR(100)       '$.id',
        [specimen.extension]           NVARCHAR(MAX)       '$.extension',
        [specimen.reference]           NVARCHAR(4000)      '$.reference',
        [specimen.type]                VARCHAR(256)        '$.type',
        [specimen.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [specimen.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [specimen.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [specimen.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [specimen.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [specimen.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [specimen.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [specimen.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [specimen.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ServiceRequestBodySite AS
SELECT
    [id],
    [bodySite.JSON],
    [bodySite.id],
    [bodySite.extension],
    [bodySite.coding],
    [bodySite.text]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [bodySite.JSON]  VARCHAR(MAX) '$.bodySite'
    ) AS rowset
    CROSS APPLY openjson (rowset.[bodySite.JSON]) with (
        [bodySite.id]                  NVARCHAR(100)       '$.id',
        [bodySite.extension]           NVARCHAR(MAX)       '$.extension',
        [bodySite.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [bodySite.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ServiceRequestNote AS
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
        BULK 'ServiceRequest/**',
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

CREATE VIEW fhir.ServiceRequestRelevantHistory AS
SELECT
    [id],
    [relevantHistory.JSON],
    [relevantHistory.id],
    [relevantHistory.extension],
    [relevantHistory.reference],
    [relevantHistory.type],
    [relevantHistory.identifier.id],
    [relevantHistory.identifier.extension],
    [relevantHistory.identifier.use],
    [relevantHistory.identifier.type],
    [relevantHistory.identifier.system],
    [relevantHistory.identifier.value],
    [relevantHistory.identifier.period],
    [relevantHistory.identifier.assigner],
    [relevantHistory.display]
FROM openrowset (
        BULK 'ServiceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [relevantHistory.JSON]  VARCHAR(MAX) '$.relevantHistory'
    ) AS rowset
    CROSS APPLY openjson (rowset.[relevantHistory.JSON]) with (
        [relevantHistory.id]           NVARCHAR(100)       '$.id',
        [relevantHistory.extension]    NVARCHAR(MAX)       '$.extension',
        [relevantHistory.reference]    NVARCHAR(4000)      '$.reference',
        [relevantHistory.type]         VARCHAR(256)        '$.type',
        [relevantHistory.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [relevantHistory.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [relevantHistory.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [relevantHistory.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [relevantHistory.identifier.system] VARCHAR(256)        '$.identifier.system',
        [relevantHistory.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [relevantHistory.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [relevantHistory.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [relevantHistory.display]      NVARCHAR(4000)      '$.display'
    ) j
