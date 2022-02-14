CREATE EXTERNAL TABLE [fhir].[DeviceRequest] (
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
    [priorRequest] VARCHAR(MAX),
    [groupIdentifier.id] NVARCHAR(100),
    [groupIdentifier.extension] NVARCHAR(MAX),
    [groupIdentifier.use] NVARCHAR(64),
    [groupIdentifier.type.id] NVARCHAR(100),
    [groupIdentifier.type.extension] NVARCHAR(MAX),
    [groupIdentifier.type.coding] NVARCHAR(MAX),
    [groupIdentifier.type.text] NVARCHAR(4000),
    [groupIdentifier.system] VARCHAR(256),
    [groupIdentifier.value] NVARCHAR(4000),
    [groupIdentifier.period.id] NVARCHAR(100),
    [groupIdentifier.period.extension] NVARCHAR(MAX),
    [groupIdentifier.period.start] VARCHAR(64),
    [groupIdentifier.period.end] VARCHAR(64),
    [groupIdentifier.assigner.id] NVARCHAR(100),
    [groupIdentifier.assigner.extension] NVARCHAR(MAX),
    [groupIdentifier.assigner.reference] NVARCHAR(4000),
    [groupIdentifier.assigner.type] VARCHAR(256),
    [groupIdentifier.assigner.identifier] NVARCHAR(MAX),
    [groupIdentifier.assigner.display] NVARCHAR(4000),
    [status] NVARCHAR(100),
    [intent] NVARCHAR(100),
    [priority] NVARCHAR(100),
    [parameter] VARCHAR(MAX),
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
    [performer.id] NVARCHAR(100),
    [performer.extension] NVARCHAR(MAX),
    [performer.reference] NVARCHAR(4000),
    [performer.type] VARCHAR(256),
    [performer.identifier.id] NVARCHAR(100),
    [performer.identifier.extension] NVARCHAR(MAX),
    [performer.identifier.use] NVARCHAR(64),
    [performer.identifier.type] NVARCHAR(MAX),
    [performer.identifier.system] VARCHAR(256),
    [performer.identifier.value] NVARCHAR(4000),
    [performer.identifier.period] NVARCHAR(MAX),
    [performer.identifier.assigner] NVARCHAR(MAX),
    [performer.display] NVARCHAR(4000),
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [insurance] VARCHAR(MAX),
    [supportingInfo] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [relevantHistory] VARCHAR(MAX),
    [code.reference.id] NVARCHAR(100),
    [code.reference.extension] NVARCHAR(MAX),
    [code.reference.reference] NVARCHAR(4000),
    [code.reference.type] VARCHAR(256),
    [code.reference.identifier.id] NVARCHAR(100),
    [code.reference.identifier.extension] NVARCHAR(MAX),
    [code.reference.identifier.use] NVARCHAR(64),
    [code.reference.identifier.type] NVARCHAR(MAX),
    [code.reference.identifier.system] VARCHAR(256),
    [code.reference.identifier.value] NVARCHAR(4000),
    [code.reference.identifier.period] NVARCHAR(MAX),
    [code.reference.identifier.assigner] NVARCHAR(MAX),
    [code.reference.display] NVARCHAR(4000),
    [code.codeableConcept.id] NVARCHAR(100),
    [code.codeableConcept.extension] NVARCHAR(MAX),
    [code.codeableConcept.coding] VARCHAR(MAX),
    [code.codeableConcept.text] NVARCHAR(4000),
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
    LOCATION='/DeviceRequest/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.DeviceRequestIdentifier AS
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
        BULK 'DeviceRequest/**',
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

CREATE VIEW fhir.DeviceRequestInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'DeviceRequest/**',
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

CREATE VIEW fhir.DeviceRequestInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'DeviceRequest/**',
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

CREATE VIEW fhir.DeviceRequestBasedOn AS
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
        BULK 'DeviceRequest/**',
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

CREATE VIEW fhir.DeviceRequestPriorRequest AS
SELECT
    [id],
    [priorRequest.JSON],
    [priorRequest.id],
    [priorRequest.extension],
    [priorRequest.reference],
    [priorRequest.type],
    [priorRequest.identifier.id],
    [priorRequest.identifier.extension],
    [priorRequest.identifier.use],
    [priorRequest.identifier.type],
    [priorRequest.identifier.system],
    [priorRequest.identifier.value],
    [priorRequest.identifier.period],
    [priorRequest.identifier.assigner],
    [priorRequest.display]
FROM openrowset (
        BULK 'DeviceRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [priorRequest.JSON]  VARCHAR(MAX) '$.priorRequest'
    ) AS rowset
    CROSS APPLY openjson (rowset.[priorRequest.JSON]) with (
        [priorRequest.id]              NVARCHAR(100)       '$.id',
        [priorRequest.extension]       NVARCHAR(MAX)       '$.extension',
        [priorRequest.reference]       NVARCHAR(4000)      '$.reference',
        [priorRequest.type]            VARCHAR(256)        '$.type',
        [priorRequest.identifier.id]   NVARCHAR(100)       '$.identifier.id',
        [priorRequest.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [priorRequest.identifier.use]  NVARCHAR(64)        '$.identifier.use',
        [priorRequest.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [priorRequest.identifier.system] VARCHAR(256)        '$.identifier.system',
        [priorRequest.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [priorRequest.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [priorRequest.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [priorRequest.display]         NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DeviceRequestParameter AS
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
        BULK 'DeviceRequest/**',
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

CREATE VIEW fhir.DeviceRequestReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'DeviceRequest/**',
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

CREATE VIEW fhir.DeviceRequestReasonReference AS
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
        BULK 'DeviceRequest/**',
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

CREATE VIEW fhir.DeviceRequestInsurance AS
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
        BULK 'DeviceRequest/**',
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

CREATE VIEW fhir.DeviceRequestSupportingInfo AS
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
        BULK 'DeviceRequest/**',
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

CREATE VIEW fhir.DeviceRequestNote AS
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
        BULK 'DeviceRequest/**',
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

CREATE VIEW fhir.DeviceRequestRelevantHistory AS
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
        BULK 'DeviceRequest/**',
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
