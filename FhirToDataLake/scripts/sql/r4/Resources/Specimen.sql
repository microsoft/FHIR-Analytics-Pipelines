CREATE EXTERNAL TABLE [fhir].[Specimen] (
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
    [accessionIdentifier.id] NVARCHAR(100),
    [accessionIdentifier.extension] NVARCHAR(MAX),
    [accessionIdentifier.use] NVARCHAR(64),
    [accessionIdentifier.type.id] NVARCHAR(100),
    [accessionIdentifier.type.extension] NVARCHAR(MAX),
    [accessionIdentifier.type.coding] NVARCHAR(MAX),
    [accessionIdentifier.type.text] NVARCHAR(4000),
    [accessionIdentifier.system] VARCHAR(256),
    [accessionIdentifier.value] NVARCHAR(4000),
    [accessionIdentifier.period.id] NVARCHAR(100),
    [accessionIdentifier.period.extension] NVARCHAR(MAX),
    [accessionIdentifier.period.start] VARCHAR(64),
    [accessionIdentifier.period.end] VARCHAR(64),
    [accessionIdentifier.assigner.id] NVARCHAR(100),
    [accessionIdentifier.assigner.extension] NVARCHAR(MAX),
    [accessionIdentifier.assigner.reference] NVARCHAR(4000),
    [accessionIdentifier.assigner.type] VARCHAR(256),
    [accessionIdentifier.assigner.identifier] NVARCHAR(MAX),
    [accessionIdentifier.assigner.display] NVARCHAR(4000),
    [status] NVARCHAR(64),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
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
    [receivedTime] VARCHAR(64),
    [parent] VARCHAR(MAX),
    [request] VARCHAR(MAX),
    [collection.id] NVARCHAR(100),
    [collection.extension] NVARCHAR(MAX),
    [collection.modifierExtension] NVARCHAR(MAX),
    [collection.collector.id] NVARCHAR(100),
    [collection.collector.extension] NVARCHAR(MAX),
    [collection.collector.reference] NVARCHAR(4000),
    [collection.collector.type] VARCHAR(256),
    [collection.collector.identifier] NVARCHAR(MAX),
    [collection.collector.display] NVARCHAR(4000),
    [collection.duration.id] NVARCHAR(100),
    [collection.duration.extension] NVARCHAR(MAX),
    [collection.duration.value] float,
    [collection.duration.comparator] NVARCHAR(64),
    [collection.duration.unit] NVARCHAR(100),
    [collection.duration.system] VARCHAR(256),
    [collection.duration.code] NVARCHAR(4000),
    [collection.quantity.id] NVARCHAR(100),
    [collection.quantity.extension] NVARCHAR(MAX),
    [collection.quantity.value] float,
    [collection.quantity.comparator] NVARCHAR(64),
    [collection.quantity.unit] NVARCHAR(100),
    [collection.quantity.system] VARCHAR(256),
    [collection.quantity.code] NVARCHAR(4000),
    [collection.method.id] NVARCHAR(100),
    [collection.method.extension] NVARCHAR(MAX),
    [collection.method.coding] NVARCHAR(MAX),
    [collection.method.text] NVARCHAR(4000),
    [collection.bodySite.id] NVARCHAR(100),
    [collection.bodySite.extension] NVARCHAR(MAX),
    [collection.bodySite.coding] NVARCHAR(MAX),
    [collection.bodySite.text] NVARCHAR(4000),
    [collection.collected.dateTime] VARCHAR(64),
    [collection.collected.period.id] NVARCHAR(100),
    [collection.collected.period.extension] NVARCHAR(MAX),
    [collection.collected.period.start] VARCHAR(64),
    [collection.collected.period.end] VARCHAR(64),
    [collection.fastingStatus.codeableConcept.id] NVARCHAR(100),
    [collection.fastingStatus.codeableConcept.extension] NVARCHAR(MAX),
    [collection.fastingStatus.codeableConcept.coding] NVARCHAR(MAX),
    [collection.fastingStatus.codeableConcept.text] NVARCHAR(4000),
    [collection.fastingStatus.duration.id] NVARCHAR(100),
    [collection.fastingStatus.duration.extension] NVARCHAR(MAX),
    [collection.fastingStatus.duration.value] float,
    [collection.fastingStatus.duration.comparator] NVARCHAR(64),
    [collection.fastingStatus.duration.unit] NVARCHAR(100),
    [collection.fastingStatus.duration.system] VARCHAR(256),
    [collection.fastingStatus.duration.code] NVARCHAR(4000),
    [processing] VARCHAR(MAX),
    [container] VARCHAR(MAX),
    [condition] VARCHAR(MAX),
    [note] VARCHAR(MAX),
) WITH (
    LOCATION='/Specimen/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SpecimenIdentifier AS
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
        BULK 'Specimen/**',
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

CREATE VIEW fhir.SpecimenParent AS
SELECT
    [id],
    [parent.JSON],
    [parent.id],
    [parent.extension],
    [parent.reference],
    [parent.type],
    [parent.identifier.id],
    [parent.identifier.extension],
    [parent.identifier.use],
    [parent.identifier.type],
    [parent.identifier.system],
    [parent.identifier.value],
    [parent.identifier.period],
    [parent.identifier.assigner],
    [parent.display]
FROM openrowset (
        BULK 'Specimen/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [parent.JSON]  VARCHAR(MAX) '$.parent'
    ) AS rowset
    CROSS APPLY openjson (rowset.[parent.JSON]) with (
        [parent.id]                    NVARCHAR(100)       '$.id',
        [parent.extension]             NVARCHAR(MAX)       '$.extension',
        [parent.reference]             NVARCHAR(4000)      '$.reference',
        [parent.type]                  VARCHAR(256)        '$.type',
        [parent.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [parent.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [parent.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [parent.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [parent.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [parent.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [parent.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [parent.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [parent.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.SpecimenRequest AS
SELECT
    [id],
    [request.JSON],
    [request.id],
    [request.extension],
    [request.reference],
    [request.type],
    [request.identifier.id],
    [request.identifier.extension],
    [request.identifier.use],
    [request.identifier.type],
    [request.identifier.system],
    [request.identifier.value],
    [request.identifier.period],
    [request.identifier.assigner],
    [request.display]
FROM openrowset (
        BULK 'Specimen/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [request.JSON]  VARCHAR(MAX) '$.request'
    ) AS rowset
    CROSS APPLY openjson (rowset.[request.JSON]) with (
        [request.id]                   NVARCHAR(100)       '$.id',
        [request.extension]            NVARCHAR(MAX)       '$.extension',
        [request.reference]            NVARCHAR(4000)      '$.reference',
        [request.type]                 VARCHAR(256)        '$.type',
        [request.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [request.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [request.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [request.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [request.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [request.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [request.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [request.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [request.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.SpecimenProcessing AS
SELECT
    [id],
    [processing.JSON],
    [processing.id],
    [processing.extension],
    [processing.modifierExtension],
    [processing.description],
    [processing.procedure.id],
    [processing.procedure.extension],
    [processing.procedure.coding],
    [processing.procedure.text],
    [processing.additive],
    [processing.time.dateTime],
    [processing.time.period.id],
    [processing.time.period.extension],
    [processing.time.period.start],
    [processing.time.period.end]
FROM openrowset (
        BULK 'Specimen/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [processing.JSON]  VARCHAR(MAX) '$.processing'
    ) AS rowset
    CROSS APPLY openjson (rowset.[processing.JSON]) with (
        [processing.id]                NVARCHAR(100)       '$.id',
        [processing.extension]         NVARCHAR(MAX)       '$.extension',
        [processing.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [processing.description]       NVARCHAR(4000)      '$.description',
        [processing.procedure.id]      NVARCHAR(100)       '$.procedure.id',
        [processing.procedure.extension] NVARCHAR(MAX)       '$.procedure.extension',
        [processing.procedure.coding]  NVARCHAR(MAX)       '$.procedure.coding',
        [processing.procedure.text]    NVARCHAR(4000)      '$.procedure.text',
        [processing.additive]          NVARCHAR(MAX)       '$.additive' AS JSON,
        [processing.time.dateTime]     VARCHAR(64)         '$.time.dateTime',
        [processing.time.period.id]    NVARCHAR(100)       '$.time.period.id',
        [processing.time.period.extension] NVARCHAR(MAX)       '$.time.period.extension',
        [processing.time.period.start] VARCHAR(64)         '$.time.period.start',
        [processing.time.period.end]   VARCHAR(64)         '$.time.period.end'
    ) j

GO

CREATE VIEW fhir.SpecimenContainer AS
SELECT
    [id],
    [container.JSON],
    [container.id],
    [container.extension],
    [container.modifierExtension],
    [container.identifier],
    [container.description],
    [container.type.id],
    [container.type.extension],
    [container.type.coding],
    [container.type.text],
    [container.capacity.id],
    [container.capacity.extension],
    [container.capacity.value],
    [container.capacity.comparator],
    [container.capacity.unit],
    [container.capacity.system],
    [container.capacity.code],
    [container.specimenQuantity.id],
    [container.specimenQuantity.extension],
    [container.specimenQuantity.value],
    [container.specimenQuantity.comparator],
    [container.specimenQuantity.unit],
    [container.specimenQuantity.system],
    [container.specimenQuantity.code],
    [container.additive.codeableConcept.id],
    [container.additive.codeableConcept.extension],
    [container.additive.codeableConcept.coding],
    [container.additive.codeableConcept.text],
    [container.additive.reference.id],
    [container.additive.reference.extension],
    [container.additive.reference.reference],
    [container.additive.reference.type],
    [container.additive.reference.identifier],
    [container.additive.reference.display]
FROM openrowset (
        BULK 'Specimen/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [container.JSON]  VARCHAR(MAX) '$.container'
    ) AS rowset
    CROSS APPLY openjson (rowset.[container.JSON]) with (
        [container.id]                 NVARCHAR(100)       '$.id',
        [container.extension]          NVARCHAR(MAX)       '$.extension',
        [container.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [container.identifier]         NVARCHAR(MAX)       '$.identifier' AS JSON,
        [container.description]        NVARCHAR(4000)      '$.description',
        [container.type.id]            NVARCHAR(100)       '$.type.id',
        [container.type.extension]     NVARCHAR(MAX)       '$.type.extension',
        [container.type.coding]        NVARCHAR(MAX)       '$.type.coding',
        [container.type.text]          NVARCHAR(4000)      '$.type.text',
        [container.capacity.id]        NVARCHAR(100)       '$.capacity.id',
        [container.capacity.extension] NVARCHAR(MAX)       '$.capacity.extension',
        [container.capacity.value]     float               '$.capacity.value',
        [container.capacity.comparator] NVARCHAR(64)        '$.capacity.comparator',
        [container.capacity.unit]      NVARCHAR(100)       '$.capacity.unit',
        [container.capacity.system]    VARCHAR(256)        '$.capacity.system',
        [container.capacity.code]      NVARCHAR(4000)      '$.capacity.code',
        [container.specimenQuantity.id] NVARCHAR(100)       '$.specimenQuantity.id',
        [container.specimenQuantity.extension] NVARCHAR(MAX)       '$.specimenQuantity.extension',
        [container.specimenQuantity.value] float               '$.specimenQuantity.value',
        [container.specimenQuantity.comparator] NVARCHAR(64)        '$.specimenQuantity.comparator',
        [container.specimenQuantity.unit] NVARCHAR(100)       '$.specimenQuantity.unit',
        [container.specimenQuantity.system] VARCHAR(256)        '$.specimenQuantity.system',
        [container.specimenQuantity.code] NVARCHAR(4000)      '$.specimenQuantity.code',
        [container.additive.codeableConcept.id] NVARCHAR(100)       '$.additive.codeableConcept.id',
        [container.additive.codeableConcept.extension] NVARCHAR(MAX)       '$.additive.codeableConcept.extension',
        [container.additive.codeableConcept.coding] NVARCHAR(MAX)       '$.additive.codeableConcept.coding',
        [container.additive.codeableConcept.text] NVARCHAR(4000)      '$.additive.codeableConcept.text',
        [container.additive.reference.id] NVARCHAR(100)       '$.additive.reference.id',
        [container.additive.reference.extension] NVARCHAR(MAX)       '$.additive.reference.extension',
        [container.additive.reference.reference] NVARCHAR(4000)      '$.additive.reference.reference',
        [container.additive.reference.type] VARCHAR(256)        '$.additive.reference.type',
        [container.additive.reference.identifier] NVARCHAR(MAX)       '$.additive.reference.identifier',
        [container.additive.reference.display] NVARCHAR(4000)      '$.additive.reference.display'
    ) j

GO

CREATE VIEW fhir.SpecimenCondition AS
SELECT
    [id],
    [condition.JSON],
    [condition.id],
    [condition.extension],
    [condition.coding],
    [condition.text]
FROM openrowset (
        BULK 'Specimen/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [condition.JSON]  VARCHAR(MAX) '$.condition'
    ) AS rowset
    CROSS APPLY openjson (rowset.[condition.JSON]) with (
        [condition.id]                 NVARCHAR(100)       '$.id',
        [condition.extension]          NVARCHAR(MAX)       '$.extension',
        [condition.coding]             NVARCHAR(MAX)       '$.coding' AS JSON,
        [condition.text]               NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.SpecimenNote AS
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
        BULK 'Specimen/**',
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
