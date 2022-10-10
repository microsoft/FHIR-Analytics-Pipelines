CREATE EXTERNAL TABLE [fhir].[BiologicallyDerivedProduct] (
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
    [productCategory] NVARCHAR(64),
    [productCode.id] NVARCHAR(100),
    [productCode.extension] NVARCHAR(MAX),
    [productCode.coding] VARCHAR(MAX),
    [productCode.text] NVARCHAR(4000),
    [status] NVARCHAR(64),
    [request] VARCHAR(MAX),
    [quantity] bigint,
    [parent] VARCHAR(MAX),
    [collection.id] NVARCHAR(100),
    [collection.extension] NVARCHAR(MAX),
    [collection.modifierExtension] NVARCHAR(MAX),
    [collection.collector.id] NVARCHAR(100),
    [collection.collector.extension] NVARCHAR(MAX),
    [collection.collector.reference] NVARCHAR(4000),
    [collection.collector.type] VARCHAR(256),
    [collection.collector.identifier] NVARCHAR(MAX),
    [collection.collector.display] NVARCHAR(4000),
    [collection.source.id] NVARCHAR(100),
    [collection.source.extension] NVARCHAR(MAX),
    [collection.source.reference] NVARCHAR(4000),
    [collection.source.type] VARCHAR(256),
    [collection.source.identifier] NVARCHAR(MAX),
    [collection.source.display] NVARCHAR(4000),
    [collection.collected.dateTime] VARCHAR(64),
    [collection.collected.period.id] NVARCHAR(100),
    [collection.collected.period.extension] NVARCHAR(MAX),
    [collection.collected.period.start] VARCHAR(64),
    [collection.collected.period.end] VARCHAR(64),
    [processing] VARCHAR(MAX),
    [manipulation.id] NVARCHAR(100),
    [manipulation.extension] NVARCHAR(MAX),
    [manipulation.modifierExtension] NVARCHAR(MAX),
    [manipulation.description] NVARCHAR(4000),
    [manipulation.time.dateTime] VARCHAR(64),
    [manipulation.time.period.id] NVARCHAR(100),
    [manipulation.time.period.extension] NVARCHAR(MAX),
    [manipulation.time.period.start] VARCHAR(64),
    [manipulation.time.period.end] VARCHAR(64),
    [storage] VARCHAR(MAX),
) WITH (
    LOCATION='/BiologicallyDerivedProduct/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.BiologicallyDerivedProductIdentifier AS
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
        BULK 'BiologicallyDerivedProduct/**',
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

CREATE VIEW fhir.BiologicallyDerivedProductRequest AS
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
        BULK 'BiologicallyDerivedProduct/**',
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

CREATE VIEW fhir.BiologicallyDerivedProductParent AS
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
        BULK 'BiologicallyDerivedProduct/**',
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

CREATE VIEW fhir.BiologicallyDerivedProductProcessing AS
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
    [processing.additive.id],
    [processing.additive.extension],
    [processing.additive.reference],
    [processing.additive.type],
    [processing.additive.identifier],
    [processing.additive.display],
    [processing.time.dateTime],
    [processing.time.period.id],
    [processing.time.period.extension],
    [processing.time.period.start],
    [processing.time.period.end]
FROM openrowset (
        BULK 'BiologicallyDerivedProduct/**',
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
        [processing.additive.id]       NVARCHAR(100)       '$.additive.id',
        [processing.additive.extension] NVARCHAR(MAX)       '$.additive.extension',
        [processing.additive.reference] NVARCHAR(4000)      '$.additive.reference',
        [processing.additive.type]     VARCHAR(256)        '$.additive.type',
        [processing.additive.identifier] NVARCHAR(MAX)       '$.additive.identifier',
        [processing.additive.display]  NVARCHAR(4000)      '$.additive.display',
        [processing.time.dateTime]     VARCHAR(64)         '$.time.dateTime',
        [processing.time.period.id]    NVARCHAR(100)       '$.time.period.id',
        [processing.time.period.extension] NVARCHAR(MAX)       '$.time.period.extension',
        [processing.time.period.start] VARCHAR(64)         '$.time.period.start',
        [processing.time.period.end]   VARCHAR(64)         '$.time.period.end'
    ) j

GO

CREATE VIEW fhir.BiologicallyDerivedProductStorage AS
SELECT
    [id],
    [storage.JSON],
    [storage.id],
    [storage.extension],
    [storage.modifierExtension],
    [storage.description],
    [storage.temperature],
    [storage.scale],
    [storage.duration.id],
    [storage.duration.extension],
    [storage.duration.start],
    [storage.duration.end]
FROM openrowset (
        BULK 'BiologicallyDerivedProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [storage.JSON]  VARCHAR(MAX) '$.storage'
    ) AS rowset
    CROSS APPLY openjson (rowset.[storage.JSON]) with (
        [storage.id]                   NVARCHAR(100)       '$.id',
        [storage.extension]            NVARCHAR(MAX)       '$.extension',
        [storage.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [storage.description]          NVARCHAR(4000)      '$.description',
        [storage.temperature]          float               '$.temperature',
        [storage.scale]                NVARCHAR(64)        '$.scale',
        [storage.duration.id]          NVARCHAR(100)       '$.duration.id',
        [storage.duration.extension]   NVARCHAR(MAX)       '$.duration.extension',
        [storage.duration.start]       VARCHAR(64)         '$.duration.start',
        [storage.duration.end]         VARCHAR(64)         '$.duration.end'
    ) j
