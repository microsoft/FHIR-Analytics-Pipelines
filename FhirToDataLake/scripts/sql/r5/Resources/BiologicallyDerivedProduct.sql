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
    [productCategory] NVARCHAR(4000),
    [productCode.id] NVARCHAR(100),
    [productCode.extension] NVARCHAR(MAX),
    [productCode.coding] VARCHAR(MAX),
    [productCode.text] NVARCHAR(4000),
    [parent] VARCHAR(MAX),
    [request] VARCHAR(MAX),
    [identifier] VARCHAR(MAX),
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
    [processingFacility] VARCHAR(MAX),
    [division] NVARCHAR(4000),
    [status] NVARCHAR(100),
    [expirationDate] VARCHAR(64),
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
    [storageTempRequirements.id] NVARCHAR(100),
    [storageTempRequirements.extension] NVARCHAR(MAX),
    [storageTempRequirements.low.id] NVARCHAR(100),
    [storageTempRequirements.low.extension] NVARCHAR(MAX),
    [storageTempRequirements.low.value] float,
    [storageTempRequirements.low.comparator] NVARCHAR(64),
    [storageTempRequirements.low.unit] NVARCHAR(100),
    [storageTempRequirements.low.system] VARCHAR(256),
    [storageTempRequirements.low.code] NVARCHAR(4000),
    [storageTempRequirements.high.id] NVARCHAR(100),
    [storageTempRequirements.high.extension] NVARCHAR(MAX),
    [storageTempRequirements.high.value] float,
    [storageTempRequirements.high.comparator] NVARCHAR(64),
    [storageTempRequirements.high.unit] NVARCHAR(100),
    [storageTempRequirements.high.system] VARCHAR(256),
    [storageTempRequirements.high.code] NVARCHAR(4000),
    [property] VARCHAR(MAX),
) WITH (
    LOCATION='/BiologicallyDerivedProduct/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

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

CREATE VIEW fhir.BiologicallyDerivedProductProcessingFacility AS
SELECT
    [id],
    [processingFacility.JSON],
    [processingFacility.id],
    [processingFacility.extension],
    [processingFacility.reference],
    [processingFacility.type],
    [processingFacility.identifier.id],
    [processingFacility.identifier.extension],
    [processingFacility.identifier.use],
    [processingFacility.identifier.type],
    [processingFacility.identifier.system],
    [processingFacility.identifier.value],
    [processingFacility.identifier.period],
    [processingFacility.identifier.assigner],
    [processingFacility.display]
FROM openrowset (
        BULK 'BiologicallyDerivedProduct/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [processingFacility.JSON]  VARCHAR(MAX) '$.processingFacility'
    ) AS rowset
    CROSS APPLY openjson (rowset.[processingFacility.JSON]) with (
        [processingFacility.id]        NVARCHAR(100)       '$.id',
        [processingFacility.extension] NVARCHAR(MAX)       '$.extension',
        [processingFacility.reference] NVARCHAR(4000)      '$.reference',
        [processingFacility.type]      VARCHAR(256)        '$.type',
        [processingFacility.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [processingFacility.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [processingFacility.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [processingFacility.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [processingFacility.identifier.system] VARCHAR(256)        '$.identifier.system',
        [processingFacility.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [processingFacility.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [processingFacility.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [processingFacility.display]   NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.BiologicallyDerivedProductProperty AS
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
    [property.value.boolean],
    [property.value.integer],
    [property.value.codeableConcept.id],
    [property.value.codeableConcept.extension],
    [property.value.codeableConcept.coding],
    [property.value.codeableConcept.text],
    [property.value.quantity.id],
    [property.value.quantity.extension],
    [property.value.quantity.value],
    [property.value.quantity.comparator],
    [property.value.quantity.unit],
    [property.value.quantity.system],
    [property.value.quantity.code],
    [property.value.range.id],
    [property.value.range.extension],
    [property.value.range.low],
    [property.value.range.high],
    [property.value.string],
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
        BULK 'BiologicallyDerivedProduct/**',
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
        [property.value.boolean]       bit                 '$.value.boolean',
        [property.value.integer]       bigint              '$.value.integer',
        [property.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [property.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [property.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [property.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [property.value.quantity.id]   NVARCHAR(100)       '$.value.quantity.id',
        [property.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [property.value.quantity.value] float               '$.value.quantity.value',
        [property.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [property.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [property.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [property.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [property.value.range.id]      NVARCHAR(100)       '$.value.range.id',
        [property.value.range.extension] NVARCHAR(MAX)       '$.value.range.extension',
        [property.value.range.low]     NVARCHAR(MAX)       '$.value.range.low',
        [property.value.range.high]    NVARCHAR(MAX)       '$.value.range.high',
        [property.value.string]        NVARCHAR(4000)      '$.value.string',
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
