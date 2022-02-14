CREATE EXTERNAL TABLE [fhir].[ObservationDefinition] (
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
    [category] VARCHAR(MAX),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [identifier] VARCHAR(MAX),
    [permittedDataType] VARCHAR(MAX),
    [multipleResultsAllowed] bit,
    [method.id] NVARCHAR(100),
    [method.extension] NVARCHAR(MAX),
    [method.coding] VARCHAR(MAX),
    [method.text] NVARCHAR(4000),
    [preferredReportName] NVARCHAR(500),
    [quantitativeDetails.id] NVARCHAR(100),
    [quantitativeDetails.extension] NVARCHAR(MAX),
    [quantitativeDetails.modifierExtension] NVARCHAR(MAX),
    [quantitativeDetails.customaryUnit.id] NVARCHAR(100),
    [quantitativeDetails.customaryUnit.extension] NVARCHAR(MAX),
    [quantitativeDetails.customaryUnit.coding] NVARCHAR(MAX),
    [quantitativeDetails.customaryUnit.text] NVARCHAR(4000),
    [quantitativeDetails.unit.id] NVARCHAR(100),
    [quantitativeDetails.unit.extension] NVARCHAR(MAX),
    [quantitativeDetails.unit.coding] NVARCHAR(MAX),
    [quantitativeDetails.unit.text] NVARCHAR(4000),
    [quantitativeDetails.conversionFactor] float,
    [quantitativeDetails.decimalPrecision] bigint,
    [qualifiedInterval] VARCHAR(MAX),
    [validCodedValueSet.id] NVARCHAR(100),
    [validCodedValueSet.extension] NVARCHAR(MAX),
    [validCodedValueSet.reference] NVARCHAR(4000),
    [validCodedValueSet.type] VARCHAR(256),
    [validCodedValueSet.identifier.id] NVARCHAR(100),
    [validCodedValueSet.identifier.extension] NVARCHAR(MAX),
    [validCodedValueSet.identifier.use] NVARCHAR(64),
    [validCodedValueSet.identifier.type] NVARCHAR(MAX),
    [validCodedValueSet.identifier.system] VARCHAR(256),
    [validCodedValueSet.identifier.value] NVARCHAR(4000),
    [validCodedValueSet.identifier.period] NVARCHAR(MAX),
    [validCodedValueSet.identifier.assigner] NVARCHAR(MAX),
    [validCodedValueSet.display] NVARCHAR(4000),
    [normalCodedValueSet.id] NVARCHAR(100),
    [normalCodedValueSet.extension] NVARCHAR(MAX),
    [normalCodedValueSet.reference] NVARCHAR(4000),
    [normalCodedValueSet.type] VARCHAR(256),
    [normalCodedValueSet.identifier.id] NVARCHAR(100),
    [normalCodedValueSet.identifier.extension] NVARCHAR(MAX),
    [normalCodedValueSet.identifier.use] NVARCHAR(64),
    [normalCodedValueSet.identifier.type] NVARCHAR(MAX),
    [normalCodedValueSet.identifier.system] VARCHAR(256),
    [normalCodedValueSet.identifier.value] NVARCHAR(4000),
    [normalCodedValueSet.identifier.period] NVARCHAR(MAX),
    [normalCodedValueSet.identifier.assigner] NVARCHAR(MAX),
    [normalCodedValueSet.display] NVARCHAR(4000),
    [abnormalCodedValueSet.id] NVARCHAR(100),
    [abnormalCodedValueSet.extension] NVARCHAR(MAX),
    [abnormalCodedValueSet.reference] NVARCHAR(4000),
    [abnormalCodedValueSet.type] VARCHAR(256),
    [abnormalCodedValueSet.identifier.id] NVARCHAR(100),
    [abnormalCodedValueSet.identifier.extension] NVARCHAR(MAX),
    [abnormalCodedValueSet.identifier.use] NVARCHAR(64),
    [abnormalCodedValueSet.identifier.type] NVARCHAR(MAX),
    [abnormalCodedValueSet.identifier.system] VARCHAR(256),
    [abnormalCodedValueSet.identifier.value] NVARCHAR(4000),
    [abnormalCodedValueSet.identifier.period] NVARCHAR(MAX),
    [abnormalCodedValueSet.identifier.assigner] NVARCHAR(MAX),
    [abnormalCodedValueSet.display] NVARCHAR(4000),
    [criticalCodedValueSet.id] NVARCHAR(100),
    [criticalCodedValueSet.extension] NVARCHAR(MAX),
    [criticalCodedValueSet.reference] NVARCHAR(4000),
    [criticalCodedValueSet.type] VARCHAR(256),
    [criticalCodedValueSet.identifier.id] NVARCHAR(100),
    [criticalCodedValueSet.identifier.extension] NVARCHAR(MAX),
    [criticalCodedValueSet.identifier.use] NVARCHAR(64),
    [criticalCodedValueSet.identifier.type] NVARCHAR(MAX),
    [criticalCodedValueSet.identifier.system] VARCHAR(256),
    [criticalCodedValueSet.identifier.value] NVARCHAR(4000),
    [criticalCodedValueSet.identifier.period] NVARCHAR(MAX),
    [criticalCodedValueSet.identifier.assigner] NVARCHAR(MAX),
    [criticalCodedValueSet.display] NVARCHAR(4000),
) WITH (
    LOCATION='/ObservationDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ObservationDefinitionCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'ObservationDefinition/**',
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

CREATE VIEW fhir.ObservationDefinitionIdentifier AS
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
        BULK 'ObservationDefinition/**',
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

CREATE VIEW fhir.ObservationDefinitionPermittedDataType AS
SELECT
    [id],
    [permittedDataType.JSON],
    [permittedDataType]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [permittedDataType.JSON]  VARCHAR(MAX) '$.permittedDataType'
    ) AS rowset
    CROSS APPLY openjson (rowset.[permittedDataType.JSON]) with (
        [permittedDataType]            NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionQualifiedInterval AS
SELECT
    [id],
    [qualifiedInterval.JSON],
    [qualifiedInterval.id],
    [qualifiedInterval.extension],
    [qualifiedInterval.modifierExtension],
    [qualifiedInterval.category],
    [qualifiedInterval.range.id],
    [qualifiedInterval.range.extension],
    [qualifiedInterval.range.low],
    [qualifiedInterval.range.high],
    [qualifiedInterval.context.id],
    [qualifiedInterval.context.extension],
    [qualifiedInterval.context.coding],
    [qualifiedInterval.context.text],
    [qualifiedInterval.appliesTo],
    [qualifiedInterval.gender],
    [qualifiedInterval.age.id],
    [qualifiedInterval.age.extension],
    [qualifiedInterval.age.low],
    [qualifiedInterval.age.high],
    [qualifiedInterval.gestationalAge.id],
    [qualifiedInterval.gestationalAge.extension],
    [qualifiedInterval.gestationalAge.low],
    [qualifiedInterval.gestationalAge.high],
    [qualifiedInterval.condition]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [qualifiedInterval.JSON]  VARCHAR(MAX) '$.qualifiedInterval'
    ) AS rowset
    CROSS APPLY openjson (rowset.[qualifiedInterval.JSON]) with (
        [qualifiedInterval.id]         NVARCHAR(100)       '$.id',
        [qualifiedInterval.extension]  NVARCHAR(MAX)       '$.extension',
        [qualifiedInterval.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [qualifiedInterval.category]   NVARCHAR(64)        '$.category',
        [qualifiedInterval.range.id]   NVARCHAR(100)       '$.range.id',
        [qualifiedInterval.range.extension] NVARCHAR(MAX)       '$.range.extension',
        [qualifiedInterval.range.low]  NVARCHAR(MAX)       '$.range.low',
        [qualifiedInterval.range.high] NVARCHAR(MAX)       '$.range.high',
        [qualifiedInterval.context.id] NVARCHAR(100)       '$.context.id',
        [qualifiedInterval.context.extension] NVARCHAR(MAX)       '$.context.extension',
        [qualifiedInterval.context.coding] NVARCHAR(MAX)       '$.context.coding',
        [qualifiedInterval.context.text] NVARCHAR(4000)      '$.context.text',
        [qualifiedInterval.appliesTo]  NVARCHAR(MAX)       '$.appliesTo' AS JSON,
        [qualifiedInterval.gender]     NVARCHAR(64)        '$.gender',
        [qualifiedInterval.age.id]     NVARCHAR(100)       '$.age.id',
        [qualifiedInterval.age.extension] NVARCHAR(MAX)       '$.age.extension',
        [qualifiedInterval.age.low]    NVARCHAR(MAX)       '$.age.low',
        [qualifiedInterval.age.high]   NVARCHAR(MAX)       '$.age.high',
        [qualifiedInterval.gestationalAge.id] NVARCHAR(100)       '$.gestationalAge.id',
        [qualifiedInterval.gestationalAge.extension] NVARCHAR(MAX)       '$.gestationalAge.extension',
        [qualifiedInterval.gestationalAge.low] NVARCHAR(MAX)       '$.gestationalAge.low',
        [qualifiedInterval.gestationalAge.high] NVARCHAR(MAX)       '$.gestationalAge.high',
        [qualifiedInterval.condition]  NVARCHAR(500)       '$.condition'
    ) j
