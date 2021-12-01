CREATE EXTERNAL TABLE [fhir].[SearchParameter] (
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
    [url] VARCHAR(256),
    [version] NVARCHAR(4000),
    [name] NVARCHAR(4000),
    [derivedFrom] VARCHAR(256),
    [status] NVARCHAR(64),
    [experimental] bit,
    [date] VARCHAR(30),
    [publisher] NVARCHAR(4000),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [purpose] NVARCHAR(MAX),
    [code] NVARCHAR(4000),
    [base] VARCHAR(MAX),
    [type] NVARCHAR(64),
    [expression] NVARCHAR(4000),
    [xpath] NVARCHAR(4000),
    [xpathUsage] NVARCHAR(64),
    [target] VARCHAR(MAX),
    [multipleOr] bit,
    [multipleAnd] bit,
    [comparator] VARCHAR(MAX),
    [modifier] VARCHAR(MAX),
    [chain] VARCHAR(MAX),
    [component] VARCHAR(MAX),
) WITH (
    LOCATION='/SearchParameter/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SearchParameterContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'SearchParameter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contact.JSON]  VARCHAR(MAX) '$.contact'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contact.JSON]) with (
        [contact.id]                   NVARCHAR(4000)      '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.name]                 NVARCHAR(4000)      '$.name',
        [contact.telecom]              NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.SearchParameterUseContext AS
SELECT
    [id],
    [useContext.JSON],
    [useContext.id],
    [useContext.extension],
    [useContext.code.id],
    [useContext.code.extension],
    [useContext.code.system],
    [useContext.code.version],
    [useContext.code.code],
    [useContext.code.display],
    [useContext.code.userSelected],
    [useContext.value.CodeableConcept.id],
    [useContext.value.CodeableConcept.extension],
    [useContext.value.CodeableConcept.coding],
    [useContext.value.CodeableConcept.text],
    [useContext.value.Quantity.id],
    [useContext.value.Quantity.extension],
    [useContext.value.Quantity.value],
    [useContext.value.Quantity.comparator],
    [useContext.value.Quantity.unit],
    [useContext.value.Quantity.system],
    [useContext.value.Quantity.code],
    [useContext.value.Range.id],
    [useContext.value.Range.extension],
    [useContext.value.Range.low],
    [useContext.value.Range.high],
    [useContext.value.Reference.id],
    [useContext.value.Reference.extension],
    [useContext.value.Reference.reference],
    [useContext.value.Reference.type],
    [useContext.value.Reference.identifier],
    [useContext.value.Reference.display]
FROM openrowset (
        BULK 'SearchParameter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [useContext.JSON]  VARCHAR(MAX) '$.useContext'
    ) AS rowset
    CROSS APPLY openjson (rowset.[useContext.JSON]) with (
        [useContext.id]                NVARCHAR(4000)      '$.id',
        [useContext.extension]         NVARCHAR(MAX)       '$.extension',
        [useContext.code.id]           NVARCHAR(4000)      '$.code.id',
        [useContext.code.extension]    NVARCHAR(MAX)       '$.code.extension',
        [useContext.code.system]       VARCHAR(256)        '$.code.system',
        [useContext.code.version]      NVARCHAR(4000)      '$.code.version',
        [useContext.code.code]         NVARCHAR(4000)      '$.code.code',
        [useContext.code.display]      NVARCHAR(4000)      '$.code.display',
        [useContext.code.userSelected] bit                 '$.code.userSelected',
        [useContext.value.CodeableConcept.id] NVARCHAR(4000)      '$.value.CodeableConcept.id',
        [useContext.value.CodeableConcept.extension] NVARCHAR(MAX)       '$.value.CodeableConcept.extension',
        [useContext.value.CodeableConcept.coding] NVARCHAR(MAX)       '$.value.CodeableConcept.coding',
        [useContext.value.CodeableConcept.text] NVARCHAR(4000)      '$.value.CodeableConcept.text',
        [useContext.value.Quantity.id] NVARCHAR(4000)      '$.value.Quantity.id',
        [useContext.value.Quantity.extension] NVARCHAR(MAX)       '$.value.Quantity.extension',
        [useContext.value.Quantity.value] float               '$.value.Quantity.value',
        [useContext.value.Quantity.comparator] NVARCHAR(64)        '$.value.Quantity.comparator',
        [useContext.value.Quantity.unit] NVARCHAR(4000)      '$.value.Quantity.unit',
        [useContext.value.Quantity.system] VARCHAR(256)        '$.value.Quantity.system',
        [useContext.value.Quantity.code] NVARCHAR(4000)      '$.value.Quantity.code',
        [useContext.value.Range.id]    NVARCHAR(4000)      '$.value.Range.id',
        [useContext.value.Range.extension] NVARCHAR(MAX)       '$.value.Range.extension',
        [useContext.value.Range.low]   NVARCHAR(MAX)       '$.value.Range.low',
        [useContext.value.Range.high]  NVARCHAR(MAX)       '$.value.Range.high',
        [useContext.value.Reference.id] NVARCHAR(4000)      '$.value.Reference.id',
        [useContext.value.Reference.extension] NVARCHAR(MAX)       '$.value.Reference.extension',
        [useContext.value.Reference.reference] NVARCHAR(4000)      '$.value.Reference.reference',
        [useContext.value.Reference.type] VARCHAR(256)        '$.value.Reference.type',
        [useContext.value.Reference.identifier] NVARCHAR(MAX)       '$.value.Reference.identifier',
        [useContext.value.Reference.display] NVARCHAR(4000)      '$.value.Reference.display'
    ) j

GO

CREATE VIEW fhir.SearchParameterJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'SearchParameter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [jurisdiction.JSON]  VARCHAR(MAX) '$.jurisdiction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[jurisdiction.JSON]) with (
        [jurisdiction.id]              NVARCHAR(4000)      '$.id',
        [jurisdiction.extension]       NVARCHAR(MAX)       '$.extension',
        [jurisdiction.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [jurisdiction.text]            NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.SearchParameterBase AS
SELECT
    [id],
    [base.JSON],
    [base]
FROM openrowset (
        BULK 'SearchParameter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [base.JSON]  VARCHAR(MAX) '$.base'
    ) AS rowset
    CROSS APPLY openjson (rowset.[base.JSON]) with (
        [base]                         NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.SearchParameterTarget AS
SELECT
    [id],
    [target.JSON],
    [target]
FROM openrowset (
        BULK 'SearchParameter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [target.JSON]  VARCHAR(MAX) '$.target'
    ) AS rowset
    CROSS APPLY openjson (rowset.[target.JSON]) with (
        [target]                       NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.SearchParameterComparator AS
SELECT
    [id],
    [comparator.JSON],
    [comparator]
FROM openrowset (
        BULK 'SearchParameter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [comparator.JSON]  VARCHAR(MAX) '$.comparator'
    ) AS rowset
    CROSS APPLY openjson (rowset.[comparator.JSON]) with (
        [comparator]                   NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.SearchParameterModifier AS
SELECT
    [id],
    [modifier.JSON],
    [modifier]
FROM openrowset (
        BULK 'SearchParameter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [modifier.JSON]  VARCHAR(MAX) '$.modifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[modifier.JSON]) with (
        [modifier]                     NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.SearchParameterChain AS
SELECT
    [id],
    [chain.JSON],
    [chain]
FROM openrowset (
        BULK 'SearchParameter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [chain.JSON]  VARCHAR(MAX) '$.chain'
    ) AS rowset
    CROSS APPLY openjson (rowset.[chain.JSON]) with (
        [chain]                        NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.SearchParameterComponent AS
SELECT
    [id],
    [component.JSON],
    [component.id],
    [component.extension],
    [component.modifierExtension],
    [component.definition],
    [component.expression]
FROM openrowset (
        BULK 'SearchParameter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [component.JSON]  VARCHAR(MAX) '$.component'
    ) AS rowset
    CROSS APPLY openjson (rowset.[component.JSON]) with (
        [component.id]                 NVARCHAR(4000)      '$.id',
        [component.extension]          NVARCHAR(MAX)       '$.extension',
        [component.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [component.definition]         VARCHAR(256)        '$.definition',
        [component.expression]         NVARCHAR(4000)      '$.expression'
    ) j
