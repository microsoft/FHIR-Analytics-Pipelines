CREATE EXTERNAL TABLE [fhir].[SearchParameter] (
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
    [url] VARCHAR(256),
    [version] NVARCHAR(100),
    [name] NVARCHAR(500),
    [derivedFrom] VARCHAR(256),
    [status] NVARCHAR(64),
    [experimental] bit,
    [date] VARCHAR(64),
    [publisher] NVARCHAR(500),
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
        [contact.id]                   NVARCHAR(100)       '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.name]                 NVARCHAR(500)       '$.name',
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
    [useContext.value.codeableConcept.id],
    [useContext.value.codeableConcept.extension],
    [useContext.value.codeableConcept.coding],
    [useContext.value.codeableConcept.text],
    [useContext.value.quantity.id],
    [useContext.value.quantity.extension],
    [useContext.value.quantity.value],
    [useContext.value.quantity.comparator],
    [useContext.value.quantity.unit],
    [useContext.value.quantity.system],
    [useContext.value.quantity.code],
    [useContext.value.range.id],
    [useContext.value.range.extension],
    [useContext.value.range.low],
    [useContext.value.range.high],
    [useContext.value.reference.id],
    [useContext.value.reference.extension],
    [useContext.value.reference.reference],
    [useContext.value.reference.type],
    [useContext.value.reference.identifier],
    [useContext.value.reference.display]
FROM openrowset (
        BULK 'SearchParameter/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [useContext.JSON]  VARCHAR(MAX) '$.useContext'
    ) AS rowset
    CROSS APPLY openjson (rowset.[useContext.JSON]) with (
        [useContext.id]                NVARCHAR(100)       '$.id',
        [useContext.extension]         NVARCHAR(MAX)       '$.extension',
        [useContext.code.id]           NVARCHAR(100)       '$.code.id',
        [useContext.code.extension]    NVARCHAR(MAX)       '$.code.extension',
        [useContext.code.system]       VARCHAR(256)        '$.code.system',
        [useContext.code.version]      NVARCHAR(100)       '$.code.version',
        [useContext.code.code]         NVARCHAR(4000)      '$.code.code',
        [useContext.code.display]      NVARCHAR(4000)      '$.code.display',
        [useContext.code.userSelected] bit                 '$.code.userSelected',
        [useContext.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [useContext.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [useContext.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [useContext.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [useContext.value.quantity.id] NVARCHAR(100)       '$.value.quantity.id',
        [useContext.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [useContext.value.quantity.value] float               '$.value.quantity.value',
        [useContext.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [useContext.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [useContext.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [useContext.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [useContext.value.range.id]    NVARCHAR(100)       '$.value.range.id',
        [useContext.value.range.extension] NVARCHAR(MAX)       '$.value.range.extension',
        [useContext.value.range.low]   NVARCHAR(MAX)       '$.value.range.low',
        [useContext.value.range.high]  NVARCHAR(MAX)       '$.value.range.high',
        [useContext.value.reference.id] NVARCHAR(100)       '$.value.reference.id',
        [useContext.value.reference.extension] NVARCHAR(MAX)       '$.value.reference.extension',
        [useContext.value.reference.reference] NVARCHAR(4000)      '$.value.reference.reference',
        [useContext.value.reference.type] VARCHAR(256)        '$.value.reference.type',
        [useContext.value.reference.identifier] NVARCHAR(MAX)       '$.value.reference.identifier',
        [useContext.value.reference.display] NVARCHAR(4000)      '$.value.reference.display'
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
        [jurisdiction.id]              NVARCHAR(100)       '$.id',
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
        [component.id]                 NVARCHAR(100)       '$.id',
        [component.extension]          NVARCHAR(MAX)       '$.extension',
        [component.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [component.definition]         VARCHAR(256)        '$.definition',
        [component.expression]         NVARCHAR(4000)      '$.expression'
    ) j
