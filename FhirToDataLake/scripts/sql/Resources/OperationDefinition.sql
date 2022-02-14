CREATE EXTERNAL TABLE [fhir].[OperationDefinition] (
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
    [title] NVARCHAR(4000),
    [status] NVARCHAR(64),
    [kind] NVARCHAR(64),
    [experimental] bit,
    [date] VARCHAR(64),
    [publisher] NVARCHAR(500),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [purpose] NVARCHAR(MAX),
    [affectsState] bit,
    [code] NVARCHAR(4000),
    [comment] NVARCHAR(MAX),
    [base] VARCHAR(256),
    [resource] VARCHAR(MAX),
    [system] bit,
    [type] bit,
    [instance] bit,
    [inputProfile] VARCHAR(256),
    [outputProfile] VARCHAR(256),
    [parameter] VARCHAR(MAX),
    [overload] VARCHAR(MAX),
) WITH (
    LOCATION='/OperationDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.OperationDefinitionContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'OperationDefinition/**',
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

CREATE VIEW fhir.OperationDefinitionUseContext AS
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
        BULK 'OperationDefinition/**',
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

CREATE VIEW fhir.OperationDefinitionJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'OperationDefinition/**',
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

CREATE VIEW fhir.OperationDefinitionResource AS
SELECT
    [id],
    [resource.JSON],
    [resource]
FROM openrowset (
        BULK 'OperationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [resource.JSON]  VARCHAR(MAX) '$.resource'
    ) AS rowset
    CROSS APPLY openjson (rowset.[resource.JSON]) with (
        [resource]                     NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.OperationDefinitionParameter AS
SELECT
    [id],
    [parameter.JSON],
    [parameter.id],
    [parameter.extension],
    [parameter.modifierExtension],
    [parameter.name],
    [parameter.use],
    [parameter.min],
    [parameter.max],
    [parameter.documentation],
    [parameter.type],
    [parameter.targetProfile],
    [parameter.searchType],
    [parameter.binding.id],
    [parameter.binding.extension],
    [parameter.binding.modifierExtension],
    [parameter.binding.strength],
    [parameter.binding.valueSet],
    [parameter.referencedFrom],
    [parameter.part]
FROM openrowset (
        BULK 'OperationDefinition/**',
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
        [parameter.name]               NVARCHAR(500)       '$.name',
        [parameter.use]                NVARCHAR(64)        '$.use',
        [parameter.min]                bigint              '$.min',
        [parameter.max]                NVARCHAR(100)       '$.max',
        [parameter.documentation]      NVARCHAR(4000)      '$.documentation',
        [parameter.type]               NVARCHAR(100)       '$.type',
        [parameter.targetProfile]      NVARCHAR(MAX)       '$.targetProfile' AS JSON,
        [parameter.searchType]         NVARCHAR(64)        '$.searchType',
        [parameter.binding.id]         NVARCHAR(100)       '$.binding.id',
        [parameter.binding.extension]  NVARCHAR(MAX)       '$.binding.extension',
        [parameter.binding.modifierExtension] NVARCHAR(MAX)       '$.binding.modifierExtension',
        [parameter.binding.strength]   NVARCHAR(64)        '$.binding.strength',
        [parameter.binding.valueSet]   VARCHAR(256)        '$.binding.valueSet',
        [parameter.referencedFrom]     NVARCHAR(MAX)       '$.referencedFrom' AS JSON,
        [parameter.part]               NVARCHAR(MAX)       '$.part' AS JSON
    ) j

GO

CREATE VIEW fhir.OperationDefinitionOverload AS
SELECT
    [id],
    [overload.JSON],
    [overload.id],
    [overload.extension],
    [overload.modifierExtension],
    [overload.parameterName],
    [overload.comment]
FROM openrowset (
        BULK 'OperationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [overload.JSON]  VARCHAR(MAX) '$.overload'
    ) AS rowset
    CROSS APPLY openjson (rowset.[overload.JSON]) with (
        [overload.id]                  NVARCHAR(100)       '$.id',
        [overload.extension]           NVARCHAR(MAX)       '$.extension',
        [overload.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [overload.parameterName]       NVARCHAR(MAX)       '$.parameterName' AS JSON,
        [overload.comment]             NVARCHAR(4000)      '$.comment'
    ) j
