CREATE EXTERNAL TABLE [fhir].[OperationDefinition] (
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
    [title] NVARCHAR(4000),
    [status] NVARCHAR(64),
    [kind] NVARCHAR(64),
    [experimental] bit,
    [date] VARCHAR(30),
    [publisher] NVARCHAR(4000),
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
        [contact.id]                   NVARCHAR(4000)      '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.name]                 NVARCHAR(4000)      '$.name',
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
        BULK 'OperationDefinition/**',
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
        [jurisdiction.id]              NVARCHAR(4000)      '$.id',
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
        [parameter.id]                 NVARCHAR(4000)      '$.id',
        [parameter.extension]          NVARCHAR(MAX)       '$.extension',
        [parameter.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [parameter.name]               NVARCHAR(4000)      '$.name',
        [parameter.use]                NVARCHAR(64)        '$.use',
        [parameter.min]                bigint              '$.min',
        [parameter.max]                NVARCHAR(4000)      '$.max',
        [parameter.documentation]      NVARCHAR(4000)      '$.documentation',
        [parameter.type]               NVARCHAR(4000)      '$.type',
        [parameter.targetProfile]      NVARCHAR(MAX)       '$.targetProfile' AS JSON,
        [parameter.searchType]         NVARCHAR(64)        '$.searchType',
        [parameter.binding.id]         NVARCHAR(4000)      '$.binding.id',
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
        [overload.id]                  NVARCHAR(4000)      '$.id',
        [overload.extension]           NVARCHAR(MAX)       '$.extension',
        [overload.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [overload.parameterName]       NVARCHAR(MAX)       '$.parameterName' AS JSON,
        [overload.comment]             NVARCHAR(4000)      '$.comment'
    ) j
