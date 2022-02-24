CREATE EXTERNAL TABLE [fhir].[CapabilityStatement] (
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
    [experimental] bit,
    [date] VARCHAR(64),
    [publisher] NVARCHAR(500),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [purpose] NVARCHAR(MAX),
    [copyright] NVARCHAR(MAX),
    [kind] NVARCHAR(64),
    [instantiates] VARCHAR(MAX),
    [imports] VARCHAR(MAX),
    [software.id] NVARCHAR(100),
    [software.extension] NVARCHAR(MAX),
    [software.modifierExtension] NVARCHAR(MAX),
    [software.name] NVARCHAR(500),
    [software.version] NVARCHAR(100),
    [software.releaseDate] VARCHAR(64),
    [implementation.id] NVARCHAR(100),
    [implementation.extension] NVARCHAR(MAX),
    [implementation.modifierExtension] NVARCHAR(MAX),
    [implementation.description] NVARCHAR(4000),
    [implementation.url] VARCHAR(256),
    [implementation.custodian.id] NVARCHAR(100),
    [implementation.custodian.extension] NVARCHAR(MAX),
    [implementation.custodian.reference] NVARCHAR(4000),
    [implementation.custodian.type] VARCHAR(256),
    [implementation.custodian.identifier] NVARCHAR(MAX),
    [implementation.custodian.display] NVARCHAR(4000),
    [fhirVersion] NVARCHAR(64),
    [format] VARCHAR(MAX),
    [patchFormat] VARCHAR(MAX),
    [implementationGuide] VARCHAR(MAX),
    [rest] VARCHAR(MAX),
    [messaging] VARCHAR(MAX),
    [document] VARCHAR(MAX),
) WITH (
    LOCATION='/CapabilityStatement/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.CapabilityStatementContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'CapabilityStatement/**',
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

CREATE VIEW fhir.CapabilityStatementUseContext AS
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
        BULK 'CapabilityStatement/**',
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

CREATE VIEW fhir.CapabilityStatementJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'CapabilityStatement/**',
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

CREATE VIEW fhir.CapabilityStatementInstantiates AS
SELECT
    [id],
    [instantiates.JSON],
    [instantiates]
FROM openrowset (
        BULK 'CapabilityStatement/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiates.JSON]  VARCHAR(MAX) '$.instantiates'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiates.JSON]) with (
        [instantiates]                 NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.CapabilityStatementImports AS
SELECT
    [id],
    [imports.JSON],
    [imports]
FROM openrowset (
        BULK 'CapabilityStatement/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [imports.JSON]  VARCHAR(MAX) '$.imports'
    ) AS rowset
    CROSS APPLY openjson (rowset.[imports.JSON]) with (
        [imports]                      NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.CapabilityStatementFormat AS
SELECT
    [id],
    [format.JSON],
    [format]
FROM openrowset (
        BULK 'CapabilityStatement/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [format.JSON]  VARCHAR(MAX) '$.format'
    ) AS rowset
    CROSS APPLY openjson (rowset.[format.JSON]) with (
        [format]                       NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.CapabilityStatementPatchFormat AS
SELECT
    [id],
    [patchFormat.JSON],
    [patchFormat]
FROM openrowset (
        BULK 'CapabilityStatement/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [patchFormat.JSON]  VARCHAR(MAX) '$.patchFormat'
    ) AS rowset
    CROSS APPLY openjson (rowset.[patchFormat.JSON]) with (
        [patchFormat]                  NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.CapabilityStatementImplementationGuide AS
SELECT
    [id],
    [implementationGuide.JSON],
    [implementationGuide]
FROM openrowset (
        BULK 'CapabilityStatement/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [implementationGuide.JSON]  VARCHAR(MAX) '$.implementationGuide'
    ) AS rowset
    CROSS APPLY openjson (rowset.[implementationGuide.JSON]) with (
        [implementationGuide]          NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.CapabilityStatementRest AS
SELECT
    [id],
    [rest.JSON],
    [rest.id],
    [rest.extension],
    [rest.modifierExtension],
    [rest.mode],
    [rest.documentation],
    [rest.security.id],
    [rest.security.extension],
    [rest.security.modifierExtension],
    [rest.security.cors],
    [rest.security.service],
    [rest.security.description],
    [rest.resource],
    [rest.interaction],
    [rest.searchParam],
    [rest.operation],
    [rest.compartment]
FROM openrowset (
        BULK 'CapabilityStatement/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [rest.JSON]  VARCHAR(MAX) '$.rest'
    ) AS rowset
    CROSS APPLY openjson (rowset.[rest.JSON]) with (
        [rest.id]                      NVARCHAR(100)       '$.id',
        [rest.extension]               NVARCHAR(MAX)       '$.extension',
        [rest.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [rest.mode]                    NVARCHAR(64)        '$.mode',
        [rest.documentation]           NVARCHAR(MAX)       '$.documentation',
        [rest.security.id]             NVARCHAR(100)       '$.security.id',
        [rest.security.extension]      NVARCHAR(MAX)       '$.security.extension',
        [rest.security.modifierExtension] NVARCHAR(MAX)       '$.security.modifierExtension',
        [rest.security.cors]           bit                 '$.security.cors',
        [rest.security.service]        NVARCHAR(MAX)       '$.security.service',
        [rest.security.description]    NVARCHAR(MAX)       '$.security.description',
        [rest.resource]                NVARCHAR(MAX)       '$.resource' AS JSON,
        [rest.interaction]             NVARCHAR(MAX)       '$.interaction' AS JSON,
        [rest.searchParam]             NVARCHAR(MAX)       '$.searchParam' AS JSON,
        [rest.operation]               NVARCHAR(MAX)       '$.operation' AS JSON,
        [rest.compartment]             NVARCHAR(MAX)       '$.compartment' AS JSON
    ) j

GO

CREATE VIEW fhir.CapabilityStatementMessaging AS
SELECT
    [id],
    [messaging.JSON],
    [messaging.id],
    [messaging.extension],
    [messaging.modifierExtension],
    [messaging.endpoint],
    [messaging.reliableCache],
    [messaging.documentation],
    [messaging.supportedMessage]
FROM openrowset (
        BULK 'CapabilityStatement/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [messaging.JSON]  VARCHAR(MAX) '$.messaging'
    ) AS rowset
    CROSS APPLY openjson (rowset.[messaging.JSON]) with (
        [messaging.id]                 NVARCHAR(100)       '$.id',
        [messaging.extension]          NVARCHAR(MAX)       '$.extension',
        [messaging.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [messaging.endpoint]           NVARCHAR(MAX)       '$.endpoint' AS JSON,
        [messaging.reliableCache]      bigint              '$.reliableCache',
        [messaging.documentation]      NVARCHAR(MAX)       '$.documentation',
        [messaging.supportedMessage]   NVARCHAR(MAX)       '$.supportedMessage' AS JSON
    ) j

GO

CREATE VIEW fhir.CapabilityStatementDocument AS
SELECT
    [id],
    [document.JSON],
    [document.id],
    [document.extension],
    [document.modifierExtension],
    [document.mode],
    [document.documentation],
    [document.profile]
FROM openrowset (
        BULK 'CapabilityStatement/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [document.JSON]  VARCHAR(MAX) '$.document'
    ) AS rowset
    CROSS APPLY openjson (rowset.[document.JSON]) with (
        [document.id]                  NVARCHAR(100)       '$.id',
        [document.extension]           NVARCHAR(MAX)       '$.extension',
        [document.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [document.mode]                NVARCHAR(64)        '$.mode',
        [document.documentation]       NVARCHAR(MAX)       '$.documentation',
        [document.profile]             VARCHAR(256)        '$.profile'
    ) j
