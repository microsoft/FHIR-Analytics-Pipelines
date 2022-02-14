CREATE EXTERNAL TABLE [fhir].[TestScript] (
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
    [identifier.id] NVARCHAR(100),
    [identifier.extension] NVARCHAR(MAX),
    [identifier.use] NVARCHAR(64),
    [identifier.type.id] NVARCHAR(100),
    [identifier.type.extension] NVARCHAR(MAX),
    [identifier.type.coding] NVARCHAR(MAX),
    [identifier.type.text] NVARCHAR(4000),
    [identifier.system] VARCHAR(256),
    [identifier.value] NVARCHAR(4000),
    [identifier.period.id] NVARCHAR(100),
    [identifier.period.extension] NVARCHAR(MAX),
    [identifier.period.start] VARCHAR(64),
    [identifier.period.end] VARCHAR(64),
    [identifier.assigner.id] NVARCHAR(100),
    [identifier.assigner.extension] NVARCHAR(MAX),
    [identifier.assigner.reference] NVARCHAR(4000),
    [identifier.assigner.type] VARCHAR(256),
    [identifier.assigner.identifier] NVARCHAR(MAX),
    [identifier.assigner.display] NVARCHAR(4000),
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
    [origin] VARCHAR(MAX),
    [destination] VARCHAR(MAX),
    [metadata.id] NVARCHAR(100),
    [metadata.extension] NVARCHAR(MAX),
    [metadata.modifierExtension] NVARCHAR(MAX),
    [metadata.link] VARCHAR(MAX),
    [metadata.capability] VARCHAR(MAX),
    [fixture] VARCHAR(MAX),
    [profile] VARCHAR(MAX),
    [variable] VARCHAR(MAX),
    [setup.id] NVARCHAR(100),
    [setup.extension] NVARCHAR(MAX),
    [setup.modifierExtension] NVARCHAR(MAX),
    [setup.action] VARCHAR(MAX),
    [test] VARCHAR(MAX),
    [teardown.id] NVARCHAR(100),
    [teardown.extension] NVARCHAR(MAX),
    [teardown.modifierExtension] NVARCHAR(MAX),
    [teardown.action] VARCHAR(MAX),
) WITH (
    LOCATION='/TestScript/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.TestScriptContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'TestScript/**',
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

CREATE VIEW fhir.TestScriptUseContext AS
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
        BULK 'TestScript/**',
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

CREATE VIEW fhir.TestScriptJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'TestScript/**',
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

CREATE VIEW fhir.TestScriptOrigin AS
SELECT
    [id],
    [origin.JSON],
    [origin.id],
    [origin.extension],
    [origin.modifierExtension],
    [origin.index],
    [origin.profile.id],
    [origin.profile.extension],
    [origin.profile.system],
    [origin.profile.version],
    [origin.profile.code],
    [origin.profile.display],
    [origin.profile.userSelected]
FROM openrowset (
        BULK 'TestScript/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [origin.JSON]  VARCHAR(MAX) '$.origin'
    ) AS rowset
    CROSS APPLY openjson (rowset.[origin.JSON]) with (
        [origin.id]                    NVARCHAR(100)       '$.id',
        [origin.extension]             NVARCHAR(MAX)       '$.extension',
        [origin.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [origin.index]                 bigint              '$.index',
        [origin.profile.id]            NVARCHAR(100)       '$.profile.id',
        [origin.profile.extension]     NVARCHAR(MAX)       '$.profile.extension',
        [origin.profile.system]        VARCHAR(256)        '$.profile.system',
        [origin.profile.version]       NVARCHAR(100)       '$.profile.version',
        [origin.profile.code]          NVARCHAR(4000)      '$.profile.code',
        [origin.profile.display]       NVARCHAR(4000)      '$.profile.display',
        [origin.profile.userSelected]  bit                 '$.profile.userSelected'
    ) j

GO

CREATE VIEW fhir.TestScriptDestination AS
SELECT
    [id],
    [destination.JSON],
    [destination.id],
    [destination.extension],
    [destination.modifierExtension],
    [destination.index],
    [destination.profile.id],
    [destination.profile.extension],
    [destination.profile.system],
    [destination.profile.version],
    [destination.profile.code],
    [destination.profile.display],
    [destination.profile.userSelected]
FROM openrowset (
        BULK 'TestScript/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [destination.JSON]  VARCHAR(MAX) '$.destination'
    ) AS rowset
    CROSS APPLY openjson (rowset.[destination.JSON]) with (
        [destination.id]               NVARCHAR(100)       '$.id',
        [destination.extension]        NVARCHAR(MAX)       '$.extension',
        [destination.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [destination.index]            bigint              '$.index',
        [destination.profile.id]       NVARCHAR(100)       '$.profile.id',
        [destination.profile.extension] NVARCHAR(MAX)       '$.profile.extension',
        [destination.profile.system]   VARCHAR(256)        '$.profile.system',
        [destination.profile.version]  NVARCHAR(100)       '$.profile.version',
        [destination.profile.code]     NVARCHAR(4000)      '$.profile.code',
        [destination.profile.display]  NVARCHAR(4000)      '$.profile.display',
        [destination.profile.userSelected] bit                 '$.profile.userSelected'
    ) j

GO

CREATE VIEW fhir.TestScriptFixture AS
SELECT
    [id],
    [fixture.JSON],
    [fixture.id],
    [fixture.extension],
    [fixture.modifierExtension],
    [fixture.autocreate],
    [fixture.autodelete],
    [fixture.resource.id],
    [fixture.resource.extension],
    [fixture.resource.reference],
    [fixture.resource.type],
    [fixture.resource.identifier],
    [fixture.resource.display]
FROM openrowset (
        BULK 'TestScript/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [fixture.JSON]  VARCHAR(MAX) '$.fixture'
    ) AS rowset
    CROSS APPLY openjson (rowset.[fixture.JSON]) with (
        [fixture.id]                   NVARCHAR(100)       '$.id',
        [fixture.extension]            NVARCHAR(MAX)       '$.extension',
        [fixture.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [fixture.autocreate]           bit                 '$.autocreate',
        [fixture.autodelete]           bit                 '$.autodelete',
        [fixture.resource.id]          NVARCHAR(100)       '$.resource.id',
        [fixture.resource.extension]   NVARCHAR(MAX)       '$.resource.extension',
        [fixture.resource.reference]   NVARCHAR(4000)      '$.resource.reference',
        [fixture.resource.type]        VARCHAR(256)        '$.resource.type',
        [fixture.resource.identifier]  NVARCHAR(MAX)       '$.resource.identifier',
        [fixture.resource.display]     NVARCHAR(4000)      '$.resource.display'
    ) j

GO

CREATE VIEW fhir.TestScriptProfile AS
SELECT
    [id],
    [profile.JSON],
    [profile.id],
    [profile.extension],
    [profile.reference],
    [profile.type],
    [profile.identifier.id],
    [profile.identifier.extension],
    [profile.identifier.use],
    [profile.identifier.type],
    [profile.identifier.system],
    [profile.identifier.value],
    [profile.identifier.period],
    [profile.identifier.assigner],
    [profile.display]
FROM openrowset (
        BULK 'TestScript/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [profile.JSON]  VARCHAR(MAX) '$.profile'
    ) AS rowset
    CROSS APPLY openjson (rowset.[profile.JSON]) with (
        [profile.id]                   NVARCHAR(100)       '$.id',
        [profile.extension]            NVARCHAR(MAX)       '$.extension',
        [profile.reference]            NVARCHAR(4000)      '$.reference',
        [profile.type]                 VARCHAR(256)        '$.type',
        [profile.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [profile.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [profile.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [profile.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [profile.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [profile.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [profile.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [profile.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [profile.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.TestScriptVariable AS
SELECT
    [id],
    [variable.JSON],
    [variable.id],
    [variable.extension],
    [variable.modifierExtension],
    [variable.name],
    [variable.defaultValue],
    [variable.description],
    [variable.expression],
    [variable.headerField],
    [variable.hint],
    [variable.path],
    [variable.sourceId]
FROM openrowset (
        BULK 'TestScript/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [variable.JSON]  VARCHAR(MAX) '$.variable'
    ) AS rowset
    CROSS APPLY openjson (rowset.[variable.JSON]) with (
        [variable.id]                  NVARCHAR(100)       '$.id',
        [variable.extension]           NVARCHAR(MAX)       '$.extension',
        [variable.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [variable.name]                NVARCHAR(500)       '$.name',
        [variable.defaultValue]        NVARCHAR(4000)      '$.defaultValue',
        [variable.description]         NVARCHAR(4000)      '$.description',
        [variable.expression]          NVARCHAR(4000)      '$.expression',
        [variable.headerField]         NVARCHAR(4000)      '$.headerField',
        [variable.hint]                NVARCHAR(4000)      '$.hint',
        [variable.path]                NVARCHAR(4000)      '$.path',
        [variable.sourceId]            VARCHAR(64)         '$.sourceId'
    ) j

GO

CREATE VIEW fhir.TestScriptTest AS
SELECT
    [id],
    [test.JSON],
    [test.id],
    [test.extension],
    [test.modifierExtension],
    [test.name],
    [test.description],
    [test.action]
FROM openrowset (
        BULK 'TestScript/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [test.JSON]  VARCHAR(MAX) '$.test'
    ) AS rowset
    CROSS APPLY openjson (rowset.[test.JSON]) with (
        [test.id]                      NVARCHAR(100)       '$.id',
        [test.extension]               NVARCHAR(MAX)       '$.extension',
        [test.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [test.name]                    NVARCHAR(500)       '$.name',
        [test.description]             NVARCHAR(4000)      '$.description',
        [test.action]                  NVARCHAR(MAX)       '$.action' AS JSON
    ) j
