CREATE EXTERNAL TABLE [fhir].[RegulatedAuthorization] (
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
    [subject] VARCHAR(MAX),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [description] NVARCHAR(MAX),
    [region] VARCHAR(MAX),
    [status.id] NVARCHAR(100),
    [status.extension] NVARCHAR(MAX),
    [status.coding] VARCHAR(MAX),
    [status.text] NVARCHAR(4000),
    [statusDate] VARCHAR(64),
    [validityPeriod.id] NVARCHAR(100),
    [validityPeriod.extension] NVARCHAR(MAX),
    [validityPeriod.start] VARCHAR(64),
    [validityPeriod.end] VARCHAR(64),
    [indication.id] NVARCHAR(100),
    [indication.extension] NVARCHAR(MAX),
    [indication.concept.id] NVARCHAR(100),
    [indication.concept.extension] NVARCHAR(MAX),
    [indication.concept.coding] NVARCHAR(MAX),
    [indication.concept.text] NVARCHAR(4000),
    [indication.reference.id] NVARCHAR(100),
    [indication.reference.extension] NVARCHAR(MAX),
    [indication.reference.reference] NVARCHAR(4000),
    [indication.reference.type] VARCHAR(256),
    [indication.reference.identifier] NVARCHAR(MAX),
    [indication.reference.display] NVARCHAR(4000),
    [intendedUse.id] NVARCHAR(100),
    [intendedUse.extension] NVARCHAR(MAX),
    [intendedUse.coding] VARCHAR(MAX),
    [intendedUse.text] NVARCHAR(4000),
    [basis] VARCHAR(MAX),
    [holder.id] NVARCHAR(100),
    [holder.extension] NVARCHAR(MAX),
    [holder.reference] NVARCHAR(4000),
    [holder.type] VARCHAR(256),
    [holder.identifier.id] NVARCHAR(100),
    [holder.identifier.extension] NVARCHAR(MAX),
    [holder.identifier.use] NVARCHAR(64),
    [holder.identifier.type] NVARCHAR(MAX),
    [holder.identifier.system] VARCHAR(256),
    [holder.identifier.value] NVARCHAR(4000),
    [holder.identifier.period] NVARCHAR(MAX),
    [holder.identifier.assigner] NVARCHAR(MAX),
    [holder.display] NVARCHAR(4000),
    [regulator.id] NVARCHAR(100),
    [regulator.extension] NVARCHAR(MAX),
    [regulator.reference] NVARCHAR(4000),
    [regulator.type] VARCHAR(256),
    [regulator.identifier.id] NVARCHAR(100),
    [regulator.identifier.extension] NVARCHAR(MAX),
    [regulator.identifier.use] NVARCHAR(64),
    [regulator.identifier.type] NVARCHAR(MAX),
    [regulator.identifier.system] VARCHAR(256),
    [regulator.identifier.value] NVARCHAR(4000),
    [regulator.identifier.period] NVARCHAR(MAX),
    [regulator.identifier.assigner] NVARCHAR(MAX),
    [regulator.display] NVARCHAR(4000),
    [attachedDocument] VARCHAR(MAX),
    [case.id] NVARCHAR(100),
    [case.extension] NVARCHAR(MAX),
    [case.modifierExtension] NVARCHAR(MAX),
    [case.identifier.id] NVARCHAR(100),
    [case.identifier.extension] NVARCHAR(MAX),
    [case.identifier.use] NVARCHAR(64),
    [case.identifier.type] NVARCHAR(MAX),
    [case.identifier.system] VARCHAR(256),
    [case.identifier.value] NVARCHAR(4000),
    [case.identifier.period] NVARCHAR(MAX),
    [case.identifier.assigner] NVARCHAR(MAX),
    [case.type.id] NVARCHAR(100),
    [case.type.extension] NVARCHAR(MAX),
    [case.type.coding] NVARCHAR(MAX),
    [case.type.text] NVARCHAR(4000),
    [case.status.id] NVARCHAR(100),
    [case.status.extension] NVARCHAR(MAX),
    [case.status.coding] NVARCHAR(MAX),
    [case.status.text] NVARCHAR(4000),
    [case.application] VARCHAR(MAX),
    [case.date.period.id] NVARCHAR(100),
    [case.date.period.extension] NVARCHAR(MAX),
    [case.date.period.start] VARCHAR(64),
    [case.date.period.end] VARCHAR(64),
    [case.date.dateTime] VARCHAR(64),
) WITH (
    LOCATION='/RegulatedAuthorization/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.RegulatedAuthorizationIdentifier AS
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
        BULK 'RegulatedAuthorization/**',
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

CREATE VIEW fhir.RegulatedAuthorizationSubject AS
SELECT
    [id],
    [subject.JSON],
    [subject.id],
    [subject.extension],
    [subject.reference],
    [subject.type],
    [subject.identifier.id],
    [subject.identifier.extension],
    [subject.identifier.use],
    [subject.identifier.type],
    [subject.identifier.system],
    [subject.identifier.value],
    [subject.identifier.period],
    [subject.identifier.assigner],
    [subject.display]
FROM openrowset (
        BULK 'RegulatedAuthorization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subject.JSON]  VARCHAR(MAX) '$.subject'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subject.JSON]) with (
        [subject.id]                   NVARCHAR(100)       '$.id',
        [subject.extension]            NVARCHAR(MAX)       '$.extension',
        [subject.reference]            NVARCHAR(4000)      '$.reference',
        [subject.type]                 VARCHAR(256)        '$.type',
        [subject.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [subject.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [subject.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [subject.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [subject.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [subject.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [subject.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [subject.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [subject.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.RegulatedAuthorizationRegion AS
SELECT
    [id],
    [region.JSON],
    [region.id],
    [region.extension],
    [region.coding],
    [region.text]
FROM openrowset (
        BULK 'RegulatedAuthorization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [region.JSON]  VARCHAR(MAX) '$.region'
    ) AS rowset
    CROSS APPLY openjson (rowset.[region.JSON]) with (
        [region.id]                    NVARCHAR(100)       '$.id',
        [region.extension]             NVARCHAR(MAX)       '$.extension',
        [region.coding]                NVARCHAR(MAX)       '$.coding' AS JSON,
        [region.text]                  NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.RegulatedAuthorizationBasis AS
SELECT
    [id],
    [basis.JSON],
    [basis.id],
    [basis.extension],
    [basis.coding],
    [basis.text]
FROM openrowset (
        BULK 'RegulatedAuthorization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [basis.JSON]  VARCHAR(MAX) '$.basis'
    ) AS rowset
    CROSS APPLY openjson (rowset.[basis.JSON]) with (
        [basis.id]                     NVARCHAR(100)       '$.id',
        [basis.extension]              NVARCHAR(MAX)       '$.extension',
        [basis.coding]                 NVARCHAR(MAX)       '$.coding' AS JSON,
        [basis.text]                   NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.RegulatedAuthorizationAttachedDocument AS
SELECT
    [id],
    [attachedDocument.JSON],
    [attachedDocument.id],
    [attachedDocument.extension],
    [attachedDocument.reference],
    [attachedDocument.type],
    [attachedDocument.identifier.id],
    [attachedDocument.identifier.extension],
    [attachedDocument.identifier.use],
    [attachedDocument.identifier.type],
    [attachedDocument.identifier.system],
    [attachedDocument.identifier.value],
    [attachedDocument.identifier.period],
    [attachedDocument.identifier.assigner],
    [attachedDocument.display]
FROM openrowset (
        BULK 'RegulatedAuthorization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [attachedDocument.JSON]  VARCHAR(MAX) '$.attachedDocument'
    ) AS rowset
    CROSS APPLY openjson (rowset.[attachedDocument.JSON]) with (
        [attachedDocument.id]          NVARCHAR(100)       '$.id',
        [attachedDocument.extension]   NVARCHAR(MAX)       '$.extension',
        [attachedDocument.reference]   NVARCHAR(4000)      '$.reference',
        [attachedDocument.type]        VARCHAR(256)        '$.type',
        [attachedDocument.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [attachedDocument.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [attachedDocument.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [attachedDocument.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [attachedDocument.identifier.system] VARCHAR(256)        '$.identifier.system',
        [attachedDocument.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [attachedDocument.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [attachedDocument.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [attachedDocument.display]     NVARCHAR(4000)      '$.display'
    ) j
