CREATE EXTERNAL TABLE [fhir].[DocumentReference] (
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
    [masterIdentifier.id] NVARCHAR(100),
    [masterIdentifier.extension] NVARCHAR(MAX),
    [masterIdentifier.use] NVARCHAR(64),
    [masterIdentifier.type.id] NVARCHAR(100),
    [masterIdentifier.type.extension] NVARCHAR(MAX),
    [masterIdentifier.type.coding] NVARCHAR(MAX),
    [masterIdentifier.type.text] NVARCHAR(4000),
    [masterIdentifier.system] VARCHAR(256),
    [masterIdentifier.value] NVARCHAR(4000),
    [masterIdentifier.period.id] NVARCHAR(100),
    [masterIdentifier.period.extension] NVARCHAR(MAX),
    [masterIdentifier.period.start] VARCHAR(64),
    [masterIdentifier.period.end] VARCHAR(64),
    [masterIdentifier.assigner.id] NVARCHAR(100),
    [masterIdentifier.assigner.extension] NVARCHAR(MAX),
    [masterIdentifier.assigner.reference] NVARCHAR(4000),
    [masterIdentifier.assigner.type] VARCHAR(256),
    [masterIdentifier.assigner.identifier] NVARCHAR(MAX),
    [masterIdentifier.assigner.display] NVARCHAR(4000),
    [identifier] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [docStatus] NVARCHAR(100),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [category] VARCHAR(MAX),
    [subject.id] NVARCHAR(100),
    [subject.extension] NVARCHAR(MAX),
    [subject.reference] NVARCHAR(4000),
    [subject.type] VARCHAR(256),
    [subject.identifier.id] NVARCHAR(100),
    [subject.identifier.extension] NVARCHAR(MAX),
    [subject.identifier.use] NVARCHAR(64),
    [subject.identifier.type] NVARCHAR(MAX),
    [subject.identifier.system] VARCHAR(256),
    [subject.identifier.value] NVARCHAR(4000),
    [subject.identifier.period] NVARCHAR(MAX),
    [subject.identifier.assigner] NVARCHAR(MAX),
    [subject.display] NVARCHAR(4000),
    [date] VARCHAR(64),
    [author] VARCHAR(MAX),
    [authenticator.id] NVARCHAR(100),
    [authenticator.extension] NVARCHAR(MAX),
    [authenticator.reference] NVARCHAR(4000),
    [authenticator.type] VARCHAR(256),
    [authenticator.identifier.id] NVARCHAR(100),
    [authenticator.identifier.extension] NVARCHAR(MAX),
    [authenticator.identifier.use] NVARCHAR(64),
    [authenticator.identifier.type] NVARCHAR(MAX),
    [authenticator.identifier.system] VARCHAR(256),
    [authenticator.identifier.value] NVARCHAR(4000),
    [authenticator.identifier.period] NVARCHAR(MAX),
    [authenticator.identifier.assigner] NVARCHAR(MAX),
    [authenticator.display] NVARCHAR(4000),
    [custodian.id] NVARCHAR(100),
    [custodian.extension] NVARCHAR(MAX),
    [custodian.reference] NVARCHAR(4000),
    [custodian.type] VARCHAR(256),
    [custodian.identifier.id] NVARCHAR(100),
    [custodian.identifier.extension] NVARCHAR(MAX),
    [custodian.identifier.use] NVARCHAR(64),
    [custodian.identifier.type] NVARCHAR(MAX),
    [custodian.identifier.system] VARCHAR(256),
    [custodian.identifier.value] NVARCHAR(4000),
    [custodian.identifier.period] NVARCHAR(MAX),
    [custodian.identifier.assigner] NVARCHAR(MAX),
    [custodian.display] NVARCHAR(4000),
    [relatesTo] VARCHAR(MAX),
    [description] NVARCHAR(4000),
    [securityLabel] VARCHAR(MAX),
    [content] VARCHAR(MAX),
    [context.id] NVARCHAR(100),
    [context.extension] NVARCHAR(MAX),
    [context.modifierExtension] NVARCHAR(MAX),
    [context.encounter] VARCHAR(MAX),
    [context.event] VARCHAR(MAX),
    [context.period.id] NVARCHAR(100),
    [context.period.extension] NVARCHAR(MAX),
    [context.period.start] VARCHAR(64),
    [context.period.end] VARCHAR(64),
    [context.facilityType.id] NVARCHAR(100),
    [context.facilityType.extension] NVARCHAR(MAX),
    [context.facilityType.coding] NVARCHAR(MAX),
    [context.facilityType.text] NVARCHAR(4000),
    [context.practiceSetting.id] NVARCHAR(100),
    [context.practiceSetting.extension] NVARCHAR(MAX),
    [context.practiceSetting.coding] NVARCHAR(MAX),
    [context.practiceSetting.text] NVARCHAR(4000),
    [context.sourcePatientInfo.id] NVARCHAR(100),
    [context.sourcePatientInfo.extension] NVARCHAR(MAX),
    [context.sourcePatientInfo.reference] NVARCHAR(4000),
    [context.sourcePatientInfo.type] VARCHAR(256),
    [context.sourcePatientInfo.identifier] NVARCHAR(MAX),
    [context.sourcePatientInfo.display] NVARCHAR(4000),
    [context.related] VARCHAR(MAX),
) WITH (
    LOCATION='/DocumentReference/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.DocumentReferenceIdentifier AS
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
        BULK 'DocumentReference/**',
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

CREATE VIEW fhir.DocumentReferenceCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'DocumentReference/**',
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

CREATE VIEW fhir.DocumentReferenceAuthor AS
SELECT
    [id],
    [author.JSON],
    [author.id],
    [author.extension],
    [author.reference],
    [author.type],
    [author.identifier.id],
    [author.identifier.extension],
    [author.identifier.use],
    [author.identifier.type],
    [author.identifier.system],
    [author.identifier.value],
    [author.identifier.period],
    [author.identifier.assigner],
    [author.display]
FROM openrowset (
        BULK 'DocumentReference/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [author.JSON]  VARCHAR(MAX) '$.author'
    ) AS rowset
    CROSS APPLY openjson (rowset.[author.JSON]) with (
        [author.id]                    NVARCHAR(100)       '$.id',
        [author.extension]             NVARCHAR(MAX)       '$.extension',
        [author.reference]             NVARCHAR(4000)      '$.reference',
        [author.type]                  VARCHAR(256)        '$.type',
        [author.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [author.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [author.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [author.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [author.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [author.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [author.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [author.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [author.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DocumentReferenceRelatesTo AS
SELECT
    [id],
    [relatesTo.JSON],
    [relatesTo.id],
    [relatesTo.extension],
    [relatesTo.modifierExtension],
    [relatesTo.code],
    [relatesTo.target.id],
    [relatesTo.target.extension],
    [relatesTo.target.reference],
    [relatesTo.target.type],
    [relatesTo.target.identifier],
    [relatesTo.target.display]
FROM openrowset (
        BULK 'DocumentReference/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [relatesTo.JSON]  VARCHAR(MAX) '$.relatesTo'
    ) AS rowset
    CROSS APPLY openjson (rowset.[relatesTo.JSON]) with (
        [relatesTo.id]                 NVARCHAR(100)       '$.id',
        [relatesTo.extension]          NVARCHAR(MAX)       '$.extension',
        [relatesTo.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [relatesTo.code]               NVARCHAR(64)        '$.code',
        [relatesTo.target.id]          NVARCHAR(100)       '$.target.id',
        [relatesTo.target.extension]   NVARCHAR(MAX)       '$.target.extension',
        [relatesTo.target.reference]   NVARCHAR(4000)      '$.target.reference',
        [relatesTo.target.type]        VARCHAR(256)        '$.target.type',
        [relatesTo.target.identifier]  NVARCHAR(MAX)       '$.target.identifier',
        [relatesTo.target.display]     NVARCHAR(4000)      '$.target.display'
    ) j

GO

CREATE VIEW fhir.DocumentReferenceSecurityLabel AS
SELECT
    [id],
    [securityLabel.JSON],
    [securityLabel.id],
    [securityLabel.extension],
    [securityLabel.coding],
    [securityLabel.text]
FROM openrowset (
        BULK 'DocumentReference/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [securityLabel.JSON]  VARCHAR(MAX) '$.securityLabel'
    ) AS rowset
    CROSS APPLY openjson (rowset.[securityLabel.JSON]) with (
        [securityLabel.id]             NVARCHAR(100)       '$.id',
        [securityLabel.extension]      NVARCHAR(MAX)       '$.extension',
        [securityLabel.coding]         NVARCHAR(MAX)       '$.coding' AS JSON,
        [securityLabel.text]           NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.DocumentReferenceContent AS
SELECT
    [id],
    [content.JSON],
    [content.id],
    [content.extension],
    [content.modifierExtension],
    [content.attachment.id],
    [content.attachment.extension],
    [content.attachment.contentType],
    [content.attachment.language],
    [content.attachment.data],
    [content.attachment.url],
    [content.attachment.size],
    [content.attachment.hash],
    [content.attachment.title],
    [content.attachment.creation],
    [content.format.id],
    [content.format.extension],
    [content.format.system],
    [content.format.version],
    [content.format.code],
    [content.format.display],
    [content.format.userSelected]
FROM openrowset (
        BULK 'DocumentReference/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [content.JSON]  VARCHAR(MAX) '$.content'
    ) AS rowset
    CROSS APPLY openjson (rowset.[content.JSON]) with (
        [content.id]                   NVARCHAR(100)       '$.id',
        [content.extension]            NVARCHAR(MAX)       '$.extension',
        [content.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [content.attachment.id]        NVARCHAR(100)       '$.attachment.id',
        [content.attachment.extension] NVARCHAR(MAX)       '$.attachment.extension',
        [content.attachment.contentType] NVARCHAR(100)       '$.attachment.contentType',
        [content.attachment.language]  NVARCHAR(100)       '$.attachment.language',
        [content.attachment.data]      NVARCHAR(MAX)       '$.attachment.data',
        [content.attachment.url]       VARCHAR(256)        '$.attachment.url',
        [content.attachment.size]      bigint              '$.attachment.size',
        [content.attachment.hash]      NVARCHAR(MAX)       '$.attachment.hash',
        [content.attachment.title]     NVARCHAR(4000)      '$.attachment.title',
        [content.attachment.creation]  VARCHAR(64)         '$.attachment.creation',
        [content.format.id]            NVARCHAR(100)       '$.format.id',
        [content.format.extension]     NVARCHAR(MAX)       '$.format.extension',
        [content.format.system]        VARCHAR(256)        '$.format.system',
        [content.format.version]       NVARCHAR(100)       '$.format.version',
        [content.format.code]          NVARCHAR(4000)      '$.format.code',
        [content.format.display]       NVARCHAR(4000)      '$.format.display',
        [content.format.userSelected]  bit                 '$.format.userSelected'
    ) j
