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
    [identifier] VARCHAR(MAX),
    [basedOn] VARCHAR(MAX),
    [status] NVARCHAR(100),
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
    [encounter] VARCHAR(MAX),
    [event] VARCHAR(MAX),
    [facilityType.id] NVARCHAR(100),
    [facilityType.extension] NVARCHAR(MAX),
    [facilityType.coding] VARCHAR(MAX),
    [facilityType.text] NVARCHAR(4000),
    [practiceSetting.id] NVARCHAR(100),
    [practiceSetting.extension] NVARCHAR(MAX),
    [practiceSetting.coding] VARCHAR(MAX),
    [practiceSetting.text] NVARCHAR(4000),
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [date] VARCHAR(64),
    [author] VARCHAR(MAX),
    [attester] VARCHAR(MAX),
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
    [description] NVARCHAR(MAX),
    [securityLabel] VARCHAR(MAX),
    [content] VARCHAR(MAX),
    [sourcePatientInfo.id] NVARCHAR(100),
    [sourcePatientInfo.extension] NVARCHAR(MAX),
    [sourcePatientInfo.reference] NVARCHAR(4000),
    [sourcePatientInfo.type] VARCHAR(256),
    [sourcePatientInfo.identifier.id] NVARCHAR(100),
    [sourcePatientInfo.identifier.extension] NVARCHAR(MAX),
    [sourcePatientInfo.identifier.use] NVARCHAR(64),
    [sourcePatientInfo.identifier.type] NVARCHAR(MAX),
    [sourcePatientInfo.identifier.system] VARCHAR(256),
    [sourcePatientInfo.identifier.value] NVARCHAR(4000),
    [sourcePatientInfo.identifier.period] NVARCHAR(MAX),
    [sourcePatientInfo.identifier.assigner] NVARCHAR(MAX),
    [sourcePatientInfo.display] NVARCHAR(4000),
    [related] VARCHAR(MAX),
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

CREATE VIEW fhir.DocumentReferenceBasedOn AS
SELECT
    [id],
    [basedOn.JSON],
    [basedOn.id],
    [basedOn.extension],
    [basedOn.reference],
    [basedOn.type],
    [basedOn.identifier.id],
    [basedOn.identifier.extension],
    [basedOn.identifier.use],
    [basedOn.identifier.type],
    [basedOn.identifier.system],
    [basedOn.identifier.value],
    [basedOn.identifier.period],
    [basedOn.identifier.assigner],
    [basedOn.display]
FROM openrowset (
        BULK 'DocumentReference/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [basedOn.JSON]  VARCHAR(MAX) '$.basedOn'
    ) AS rowset
    CROSS APPLY openjson (rowset.[basedOn.JSON]) with (
        [basedOn.id]                   NVARCHAR(100)       '$.id',
        [basedOn.extension]            NVARCHAR(MAX)       '$.extension',
        [basedOn.reference]            NVARCHAR(4000)      '$.reference',
        [basedOn.type]                 VARCHAR(256)        '$.type',
        [basedOn.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [basedOn.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [basedOn.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [basedOn.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [basedOn.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [basedOn.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [basedOn.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [basedOn.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [basedOn.display]              NVARCHAR(4000)      '$.display'
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

CREATE VIEW fhir.DocumentReferenceEncounter AS
SELECT
    [id],
    [encounter.JSON],
    [encounter.id],
    [encounter.extension],
    [encounter.reference],
    [encounter.type],
    [encounter.identifier.id],
    [encounter.identifier.extension],
    [encounter.identifier.use],
    [encounter.identifier.type],
    [encounter.identifier.system],
    [encounter.identifier.value],
    [encounter.identifier.period],
    [encounter.identifier.assigner],
    [encounter.display]
FROM openrowset (
        BULK 'DocumentReference/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [encounter.JSON]  VARCHAR(MAX) '$.encounter'
    ) AS rowset
    CROSS APPLY openjson (rowset.[encounter.JSON]) with (
        [encounter.id]                 NVARCHAR(100)       '$.id',
        [encounter.extension]          NVARCHAR(MAX)       '$.extension',
        [encounter.reference]          NVARCHAR(4000)      '$.reference',
        [encounter.type]               VARCHAR(256)        '$.type',
        [encounter.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [encounter.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [encounter.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [encounter.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [encounter.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [encounter.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [encounter.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [encounter.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [encounter.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.DocumentReferenceEvent AS
SELECT
    [id],
    [event.JSON],
    [event.id],
    [event.extension],
    [event.coding],
    [event.text]
FROM openrowset (
        BULK 'DocumentReference/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [event.JSON]  VARCHAR(MAX) '$.event'
    ) AS rowset
    CROSS APPLY openjson (rowset.[event.JSON]) with (
        [event.id]                     NVARCHAR(100)       '$.id',
        [event.extension]              NVARCHAR(MAX)       '$.extension',
        [event.coding]                 NVARCHAR(MAX)       '$.coding' AS JSON,
        [event.text]                   NVARCHAR(4000)      '$.text'
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

CREATE VIEW fhir.DocumentReferenceAttester AS
SELECT
    [id],
    [attester.JSON],
    [attester.id],
    [attester.extension],
    [attester.modifierExtension],
    [attester.mode],
    [attester.time],
    [attester.party.id],
    [attester.party.extension],
    [attester.party.reference],
    [attester.party.type],
    [attester.party.identifier],
    [attester.party.display]
FROM openrowset (
        BULK 'DocumentReference/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [attester.JSON]  VARCHAR(MAX) '$.attester'
    ) AS rowset
    CROSS APPLY openjson (rowset.[attester.JSON]) with (
        [attester.id]                  NVARCHAR(100)       '$.id',
        [attester.extension]           NVARCHAR(MAX)       '$.extension',
        [attester.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [attester.mode]                NVARCHAR(100)       '$.mode',
        [attester.time]                VARCHAR(64)         '$.time',
        [attester.party.id]            NVARCHAR(100)       '$.party.id',
        [attester.party.extension]     NVARCHAR(MAX)       '$.party.extension',
        [attester.party.reference]     NVARCHAR(4000)      '$.party.reference',
        [attester.party.type]          VARCHAR(256)        '$.party.type',
        [attester.party.identifier]    NVARCHAR(MAX)       '$.party.identifier',
        [attester.party.display]       NVARCHAR(4000)      '$.party.display'
    ) j

GO

CREATE VIEW fhir.DocumentReferenceRelatesTo AS
SELECT
    [id],
    [relatesTo.JSON],
    [relatesTo.id],
    [relatesTo.extension],
    [relatesTo.modifierExtension],
    [relatesTo.code.id],
    [relatesTo.code.extension],
    [relatesTo.code.coding],
    [relatesTo.code.text],
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
        [relatesTo.code.id]            NVARCHAR(100)       '$.code.id',
        [relatesTo.code.extension]     NVARCHAR(MAX)       '$.code.extension',
        [relatesTo.code.coding]        NVARCHAR(MAX)       '$.code.coding',
        [relatesTo.code.text]          NVARCHAR(4000)      '$.code.text',
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
    [content.attachment.height],
    [content.attachment.width],
    [content.attachment.frames],
    [content.attachment.duration],
    [content.attachment.pages],
    [content.format.id],
    [content.format.extension],
    [content.format.system],
    [content.format.version],
    [content.format.code],
    [content.format.display],
    [content.format.userSelected],
    [content.identifier.id],
    [content.identifier.extension],
    [content.identifier.use],
    [content.identifier.type],
    [content.identifier.system],
    [content.identifier.value],
    [content.identifier.period],
    [content.identifier.assigner]
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
        [content.attachment.size]      NVARCHAR(MAX)       '$.attachment.size',
        [content.attachment.hash]      NVARCHAR(MAX)       '$.attachment.hash',
        [content.attachment.title]     NVARCHAR(4000)      '$.attachment.title',
        [content.attachment.creation]  VARCHAR(64)         '$.attachment.creation',
        [content.attachment.height]    bigint              '$.attachment.height',
        [content.attachment.width]     bigint              '$.attachment.width',
        [content.attachment.frames]    bigint              '$.attachment.frames',
        [content.attachment.duration]  float               '$.attachment.duration',
        [content.attachment.pages]     bigint              '$.attachment.pages',
        [content.format.id]            NVARCHAR(100)       '$.format.id',
        [content.format.extension]     NVARCHAR(MAX)       '$.format.extension',
        [content.format.system]        VARCHAR(256)        '$.format.system',
        [content.format.version]       NVARCHAR(100)       '$.format.version',
        [content.format.code]          NVARCHAR(4000)      '$.format.code',
        [content.format.display]       NVARCHAR(4000)      '$.format.display',
        [content.format.userSelected]  bit                 '$.format.userSelected',
        [content.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [content.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [content.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [content.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [content.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [content.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [content.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [content.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner'
    ) j

GO

CREATE VIEW fhir.DocumentReferenceRelated AS
SELECT
    [id],
    [related.JSON],
    [related.id],
    [related.extension],
    [related.reference],
    [related.type],
    [related.identifier.id],
    [related.identifier.extension],
    [related.identifier.use],
    [related.identifier.type],
    [related.identifier.system],
    [related.identifier.value],
    [related.identifier.period],
    [related.identifier.assigner],
    [related.display]
FROM openrowset (
        BULK 'DocumentReference/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [related.JSON]  VARCHAR(MAX) '$.related'
    ) AS rowset
    CROSS APPLY openjson (rowset.[related.JSON]) with (
        [related.id]                   NVARCHAR(100)       '$.id',
        [related.extension]            NVARCHAR(MAX)       '$.extension',
        [related.reference]            NVARCHAR(4000)      '$.reference',
        [related.type]                 VARCHAR(256)        '$.type',
        [related.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [related.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [related.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [related.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [related.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [related.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [related.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [related.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [related.display]              NVARCHAR(4000)      '$.display'
    ) j
