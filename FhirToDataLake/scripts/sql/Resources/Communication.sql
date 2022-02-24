CREATE EXTERNAL TABLE [fhir].[Communication] (
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
    [instantiatesCanonical] VARCHAR(MAX),
    [instantiatesUri] VARCHAR(MAX),
    [basedOn] VARCHAR(MAX),
    [partOf] VARCHAR(MAX),
    [inResponseTo] VARCHAR(MAX),
    [status] NVARCHAR(100),
    [statusReason.id] NVARCHAR(100),
    [statusReason.extension] NVARCHAR(MAX),
    [statusReason.coding] VARCHAR(MAX),
    [statusReason.text] NVARCHAR(4000),
    [category] VARCHAR(MAX),
    [priority] NVARCHAR(100),
    [medium] VARCHAR(MAX),
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
    [topic.id] NVARCHAR(100),
    [topic.extension] NVARCHAR(MAX),
    [topic.coding] VARCHAR(MAX),
    [topic.text] NVARCHAR(4000),
    [about] VARCHAR(MAX),
    [encounter.id] NVARCHAR(100),
    [encounter.extension] NVARCHAR(MAX),
    [encounter.reference] NVARCHAR(4000),
    [encounter.type] VARCHAR(256),
    [encounter.identifier.id] NVARCHAR(100),
    [encounter.identifier.extension] NVARCHAR(MAX),
    [encounter.identifier.use] NVARCHAR(64),
    [encounter.identifier.type] NVARCHAR(MAX),
    [encounter.identifier.system] VARCHAR(256),
    [encounter.identifier.value] NVARCHAR(4000),
    [encounter.identifier.period] NVARCHAR(MAX),
    [encounter.identifier.assigner] NVARCHAR(MAX),
    [encounter.display] NVARCHAR(4000),
    [sent] VARCHAR(64),
    [received] VARCHAR(64),
    [recipient] VARCHAR(MAX),
    [sender.id] NVARCHAR(100),
    [sender.extension] NVARCHAR(MAX),
    [sender.reference] NVARCHAR(4000),
    [sender.type] VARCHAR(256),
    [sender.identifier.id] NVARCHAR(100),
    [sender.identifier.extension] NVARCHAR(MAX),
    [sender.identifier.use] NVARCHAR(64),
    [sender.identifier.type] NVARCHAR(MAX),
    [sender.identifier.system] VARCHAR(256),
    [sender.identifier.value] NVARCHAR(4000),
    [sender.identifier.period] NVARCHAR(MAX),
    [sender.identifier.assigner] NVARCHAR(MAX),
    [sender.display] NVARCHAR(4000),
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [payload] VARCHAR(MAX),
    [note] VARCHAR(MAX),
) WITH (
    LOCATION='/Communication/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.CommunicationIdentifier AS
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
        BULK 'Communication/**',
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

CREATE VIEW fhir.CommunicationInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'Communication/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesCanonical.JSON]  VARCHAR(MAX) '$.instantiatesCanonical'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesCanonical.JSON]) with (
        [instantiatesCanonical]        NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.CommunicationInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'Communication/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesUri.JSON]  VARCHAR(MAX) '$.instantiatesUri'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesUri.JSON]) with (
        [instantiatesUri]              NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.CommunicationBasedOn AS
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
        BULK 'Communication/**',
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

CREATE VIEW fhir.CommunicationPartOf AS
SELECT
    [id],
    [partOf.JSON],
    [partOf.id],
    [partOf.extension],
    [partOf.reference],
    [partOf.type],
    [partOf.identifier.id],
    [partOf.identifier.extension],
    [partOf.identifier.use],
    [partOf.identifier.type],
    [partOf.identifier.system],
    [partOf.identifier.value],
    [partOf.identifier.period],
    [partOf.identifier.assigner],
    [partOf.display]
FROM openrowset (
        BULK 'Communication/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [partOf.JSON]  VARCHAR(MAX) '$.partOf'
    ) AS rowset
    CROSS APPLY openjson (rowset.[partOf.JSON]) with (
        [partOf.id]                    NVARCHAR(100)       '$.id',
        [partOf.extension]             NVARCHAR(MAX)       '$.extension',
        [partOf.reference]             NVARCHAR(4000)      '$.reference',
        [partOf.type]                  VARCHAR(256)        '$.type',
        [partOf.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [partOf.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [partOf.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [partOf.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [partOf.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [partOf.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [partOf.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [partOf.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [partOf.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CommunicationInResponseTo AS
SELECT
    [id],
    [inResponseTo.JSON],
    [inResponseTo.id],
    [inResponseTo.extension],
    [inResponseTo.reference],
    [inResponseTo.type],
    [inResponseTo.identifier.id],
    [inResponseTo.identifier.extension],
    [inResponseTo.identifier.use],
    [inResponseTo.identifier.type],
    [inResponseTo.identifier.system],
    [inResponseTo.identifier.value],
    [inResponseTo.identifier.period],
    [inResponseTo.identifier.assigner],
    [inResponseTo.display]
FROM openrowset (
        BULK 'Communication/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [inResponseTo.JSON]  VARCHAR(MAX) '$.inResponseTo'
    ) AS rowset
    CROSS APPLY openjson (rowset.[inResponseTo.JSON]) with (
        [inResponseTo.id]              NVARCHAR(100)       '$.id',
        [inResponseTo.extension]       NVARCHAR(MAX)       '$.extension',
        [inResponseTo.reference]       NVARCHAR(4000)      '$.reference',
        [inResponseTo.type]            VARCHAR(256)        '$.type',
        [inResponseTo.identifier.id]   NVARCHAR(100)       '$.identifier.id',
        [inResponseTo.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [inResponseTo.identifier.use]  NVARCHAR(64)        '$.identifier.use',
        [inResponseTo.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [inResponseTo.identifier.system] VARCHAR(256)        '$.identifier.system',
        [inResponseTo.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [inResponseTo.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [inResponseTo.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [inResponseTo.display]         NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CommunicationCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'Communication/**',
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

CREATE VIEW fhir.CommunicationMedium AS
SELECT
    [id],
    [medium.JSON],
    [medium.id],
    [medium.extension],
    [medium.coding],
    [medium.text]
FROM openrowset (
        BULK 'Communication/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [medium.JSON]  VARCHAR(MAX) '$.medium'
    ) AS rowset
    CROSS APPLY openjson (rowset.[medium.JSON]) with (
        [medium.id]                    NVARCHAR(100)       '$.id',
        [medium.extension]             NVARCHAR(MAX)       '$.extension',
        [medium.coding]                NVARCHAR(MAX)       '$.coding' AS JSON,
        [medium.text]                  NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.CommunicationAbout AS
SELECT
    [id],
    [about.JSON],
    [about.id],
    [about.extension],
    [about.reference],
    [about.type],
    [about.identifier.id],
    [about.identifier.extension],
    [about.identifier.use],
    [about.identifier.type],
    [about.identifier.system],
    [about.identifier.value],
    [about.identifier.period],
    [about.identifier.assigner],
    [about.display]
FROM openrowset (
        BULK 'Communication/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [about.JSON]  VARCHAR(MAX) '$.about'
    ) AS rowset
    CROSS APPLY openjson (rowset.[about.JSON]) with (
        [about.id]                     NVARCHAR(100)       '$.id',
        [about.extension]              NVARCHAR(MAX)       '$.extension',
        [about.reference]              NVARCHAR(4000)      '$.reference',
        [about.type]                   VARCHAR(256)        '$.type',
        [about.identifier.id]          NVARCHAR(100)       '$.identifier.id',
        [about.identifier.extension]   NVARCHAR(MAX)       '$.identifier.extension',
        [about.identifier.use]         NVARCHAR(64)        '$.identifier.use',
        [about.identifier.type]        NVARCHAR(MAX)       '$.identifier.type',
        [about.identifier.system]      VARCHAR(256)        '$.identifier.system',
        [about.identifier.value]       NVARCHAR(4000)      '$.identifier.value',
        [about.identifier.period]      NVARCHAR(MAX)       '$.identifier.period',
        [about.identifier.assigner]    NVARCHAR(MAX)       '$.identifier.assigner',
        [about.display]                NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CommunicationRecipient AS
SELECT
    [id],
    [recipient.JSON],
    [recipient.id],
    [recipient.extension],
    [recipient.reference],
    [recipient.type],
    [recipient.identifier.id],
    [recipient.identifier.extension],
    [recipient.identifier.use],
    [recipient.identifier.type],
    [recipient.identifier.system],
    [recipient.identifier.value],
    [recipient.identifier.period],
    [recipient.identifier.assigner],
    [recipient.display]
FROM openrowset (
        BULK 'Communication/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [recipient.JSON]  VARCHAR(MAX) '$.recipient'
    ) AS rowset
    CROSS APPLY openjson (rowset.[recipient.JSON]) with (
        [recipient.id]                 NVARCHAR(100)       '$.id',
        [recipient.extension]          NVARCHAR(MAX)       '$.extension',
        [recipient.reference]          NVARCHAR(4000)      '$.reference',
        [recipient.type]               VARCHAR(256)        '$.type',
        [recipient.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [recipient.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [recipient.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [recipient.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [recipient.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [recipient.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [recipient.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [recipient.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [recipient.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CommunicationReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'Communication/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonCode.JSON]  VARCHAR(MAX) '$.reasonCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonCode.JSON]) with (
        [reasonCode.id]                NVARCHAR(100)       '$.id',
        [reasonCode.extension]         NVARCHAR(MAX)       '$.extension',
        [reasonCode.coding]            NVARCHAR(MAX)       '$.coding' AS JSON,
        [reasonCode.text]              NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.CommunicationReasonReference AS
SELECT
    [id],
    [reasonReference.JSON],
    [reasonReference.id],
    [reasonReference.extension],
    [reasonReference.reference],
    [reasonReference.type],
    [reasonReference.identifier.id],
    [reasonReference.identifier.extension],
    [reasonReference.identifier.use],
    [reasonReference.identifier.type],
    [reasonReference.identifier.system],
    [reasonReference.identifier.value],
    [reasonReference.identifier.period],
    [reasonReference.identifier.assigner],
    [reasonReference.display]
FROM openrowset (
        BULK 'Communication/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonReference.JSON]  VARCHAR(MAX) '$.reasonReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonReference.JSON]) with (
        [reasonReference.id]           NVARCHAR(100)       '$.id',
        [reasonReference.extension]    NVARCHAR(MAX)       '$.extension',
        [reasonReference.reference]    NVARCHAR(4000)      '$.reference',
        [reasonReference.type]         VARCHAR(256)        '$.type',
        [reasonReference.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [reasonReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [reasonReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [reasonReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [reasonReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [reasonReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [reasonReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [reasonReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [reasonReference.display]      NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CommunicationPayload AS
SELECT
    [id],
    [payload.JSON],
    [payload.id],
    [payload.extension],
    [payload.modifierExtension],
    [payload.content.string],
    [payload.content.attachment.id],
    [payload.content.attachment.extension],
    [payload.content.attachment.contentType],
    [payload.content.attachment.language],
    [payload.content.attachment.data],
    [payload.content.attachment.url],
    [payload.content.attachment.size],
    [payload.content.attachment.hash],
    [payload.content.attachment.title],
    [payload.content.attachment.creation],
    [payload.content.reference.id],
    [payload.content.reference.extension],
    [payload.content.reference.reference],
    [payload.content.reference.type],
    [payload.content.reference.identifier],
    [payload.content.reference.display]
FROM openrowset (
        BULK 'Communication/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [payload.JSON]  VARCHAR(MAX) '$.payload'
    ) AS rowset
    CROSS APPLY openjson (rowset.[payload.JSON]) with (
        [payload.id]                   NVARCHAR(100)       '$.id',
        [payload.extension]            NVARCHAR(MAX)       '$.extension',
        [payload.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [payload.content.string]       NVARCHAR(4000)      '$.content.string',
        [payload.content.attachment.id] NVARCHAR(100)       '$.content.attachment.id',
        [payload.content.attachment.extension] NVARCHAR(MAX)       '$.content.attachment.extension',
        [payload.content.attachment.contentType] NVARCHAR(100)       '$.content.attachment.contentType',
        [payload.content.attachment.language] NVARCHAR(100)       '$.content.attachment.language',
        [payload.content.attachment.data] NVARCHAR(MAX)       '$.content.attachment.data',
        [payload.content.attachment.url] VARCHAR(256)        '$.content.attachment.url',
        [payload.content.attachment.size] bigint              '$.content.attachment.size',
        [payload.content.attachment.hash] NVARCHAR(MAX)       '$.content.attachment.hash',
        [payload.content.attachment.title] NVARCHAR(4000)      '$.content.attachment.title',
        [payload.content.attachment.creation] VARCHAR(64)         '$.content.attachment.creation',
        [payload.content.reference.id] NVARCHAR(100)       '$.content.reference.id',
        [payload.content.reference.extension] NVARCHAR(MAX)       '$.content.reference.extension',
        [payload.content.reference.reference] NVARCHAR(4000)      '$.content.reference.reference',
        [payload.content.reference.type] VARCHAR(256)        '$.content.reference.type',
        [payload.content.reference.identifier] NVARCHAR(MAX)       '$.content.reference.identifier',
        [payload.content.reference.display] NVARCHAR(4000)      '$.content.reference.display'
    ) j

GO

CREATE VIEW fhir.CommunicationNote AS
SELECT
    [id],
    [note.JSON],
    [note.id],
    [note.extension],
    [note.time],
    [note.text],
    [note.author.reference.id],
    [note.author.reference.extension],
    [note.author.reference.reference],
    [note.author.reference.type],
    [note.author.reference.identifier],
    [note.author.reference.display],
    [note.author.string]
FROM openrowset (
        BULK 'Communication/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [note.JSON]  VARCHAR(MAX) '$.note'
    ) AS rowset
    CROSS APPLY openjson (rowset.[note.JSON]) with (
        [note.id]                      NVARCHAR(100)       '$.id',
        [note.extension]               NVARCHAR(MAX)       '$.extension',
        [note.time]                    VARCHAR(64)         '$.time',
        [note.text]                    NVARCHAR(MAX)       '$.text',
        [note.author.reference.id]     NVARCHAR(100)       '$.author.reference.id',
        [note.author.reference.extension] NVARCHAR(MAX)       '$.author.reference.extension',
        [note.author.reference.reference] NVARCHAR(4000)      '$.author.reference.reference',
        [note.author.reference.type]   VARCHAR(256)        '$.author.reference.type',
        [note.author.reference.identifier] NVARCHAR(MAX)       '$.author.reference.identifier',
        [note.author.reference.display] NVARCHAR(4000)      '$.author.reference.display',
        [note.author.string]           NVARCHAR(4000)      '$.author.string'
    ) j
