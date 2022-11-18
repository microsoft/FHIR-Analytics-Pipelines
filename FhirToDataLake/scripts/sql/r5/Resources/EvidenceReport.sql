CREATE EXTERNAL TABLE [fhir].[EvidenceReport] (
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
    [identifier] VARCHAR(MAX),
    [version] NVARCHAR(100),
    [name] NVARCHAR(500),
    [title] NVARCHAR(4000),
    [status] NVARCHAR(100),
    [experimental] bit,
    [date] VARCHAR(64),
    [publisher] NVARCHAR(500),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [purpose] NVARCHAR(MAX),
    [copyright] NVARCHAR(MAX),
    [approvalDate] VARCHAR(64),
    [lastReviewDate] VARCHAR(64),
    [effectivePeriod.id] NVARCHAR(100),
    [effectivePeriod.extension] NVARCHAR(MAX),
    [effectivePeriod.start] VARCHAR(64),
    [effectivePeriod.end] VARCHAR(64),
    [topic] VARCHAR(MAX),
    [author] VARCHAR(MAX),
    [editor] VARCHAR(MAX),
    [reviewer] VARCHAR(MAX),
    [endorser] VARCHAR(MAX),
    [relatedArtifact] VARCHAR(MAX),
    [relatedIdentifier] VARCHAR(MAX),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [note] VARCHAR(MAX),
    [subject.id] NVARCHAR(100),
    [subject.extension] NVARCHAR(MAX),
    [subject.modifierExtension] NVARCHAR(MAX),
    [subject.characteristic] VARCHAR(MAX),
    [subject.note] VARCHAR(MAX),
    [relatesTo] VARCHAR(MAX),
    [section] VARCHAR(MAX),
    [citeAs.reference.id] NVARCHAR(100),
    [citeAs.reference.extension] NVARCHAR(MAX),
    [citeAs.reference.reference] NVARCHAR(4000),
    [citeAs.reference.type] VARCHAR(256),
    [citeAs.reference.identifier.id] NVARCHAR(100),
    [citeAs.reference.identifier.extension] NVARCHAR(MAX),
    [citeAs.reference.identifier.use] NVARCHAR(64),
    [citeAs.reference.identifier.type] NVARCHAR(MAX),
    [citeAs.reference.identifier.system] VARCHAR(256),
    [citeAs.reference.identifier.value] NVARCHAR(4000),
    [citeAs.reference.identifier.period] NVARCHAR(MAX),
    [citeAs.reference.identifier.assigner] NVARCHAR(MAX),
    [citeAs.reference.display] NVARCHAR(4000),
    [citeAs.markdown] NVARCHAR(MAX),
) WITH (
    LOCATION='/EvidenceReport/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.EvidenceReportIdentifier AS
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
        BULK 'EvidenceReport/**',
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

CREATE VIEW fhir.EvidenceReportContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'EvidenceReport/**',
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

CREATE VIEW fhir.EvidenceReportUseContext AS
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
        BULK 'EvidenceReport/**',
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

CREATE VIEW fhir.EvidenceReportJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'EvidenceReport/**',
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

CREATE VIEW fhir.EvidenceReportTopic AS
SELECT
    [id],
    [topic.JSON],
    [topic.id],
    [topic.extension],
    [topic.coding],
    [topic.text]
FROM openrowset (
        BULK 'EvidenceReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [topic.JSON]  VARCHAR(MAX) '$.topic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[topic.JSON]) with (
        [topic.id]                     NVARCHAR(100)       '$.id',
        [topic.extension]              NVARCHAR(MAX)       '$.extension',
        [topic.coding]                 NVARCHAR(MAX)       '$.coding' AS JSON,
        [topic.text]                   NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.EvidenceReportAuthor AS
SELECT
    [id],
    [author.JSON],
    [author.id],
    [author.extension],
    [author.name],
    [author.telecom]
FROM openrowset (
        BULK 'EvidenceReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [author.JSON]  VARCHAR(MAX) '$.author'
    ) AS rowset
    CROSS APPLY openjson (rowset.[author.JSON]) with (
        [author.id]                    NVARCHAR(100)       '$.id',
        [author.extension]             NVARCHAR(MAX)       '$.extension',
        [author.name]                  NVARCHAR(500)       '$.name',
        [author.telecom]               NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.EvidenceReportEditor AS
SELECT
    [id],
    [editor.JSON],
    [editor.id],
    [editor.extension],
    [editor.name],
    [editor.telecom]
FROM openrowset (
        BULK 'EvidenceReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [editor.JSON]  VARCHAR(MAX) '$.editor'
    ) AS rowset
    CROSS APPLY openjson (rowset.[editor.JSON]) with (
        [editor.id]                    NVARCHAR(100)       '$.id',
        [editor.extension]             NVARCHAR(MAX)       '$.extension',
        [editor.name]                  NVARCHAR(500)       '$.name',
        [editor.telecom]               NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.EvidenceReportReviewer AS
SELECT
    [id],
    [reviewer.JSON],
    [reviewer.id],
    [reviewer.extension],
    [reviewer.name],
    [reviewer.telecom]
FROM openrowset (
        BULK 'EvidenceReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reviewer.JSON]  VARCHAR(MAX) '$.reviewer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reviewer.JSON]) with (
        [reviewer.id]                  NVARCHAR(100)       '$.id',
        [reviewer.extension]           NVARCHAR(MAX)       '$.extension',
        [reviewer.name]                NVARCHAR(500)       '$.name',
        [reviewer.telecom]             NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.EvidenceReportEndorser AS
SELECT
    [id],
    [endorser.JSON],
    [endorser.id],
    [endorser.extension],
    [endorser.name],
    [endorser.telecom]
FROM openrowset (
        BULK 'EvidenceReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [endorser.JSON]  VARCHAR(MAX) '$.endorser'
    ) AS rowset
    CROSS APPLY openjson (rowset.[endorser.JSON]) with (
        [endorser.id]                  NVARCHAR(100)       '$.id',
        [endorser.extension]           NVARCHAR(MAX)       '$.extension',
        [endorser.name]                NVARCHAR(500)       '$.name',
        [endorser.telecom]             NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.EvidenceReportRelatedArtifact AS
SELECT
    [id],
    [relatedArtifact.JSON],
    [relatedArtifact.id],
    [relatedArtifact.extension],
    [relatedArtifact.type],
    [relatedArtifact.classifier],
    [relatedArtifact.label],
    [relatedArtifact.display],
    [relatedArtifact.citation],
    [relatedArtifact.document.id],
    [relatedArtifact.document.extension],
    [relatedArtifact.document.contentType],
    [relatedArtifact.document.language],
    [relatedArtifact.document.data],
    [relatedArtifact.document.url],
    [relatedArtifact.document.size],
    [relatedArtifact.document.hash],
    [relatedArtifact.document.title],
    [relatedArtifact.document.creation],
    [relatedArtifact.document.height],
    [relatedArtifact.document.width],
    [relatedArtifact.document.frames],
    [relatedArtifact.document.duration],
    [relatedArtifact.document.pages],
    [relatedArtifact.resource],
    [relatedArtifact.resourceReference.id],
    [relatedArtifact.resourceReference.extension],
    [relatedArtifact.resourceReference.reference],
    [relatedArtifact.resourceReference.type],
    [relatedArtifact.resourceReference.identifier],
    [relatedArtifact.resourceReference.display]
FROM openrowset (
        BULK 'EvidenceReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [relatedArtifact.JSON]  VARCHAR(MAX) '$.relatedArtifact'
    ) AS rowset
    CROSS APPLY openjson (rowset.[relatedArtifact.JSON]) with (
        [relatedArtifact.id]           NVARCHAR(100)       '$.id',
        [relatedArtifact.extension]    NVARCHAR(MAX)       '$.extension',
        [relatedArtifact.type]         NVARCHAR(64)        '$.type',
        [relatedArtifact.classifier]   NVARCHAR(MAX)       '$.classifier' AS JSON,
        [relatedArtifact.label]        NVARCHAR(100)       '$.label',
        [relatedArtifact.display]      NVARCHAR(4000)      '$.display',
        [relatedArtifact.citation]     NVARCHAR(MAX)       '$.citation',
        [relatedArtifact.document.id]  NVARCHAR(100)       '$.document.id',
        [relatedArtifact.document.extension] NVARCHAR(MAX)       '$.document.extension',
        [relatedArtifact.document.contentType] NVARCHAR(100)       '$.document.contentType',
        [relatedArtifact.document.language] NVARCHAR(100)       '$.document.language',
        [relatedArtifact.document.data] NVARCHAR(MAX)       '$.document.data',
        [relatedArtifact.document.url] VARCHAR(256)        '$.document.url',
        [relatedArtifact.document.size] NVARCHAR(MAX)       '$.document.size',
        [relatedArtifact.document.hash] NVARCHAR(MAX)       '$.document.hash',
        [relatedArtifact.document.title] NVARCHAR(4000)      '$.document.title',
        [relatedArtifact.document.creation] VARCHAR(64)         '$.document.creation',
        [relatedArtifact.document.height] bigint              '$.document.height',
        [relatedArtifact.document.width] bigint              '$.document.width',
        [relatedArtifact.document.frames] bigint              '$.document.frames',
        [relatedArtifact.document.duration] float               '$.document.duration',
        [relatedArtifact.document.pages] bigint              '$.document.pages',
        [relatedArtifact.resource]     VARCHAR(256)        '$.resource',
        [relatedArtifact.resourceReference.id] NVARCHAR(100)       '$.resourceReference.id',
        [relatedArtifact.resourceReference.extension] NVARCHAR(MAX)       '$.resourceReference.extension',
        [relatedArtifact.resourceReference.reference] NVARCHAR(4000)      '$.resourceReference.reference',
        [relatedArtifact.resourceReference.type] VARCHAR(256)        '$.resourceReference.type',
        [relatedArtifact.resourceReference.identifier] NVARCHAR(MAX)       '$.resourceReference.identifier',
        [relatedArtifact.resourceReference.display] NVARCHAR(4000)      '$.resourceReference.display'
    ) j

GO

CREATE VIEW fhir.EvidenceReportRelatedIdentifier AS
SELECT
    [id],
    [relatedIdentifier.JSON],
    [relatedIdentifier.id],
    [relatedIdentifier.extension],
    [relatedIdentifier.use],
    [relatedIdentifier.type.id],
    [relatedIdentifier.type.extension],
    [relatedIdentifier.type.coding],
    [relatedIdentifier.type.text],
    [relatedIdentifier.system],
    [relatedIdentifier.value],
    [relatedIdentifier.period.id],
    [relatedIdentifier.period.extension],
    [relatedIdentifier.period.start],
    [relatedIdentifier.period.end],
    [relatedIdentifier.assigner.id],
    [relatedIdentifier.assigner.extension],
    [relatedIdentifier.assigner.reference],
    [relatedIdentifier.assigner.type],
    [relatedIdentifier.assigner.identifier],
    [relatedIdentifier.assigner.display]
FROM openrowset (
        BULK 'EvidenceReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [relatedIdentifier.JSON]  VARCHAR(MAX) '$.relatedIdentifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[relatedIdentifier.JSON]) with (
        [relatedIdentifier.id]         NVARCHAR(100)       '$.id',
        [relatedIdentifier.extension]  NVARCHAR(MAX)       '$.extension',
        [relatedIdentifier.use]        NVARCHAR(64)        '$.use',
        [relatedIdentifier.type.id]    NVARCHAR(100)       '$.type.id',
        [relatedIdentifier.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [relatedIdentifier.type.coding] NVARCHAR(MAX)       '$.type.coding',
        [relatedIdentifier.type.text]  NVARCHAR(4000)      '$.type.text',
        [relatedIdentifier.system]     VARCHAR(256)        '$.system',
        [relatedIdentifier.value]      NVARCHAR(4000)      '$.value',
        [relatedIdentifier.period.id]  NVARCHAR(100)       '$.period.id',
        [relatedIdentifier.period.extension] NVARCHAR(MAX)       '$.period.extension',
        [relatedIdentifier.period.start] VARCHAR(64)         '$.period.start',
        [relatedIdentifier.period.end] VARCHAR(64)         '$.period.end',
        [relatedIdentifier.assigner.id] NVARCHAR(100)       '$.assigner.id',
        [relatedIdentifier.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [relatedIdentifier.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [relatedIdentifier.assigner.type] VARCHAR(256)        '$.assigner.type',
        [relatedIdentifier.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [relatedIdentifier.assigner.display] NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.EvidenceReportNote AS
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
        BULK 'EvidenceReport/**',
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

GO

CREATE VIEW fhir.EvidenceReportRelatesTo AS
SELECT
    [id],
    [relatesTo.JSON],
    [relatesTo.id],
    [relatesTo.extension],
    [relatesTo.modifierExtension],
    [relatesTo.code],
    [relatesTo.target.id],
    [relatesTo.target.extension],
    [relatesTo.target.modifierExtension],
    [relatesTo.target.url],
    [relatesTo.target.identifier],
    [relatesTo.target.display],
    [relatesTo.target.resource]
FROM openrowset (
        BULK 'EvidenceReport/**',
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
        [relatesTo.code]               NVARCHAR(4000)      '$.code',
        [relatesTo.target.id]          NVARCHAR(100)       '$.target.id',
        [relatesTo.target.extension]   NVARCHAR(MAX)       '$.target.extension',
        [relatesTo.target.modifierExtension] NVARCHAR(MAX)       '$.target.modifierExtension',
        [relatesTo.target.url]         VARCHAR(256)        '$.target.url',
        [relatesTo.target.identifier]  NVARCHAR(MAX)       '$.target.identifier',
        [relatesTo.target.display]     NVARCHAR(MAX)       '$.target.display',
        [relatesTo.target.resource]    NVARCHAR(MAX)       '$.target.resource'
    ) j

GO

CREATE VIEW fhir.EvidenceReportSection AS
SELECT
    [id],
    [section.JSON],
    [section.id],
    [section.extension],
    [section.modifierExtension],
    [section.title],
    [section.focus.id],
    [section.focus.extension],
    [section.focus.coding],
    [section.focus.text],
    [section.focusReference.id],
    [section.focusReference.extension],
    [section.focusReference.reference],
    [section.focusReference.type],
    [section.focusReference.identifier],
    [section.focusReference.display],
    [section.author],
    [section.text.id],
    [section.text.extension],
    [section.text.status],
    [section.text.div],
    [section.mode],
    [section.orderedBy.id],
    [section.orderedBy.extension],
    [section.orderedBy.coding],
    [section.orderedBy.text],
    [section.entryClassifier],
    [section.entryReference],
    [section.entryQuantity],
    [section.emptyReason.id],
    [section.emptyReason.extension],
    [section.emptyReason.coding],
    [section.emptyReason.text],
    [section.section]
FROM openrowset (
        BULK 'EvidenceReport/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [section.JSON]  VARCHAR(MAX) '$.section'
    ) AS rowset
    CROSS APPLY openjson (rowset.[section.JSON]) with (
        [section.id]                   NVARCHAR(100)       '$.id',
        [section.extension]            NVARCHAR(MAX)       '$.extension',
        [section.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [section.title]                NVARCHAR(4000)      '$.title',
        [section.focus.id]             NVARCHAR(100)       '$.focus.id',
        [section.focus.extension]      NVARCHAR(MAX)       '$.focus.extension',
        [section.focus.coding]         NVARCHAR(MAX)       '$.focus.coding',
        [section.focus.text]           NVARCHAR(4000)      '$.focus.text',
        [section.focusReference.id]    NVARCHAR(100)       '$.focusReference.id',
        [section.focusReference.extension] NVARCHAR(MAX)       '$.focusReference.extension',
        [section.focusReference.reference] NVARCHAR(4000)      '$.focusReference.reference',
        [section.focusReference.type]  VARCHAR(256)        '$.focusReference.type',
        [section.focusReference.identifier] NVARCHAR(MAX)       '$.focusReference.identifier',
        [section.focusReference.display] NVARCHAR(4000)      '$.focusReference.display',
        [section.author]               NVARCHAR(MAX)       '$.author' AS JSON,
        [section.text.id]              NVARCHAR(100)       '$.text.id',
        [section.text.extension]       NVARCHAR(MAX)       '$.text.extension',
        [section.text.status]          NVARCHAR(64)        '$.text.status',
        [section.text.div]             NVARCHAR(MAX)       '$.text.div',
        [section.mode]                 NVARCHAR(100)       '$.mode',
        [section.orderedBy.id]         NVARCHAR(100)       '$.orderedBy.id',
        [section.orderedBy.extension]  NVARCHAR(MAX)       '$.orderedBy.extension',
        [section.orderedBy.coding]     NVARCHAR(MAX)       '$.orderedBy.coding',
        [section.orderedBy.text]       NVARCHAR(4000)      '$.orderedBy.text',
        [section.entryClassifier]      NVARCHAR(MAX)       '$.entryClassifier' AS JSON,
        [section.entryReference]       NVARCHAR(MAX)       '$.entryReference' AS JSON,
        [section.entryQuantity]        NVARCHAR(MAX)       '$.entryQuantity' AS JSON,
        [section.emptyReason.id]       NVARCHAR(100)       '$.emptyReason.id',
        [section.emptyReason.extension] NVARCHAR(MAX)       '$.emptyReason.extension',
        [section.emptyReason.coding]   NVARCHAR(MAX)       '$.emptyReason.coding',
        [section.emptyReason.text]     NVARCHAR(4000)      '$.emptyReason.text',
        [section.section]              NVARCHAR(MAX)       '$.section' AS JSON
    ) j
