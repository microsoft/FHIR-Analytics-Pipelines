CREATE EXTERNAL TABLE [fhir].[Evidence] (
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
    [assertion] NVARCHAR(MAX),
    [note] VARCHAR(MAX),
    [variableDefinition] VARCHAR(MAX),
    [synthesisType.id] NVARCHAR(100),
    [synthesisType.extension] NVARCHAR(MAX),
    [synthesisType.coding] VARCHAR(MAX),
    [synthesisType.text] NVARCHAR(4000),
    [studyType.id] NVARCHAR(100),
    [studyType.extension] NVARCHAR(MAX),
    [studyType.coding] VARCHAR(MAX),
    [studyType.text] NVARCHAR(4000),
    [statistic] VARCHAR(MAX),
    [certainty] VARCHAR(MAX),
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
    LOCATION='/Evidence/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.EvidenceIdentifier AS
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
        BULK 'Evidence/**',
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

CREATE VIEW fhir.EvidenceContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'Evidence/**',
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

CREATE VIEW fhir.EvidenceUseContext AS
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
        BULK 'Evidence/**',
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

CREATE VIEW fhir.EvidenceJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'Evidence/**',
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

CREATE VIEW fhir.EvidenceTopic AS
SELECT
    [id],
    [topic.JSON],
    [topic.id],
    [topic.extension],
    [topic.coding],
    [topic.text]
FROM openrowset (
        BULK 'Evidence/**',
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

CREATE VIEW fhir.EvidenceAuthor AS
SELECT
    [id],
    [author.JSON],
    [author.id],
    [author.extension],
    [author.name],
    [author.telecom]
FROM openrowset (
        BULK 'Evidence/**',
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

CREATE VIEW fhir.EvidenceEditor AS
SELECT
    [id],
    [editor.JSON],
    [editor.id],
    [editor.extension],
    [editor.name],
    [editor.telecom]
FROM openrowset (
        BULK 'Evidence/**',
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

CREATE VIEW fhir.EvidenceReviewer AS
SELECT
    [id],
    [reviewer.JSON],
    [reviewer.id],
    [reviewer.extension],
    [reviewer.name],
    [reviewer.telecom]
FROM openrowset (
        BULK 'Evidence/**',
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

CREATE VIEW fhir.EvidenceEndorser AS
SELECT
    [id],
    [endorser.JSON],
    [endorser.id],
    [endorser.extension],
    [endorser.name],
    [endorser.telecom]
FROM openrowset (
        BULK 'Evidence/**',
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

CREATE VIEW fhir.EvidenceRelatedArtifact AS
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
        BULK 'Evidence/**',
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

CREATE VIEW fhir.EvidenceNote AS
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
        BULK 'Evidence/**',
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

CREATE VIEW fhir.EvidenceVariableDefinition AS
SELECT
    [id],
    [variableDefinition.JSON],
    [variableDefinition.id],
    [variableDefinition.extension],
    [variableDefinition.modifierExtension],
    [variableDefinition.description],
    [variableDefinition.note],
    [variableDefinition.variableRole.id],
    [variableDefinition.variableRole.extension],
    [variableDefinition.variableRole.coding],
    [variableDefinition.variableRole.text],
    [variableDefinition.observed.id],
    [variableDefinition.observed.extension],
    [variableDefinition.observed.reference],
    [variableDefinition.observed.type],
    [variableDefinition.observed.identifier],
    [variableDefinition.observed.display],
    [variableDefinition.intended.id],
    [variableDefinition.intended.extension],
    [variableDefinition.intended.reference],
    [variableDefinition.intended.type],
    [variableDefinition.intended.identifier],
    [variableDefinition.intended.display],
    [variableDefinition.directnessMatch.id],
    [variableDefinition.directnessMatch.extension],
    [variableDefinition.directnessMatch.coding],
    [variableDefinition.directnessMatch.text]
FROM openrowset (
        BULK 'Evidence/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [variableDefinition.JSON]  VARCHAR(MAX) '$.variableDefinition'
    ) AS rowset
    CROSS APPLY openjson (rowset.[variableDefinition.JSON]) with (
        [variableDefinition.id]        NVARCHAR(100)       '$.id',
        [variableDefinition.extension] NVARCHAR(MAX)       '$.extension',
        [variableDefinition.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [variableDefinition.description] NVARCHAR(MAX)       '$.description',
        [variableDefinition.note]      NVARCHAR(MAX)       '$.note' AS JSON,
        [variableDefinition.variableRole.id] NVARCHAR(100)       '$.variableRole.id',
        [variableDefinition.variableRole.extension] NVARCHAR(MAX)       '$.variableRole.extension',
        [variableDefinition.variableRole.coding] NVARCHAR(MAX)       '$.variableRole.coding',
        [variableDefinition.variableRole.text] NVARCHAR(4000)      '$.variableRole.text',
        [variableDefinition.observed.id] NVARCHAR(100)       '$.observed.id',
        [variableDefinition.observed.extension] NVARCHAR(MAX)       '$.observed.extension',
        [variableDefinition.observed.reference] NVARCHAR(4000)      '$.observed.reference',
        [variableDefinition.observed.type] VARCHAR(256)        '$.observed.type',
        [variableDefinition.observed.identifier] NVARCHAR(MAX)       '$.observed.identifier',
        [variableDefinition.observed.display] NVARCHAR(4000)      '$.observed.display',
        [variableDefinition.intended.id] NVARCHAR(100)       '$.intended.id',
        [variableDefinition.intended.extension] NVARCHAR(MAX)       '$.intended.extension',
        [variableDefinition.intended.reference] NVARCHAR(4000)      '$.intended.reference',
        [variableDefinition.intended.type] VARCHAR(256)        '$.intended.type',
        [variableDefinition.intended.identifier] NVARCHAR(MAX)       '$.intended.identifier',
        [variableDefinition.intended.display] NVARCHAR(4000)      '$.intended.display',
        [variableDefinition.directnessMatch.id] NVARCHAR(100)       '$.directnessMatch.id',
        [variableDefinition.directnessMatch.extension] NVARCHAR(MAX)       '$.directnessMatch.extension',
        [variableDefinition.directnessMatch.coding] NVARCHAR(MAX)       '$.directnessMatch.coding',
        [variableDefinition.directnessMatch.text] NVARCHAR(4000)      '$.directnessMatch.text'
    ) j

GO

CREATE VIEW fhir.EvidenceStatistic AS
SELECT
    [id],
    [statistic.JSON],
    [statistic.id],
    [statistic.extension],
    [statistic.modifierExtension],
    [statistic.description],
    [statistic.note],
    [statistic.statisticType.id],
    [statistic.statisticType.extension],
    [statistic.statisticType.coding],
    [statistic.statisticType.text],
    [statistic.category.id],
    [statistic.category.extension],
    [statistic.category.coding],
    [statistic.category.text],
    [statistic.quantity.id],
    [statistic.quantity.extension],
    [statistic.quantity.value],
    [statistic.quantity.comparator],
    [statistic.quantity.unit],
    [statistic.quantity.system],
    [statistic.quantity.code],
    [statistic.numberOfEvents],
    [statistic.numberAffected],
    [statistic.sampleSize.id],
    [statistic.sampleSize.extension],
    [statistic.sampleSize.modifierExtension],
    [statistic.sampleSize.description],
    [statistic.sampleSize.note],
    [statistic.sampleSize.numberOfStudies],
    [statistic.sampleSize.numberOfParticipants],
    [statistic.sampleSize.knownDataCount],
    [statistic.attributeEstimate],
    [statistic.modelCharacteristic]
FROM openrowset (
        BULK 'Evidence/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [statistic.JSON]  VARCHAR(MAX) '$.statistic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[statistic.JSON]) with (
        [statistic.id]                 NVARCHAR(100)       '$.id',
        [statistic.extension]          NVARCHAR(MAX)       '$.extension',
        [statistic.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [statistic.description]        NVARCHAR(4000)      '$.description',
        [statistic.note]               NVARCHAR(MAX)       '$.note' AS JSON,
        [statistic.statisticType.id]   NVARCHAR(100)       '$.statisticType.id',
        [statistic.statisticType.extension] NVARCHAR(MAX)       '$.statisticType.extension',
        [statistic.statisticType.coding] NVARCHAR(MAX)       '$.statisticType.coding',
        [statistic.statisticType.text] NVARCHAR(4000)      '$.statisticType.text',
        [statistic.category.id]        NVARCHAR(100)       '$.category.id',
        [statistic.category.extension] NVARCHAR(MAX)       '$.category.extension',
        [statistic.category.coding]    NVARCHAR(MAX)       '$.category.coding',
        [statistic.category.text]      NVARCHAR(4000)      '$.category.text',
        [statistic.quantity.id]        NVARCHAR(100)       '$.quantity.id',
        [statistic.quantity.extension] NVARCHAR(MAX)       '$.quantity.extension',
        [statistic.quantity.value]     float               '$.quantity.value',
        [statistic.quantity.comparator] NVARCHAR(64)        '$.quantity.comparator',
        [statistic.quantity.unit]      NVARCHAR(100)       '$.quantity.unit',
        [statistic.quantity.system]    VARCHAR(256)        '$.quantity.system',
        [statistic.quantity.code]      NVARCHAR(4000)      '$.quantity.code',
        [statistic.numberOfEvents]     bigint              '$.numberOfEvents',
        [statistic.numberAffected]     bigint              '$.numberAffected',
        [statistic.sampleSize.id]      NVARCHAR(100)       '$.sampleSize.id',
        [statistic.sampleSize.extension] NVARCHAR(MAX)       '$.sampleSize.extension',
        [statistic.sampleSize.modifierExtension] NVARCHAR(MAX)       '$.sampleSize.modifierExtension',
        [statistic.sampleSize.description] NVARCHAR(4000)      '$.sampleSize.description',
        [statistic.sampleSize.note]    NVARCHAR(MAX)       '$.sampleSize.note',
        [statistic.sampleSize.numberOfStudies] bigint              '$.sampleSize.numberOfStudies',
        [statistic.sampleSize.numberOfParticipants] bigint              '$.sampleSize.numberOfParticipants',
        [statistic.sampleSize.knownDataCount] bigint              '$.sampleSize.knownDataCount',
        [statistic.attributeEstimate]  NVARCHAR(MAX)       '$.attributeEstimate' AS JSON,
        [statistic.modelCharacteristic] NVARCHAR(MAX)       '$.modelCharacteristic' AS JSON
    ) j

GO

CREATE VIEW fhir.EvidenceCertainty AS
SELECT
    [id],
    [certainty.JSON],
    [certainty.id],
    [certainty.extension],
    [certainty.modifierExtension],
    [certainty.description],
    [certainty.note],
    [certainty.type.id],
    [certainty.type.extension],
    [certainty.type.coding],
    [certainty.type.text],
    [certainty.rating.id],
    [certainty.rating.extension],
    [certainty.rating.coding],
    [certainty.rating.text],
    [certainty.rater],
    [certainty.subcomponent]
FROM openrowset (
        BULK 'Evidence/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [certainty.JSON]  VARCHAR(MAX) '$.certainty'
    ) AS rowset
    CROSS APPLY openjson (rowset.[certainty.JSON]) with (
        [certainty.id]                 NVARCHAR(100)       '$.id',
        [certainty.extension]          NVARCHAR(MAX)       '$.extension',
        [certainty.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [certainty.description]        NVARCHAR(4000)      '$.description',
        [certainty.note]               NVARCHAR(MAX)       '$.note' AS JSON,
        [certainty.type.id]            NVARCHAR(100)       '$.type.id',
        [certainty.type.extension]     NVARCHAR(MAX)       '$.type.extension',
        [certainty.type.coding]        NVARCHAR(MAX)       '$.type.coding',
        [certainty.type.text]          NVARCHAR(4000)      '$.type.text',
        [certainty.rating.id]          NVARCHAR(100)       '$.rating.id',
        [certainty.rating.extension]   NVARCHAR(MAX)       '$.rating.extension',
        [certainty.rating.coding]      NVARCHAR(MAX)       '$.rating.coding',
        [certainty.rating.text]        NVARCHAR(4000)      '$.rating.text',
        [certainty.rater]              NVARCHAR(4000)      '$.rater',
        [certainty.subcomponent]       NVARCHAR(MAX)       '$.subcomponent' AS JSON
    ) j
