CREATE EXTERNAL TABLE [fhir].[EffectEvidenceSynthesis] (
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
    [identifier] VARCHAR(MAX),
    [version] NVARCHAR(4000),
    [name] NVARCHAR(4000),
    [title] NVARCHAR(4000),
    [status] NVARCHAR(64),
    [date] VARCHAR(30),
    [publisher] NVARCHAR(4000),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [note] VARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [copyright] NVARCHAR(MAX),
    [approvalDate] VARCHAR(10),
    [lastReviewDate] VARCHAR(10),
    [effectivePeriod.id] NVARCHAR(4000),
    [effectivePeriod.extension] NVARCHAR(MAX),
    [effectivePeriod.start] VARCHAR(30),
    [effectivePeriod.end] VARCHAR(30),
    [topic] VARCHAR(MAX),
    [author] VARCHAR(MAX),
    [editor] VARCHAR(MAX),
    [reviewer] VARCHAR(MAX),
    [endorser] VARCHAR(MAX),
    [relatedArtifact] VARCHAR(MAX),
    [synthesisType.id] NVARCHAR(4000),
    [synthesisType.extension] NVARCHAR(MAX),
    [synthesisType.coding] VARCHAR(MAX),
    [synthesisType.text] NVARCHAR(4000),
    [studyType.id] NVARCHAR(4000),
    [studyType.extension] NVARCHAR(MAX),
    [studyType.coding] VARCHAR(MAX),
    [studyType.text] NVARCHAR(4000),
    [population.id] NVARCHAR(4000),
    [population.extension] NVARCHAR(MAX),
    [population.reference] NVARCHAR(4000),
    [population.type] VARCHAR(256),
    [population.identifier.id] NVARCHAR(4000),
    [population.identifier.extension] NVARCHAR(MAX),
    [population.identifier.use] NVARCHAR(64),
    [population.identifier.type] NVARCHAR(MAX),
    [population.identifier.system] VARCHAR(256),
    [population.identifier.value] NVARCHAR(4000),
    [population.identifier.period] NVARCHAR(MAX),
    [population.identifier.assigner] NVARCHAR(MAX),
    [population.display] NVARCHAR(4000),
    [exposure.id] NVARCHAR(4000),
    [exposure.extension] NVARCHAR(MAX),
    [exposure.reference] NVARCHAR(4000),
    [exposure.type] VARCHAR(256),
    [exposure.identifier.id] NVARCHAR(4000),
    [exposure.identifier.extension] NVARCHAR(MAX),
    [exposure.identifier.use] NVARCHAR(64),
    [exposure.identifier.type] NVARCHAR(MAX),
    [exposure.identifier.system] VARCHAR(256),
    [exposure.identifier.value] NVARCHAR(4000),
    [exposure.identifier.period] NVARCHAR(MAX),
    [exposure.identifier.assigner] NVARCHAR(MAX),
    [exposure.display] NVARCHAR(4000),
    [exposureAlternative.id] NVARCHAR(4000),
    [exposureAlternative.extension] NVARCHAR(MAX),
    [exposureAlternative.reference] NVARCHAR(4000),
    [exposureAlternative.type] VARCHAR(256),
    [exposureAlternative.identifier.id] NVARCHAR(4000),
    [exposureAlternative.identifier.extension] NVARCHAR(MAX),
    [exposureAlternative.identifier.use] NVARCHAR(64),
    [exposureAlternative.identifier.type] NVARCHAR(MAX),
    [exposureAlternative.identifier.system] VARCHAR(256),
    [exposureAlternative.identifier.value] NVARCHAR(4000),
    [exposureAlternative.identifier.period] NVARCHAR(MAX),
    [exposureAlternative.identifier.assigner] NVARCHAR(MAX),
    [exposureAlternative.display] NVARCHAR(4000),
    [outcome.id] NVARCHAR(4000),
    [outcome.extension] NVARCHAR(MAX),
    [outcome.reference] NVARCHAR(4000),
    [outcome.type] VARCHAR(256),
    [outcome.identifier.id] NVARCHAR(4000),
    [outcome.identifier.extension] NVARCHAR(MAX),
    [outcome.identifier.use] NVARCHAR(64),
    [outcome.identifier.type] NVARCHAR(MAX),
    [outcome.identifier.system] VARCHAR(256),
    [outcome.identifier.value] NVARCHAR(4000),
    [outcome.identifier.period] NVARCHAR(MAX),
    [outcome.identifier.assigner] NVARCHAR(MAX),
    [outcome.display] NVARCHAR(4000),
    [sampleSize.id] NVARCHAR(4000),
    [sampleSize.extension] NVARCHAR(MAX),
    [sampleSize.modifierExtension] NVARCHAR(MAX),
    [sampleSize.description] NVARCHAR(4000),
    [sampleSize.numberOfStudies] bigint,
    [sampleSize.numberOfParticipants] bigint,
    [resultsByExposure] VARCHAR(MAX),
    [effectEstimate] VARCHAR(MAX),
    [certainty] VARCHAR(MAX),
) WITH (
    LOCATION='/EffectEvidenceSynthesis/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.EffectEvidenceSynthesisIdentifier AS
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
        BULK 'EffectEvidenceSynthesis/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [identifier.JSON]  VARCHAR(MAX) '$.identifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[identifier.JSON]) with (
        [identifier.id]                NVARCHAR(4000)      '$.id',
        [identifier.extension]         NVARCHAR(MAX)       '$.extension',
        [identifier.use]               NVARCHAR(64)        '$.use',
        [identifier.type.id]           NVARCHAR(4000)      '$.type.id',
        [identifier.type.extension]    NVARCHAR(MAX)       '$.type.extension',
        [identifier.type.coding]       NVARCHAR(MAX)       '$.type.coding',
        [identifier.type.text]         NVARCHAR(4000)      '$.type.text',
        [identifier.system]            VARCHAR(256)        '$.system',
        [identifier.value]             NVARCHAR(4000)      '$.value',
        [identifier.period.id]         NVARCHAR(4000)      '$.period.id',
        [identifier.period.extension]  NVARCHAR(MAX)       '$.period.extension',
        [identifier.period.start]      VARCHAR(30)         '$.period.start',
        [identifier.period.end]        VARCHAR(30)         '$.period.end',
        [identifier.assigner.id]       NVARCHAR(4000)      '$.assigner.id',
        [identifier.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [identifier.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [identifier.assigner.type]     VARCHAR(256)        '$.assigner.type',
        [identifier.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [identifier.assigner.display]  NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.EffectEvidenceSynthesisContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
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

CREATE VIEW fhir.EffectEvidenceSynthesisNote AS
SELECT
    [id],
    [note.JSON],
    [note.id],
    [note.extension],
    [note.time],
    [note.text],
    [note.author.Reference.id],
    [note.author.Reference.extension],
    [note.author.Reference.reference],
    [note.author.Reference.type],
    [note.author.Reference.identifier],
    [note.author.Reference.display],
    [note.author.string]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [note.JSON]  VARCHAR(MAX) '$.note'
    ) AS rowset
    CROSS APPLY openjson (rowset.[note.JSON]) with (
        [note.id]                      NVARCHAR(4000)      '$.id',
        [note.extension]               NVARCHAR(MAX)       '$.extension',
        [note.time]                    VARCHAR(30)         '$.time',
        [note.text]                    NVARCHAR(MAX)       '$.text',
        [note.author.Reference.id]     NVARCHAR(4000)      '$.author.Reference.id',
        [note.author.Reference.extension] NVARCHAR(MAX)       '$.author.Reference.extension',
        [note.author.Reference.reference] NVARCHAR(4000)      '$.author.Reference.reference',
        [note.author.Reference.type]   VARCHAR(256)        '$.author.Reference.type',
        [note.author.Reference.identifier] NVARCHAR(MAX)       '$.author.Reference.identifier',
        [note.author.Reference.display] NVARCHAR(4000)      '$.author.Reference.display',
        [note.author.string]           NVARCHAR(4000)      '$.author.string'
    ) j

GO

CREATE VIEW fhir.EffectEvidenceSynthesisUseContext AS
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
        BULK 'EffectEvidenceSynthesis/**',
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

CREATE VIEW fhir.EffectEvidenceSynthesisJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
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

CREATE VIEW fhir.EffectEvidenceSynthesisTopic AS
SELECT
    [id],
    [topic.JSON],
    [topic.id],
    [topic.extension],
    [topic.coding],
    [topic.text]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [topic.JSON]  VARCHAR(MAX) '$.topic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[topic.JSON]) with (
        [topic.id]                     NVARCHAR(4000)      '$.id',
        [topic.extension]              NVARCHAR(MAX)       '$.extension',
        [topic.coding]                 NVARCHAR(MAX)       '$.coding' AS JSON,
        [topic.text]                   NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.EffectEvidenceSynthesisAuthor AS
SELECT
    [id],
    [author.JSON],
    [author.id],
    [author.extension],
    [author.name],
    [author.telecom]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [author.JSON]  VARCHAR(MAX) '$.author'
    ) AS rowset
    CROSS APPLY openjson (rowset.[author.JSON]) with (
        [author.id]                    NVARCHAR(4000)      '$.id',
        [author.extension]             NVARCHAR(MAX)       '$.extension',
        [author.name]                  NVARCHAR(4000)      '$.name',
        [author.telecom]               NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.EffectEvidenceSynthesisEditor AS
SELECT
    [id],
    [editor.JSON],
    [editor.id],
    [editor.extension],
    [editor.name],
    [editor.telecom]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [editor.JSON]  VARCHAR(MAX) '$.editor'
    ) AS rowset
    CROSS APPLY openjson (rowset.[editor.JSON]) with (
        [editor.id]                    NVARCHAR(4000)      '$.id',
        [editor.extension]             NVARCHAR(MAX)       '$.extension',
        [editor.name]                  NVARCHAR(4000)      '$.name',
        [editor.telecom]               NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.EffectEvidenceSynthesisReviewer AS
SELECT
    [id],
    [reviewer.JSON],
    [reviewer.id],
    [reviewer.extension],
    [reviewer.name],
    [reviewer.telecom]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reviewer.JSON]  VARCHAR(MAX) '$.reviewer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reviewer.JSON]) with (
        [reviewer.id]                  NVARCHAR(4000)      '$.id',
        [reviewer.extension]           NVARCHAR(MAX)       '$.extension',
        [reviewer.name]                NVARCHAR(4000)      '$.name',
        [reviewer.telecom]             NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.EffectEvidenceSynthesisEndorser AS
SELECT
    [id],
    [endorser.JSON],
    [endorser.id],
    [endorser.extension],
    [endorser.name],
    [endorser.telecom]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [endorser.JSON]  VARCHAR(MAX) '$.endorser'
    ) AS rowset
    CROSS APPLY openjson (rowset.[endorser.JSON]) with (
        [endorser.id]                  NVARCHAR(4000)      '$.id',
        [endorser.extension]           NVARCHAR(MAX)       '$.extension',
        [endorser.name]                NVARCHAR(4000)      '$.name',
        [endorser.telecom]             NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.EffectEvidenceSynthesisRelatedArtifact AS
SELECT
    [id],
    [relatedArtifact.JSON],
    [relatedArtifact.id],
    [relatedArtifact.extension],
    [relatedArtifact.type],
    [relatedArtifact.label],
    [relatedArtifact.display],
    [relatedArtifact.citation],
    [relatedArtifact.url],
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
    [relatedArtifact.resource]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [relatedArtifact.JSON]  VARCHAR(MAX) '$.relatedArtifact'
    ) AS rowset
    CROSS APPLY openjson (rowset.[relatedArtifact.JSON]) with (
        [relatedArtifact.id]           NVARCHAR(4000)      '$.id',
        [relatedArtifact.extension]    NVARCHAR(MAX)       '$.extension',
        [relatedArtifact.type]         NVARCHAR(64)        '$.type',
        [relatedArtifact.label]        NVARCHAR(4000)      '$.label',
        [relatedArtifact.display]      NVARCHAR(4000)      '$.display',
        [relatedArtifact.citation]     NVARCHAR(MAX)       '$.citation',
        [relatedArtifact.url]          VARCHAR(256)        '$.url',
        [relatedArtifact.document.id]  NVARCHAR(4000)      '$.document.id',
        [relatedArtifact.document.extension] NVARCHAR(MAX)       '$.document.extension',
        [relatedArtifact.document.contentType] NVARCHAR(4000)      '$.document.contentType',
        [relatedArtifact.document.language] NVARCHAR(4000)      '$.document.language',
        [relatedArtifact.document.data] NVARCHAR(MAX)       '$.document.data',
        [relatedArtifact.document.url] VARCHAR(256)        '$.document.url',
        [relatedArtifact.document.size] bigint              '$.document.size',
        [relatedArtifact.document.hash] NVARCHAR(MAX)       '$.document.hash',
        [relatedArtifact.document.title] NVARCHAR(4000)      '$.document.title',
        [relatedArtifact.document.creation] VARCHAR(30)         '$.document.creation',
        [relatedArtifact.resource]     VARCHAR(256)        '$.resource'
    ) j

GO

CREATE VIEW fhir.EffectEvidenceSynthesisResultsByExposure AS
SELECT
    [id],
    [resultsByExposure.JSON],
    [resultsByExposure.id],
    [resultsByExposure.extension],
    [resultsByExposure.modifierExtension],
    [resultsByExposure.description],
    [resultsByExposure.exposureState],
    [resultsByExposure.variantState.id],
    [resultsByExposure.variantState.extension],
    [resultsByExposure.variantState.coding],
    [resultsByExposure.variantState.text],
    [resultsByExposure.riskEvidenceSynthesis.id],
    [resultsByExposure.riskEvidenceSynthesis.extension],
    [resultsByExposure.riskEvidenceSynthesis.reference],
    [resultsByExposure.riskEvidenceSynthesis.type],
    [resultsByExposure.riskEvidenceSynthesis.identifier],
    [resultsByExposure.riskEvidenceSynthesis.display]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [resultsByExposure.JSON]  VARCHAR(MAX) '$.resultsByExposure'
    ) AS rowset
    CROSS APPLY openjson (rowset.[resultsByExposure.JSON]) with (
        [resultsByExposure.id]         NVARCHAR(4000)      '$.id',
        [resultsByExposure.extension]  NVARCHAR(MAX)       '$.extension',
        [resultsByExposure.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [resultsByExposure.description] NVARCHAR(4000)      '$.description',
        [resultsByExposure.exposureState] NVARCHAR(64)        '$.exposureState',
        [resultsByExposure.variantState.id] NVARCHAR(4000)      '$.variantState.id',
        [resultsByExposure.variantState.extension] NVARCHAR(MAX)       '$.variantState.extension',
        [resultsByExposure.variantState.coding] NVARCHAR(MAX)       '$.variantState.coding',
        [resultsByExposure.variantState.text] NVARCHAR(4000)      '$.variantState.text',
        [resultsByExposure.riskEvidenceSynthesis.id] NVARCHAR(4000)      '$.riskEvidenceSynthesis.id',
        [resultsByExposure.riskEvidenceSynthesis.extension] NVARCHAR(MAX)       '$.riskEvidenceSynthesis.extension',
        [resultsByExposure.riskEvidenceSynthesis.reference] NVARCHAR(4000)      '$.riskEvidenceSynthesis.reference',
        [resultsByExposure.riskEvidenceSynthesis.type] VARCHAR(256)        '$.riskEvidenceSynthesis.type',
        [resultsByExposure.riskEvidenceSynthesis.identifier] NVARCHAR(MAX)       '$.riskEvidenceSynthesis.identifier',
        [resultsByExposure.riskEvidenceSynthesis.display] NVARCHAR(4000)      '$.riskEvidenceSynthesis.display'
    ) j

GO

CREATE VIEW fhir.EffectEvidenceSynthesisEffectEstimate AS
SELECT
    [id],
    [effectEstimate.JSON],
    [effectEstimate.id],
    [effectEstimate.extension],
    [effectEstimate.modifierExtension],
    [effectEstimate.description],
    [effectEstimate.type.id],
    [effectEstimate.type.extension],
    [effectEstimate.type.coding],
    [effectEstimate.type.text],
    [effectEstimate.variantState.id],
    [effectEstimate.variantState.extension],
    [effectEstimate.variantState.coding],
    [effectEstimate.variantState.text],
    [effectEstimate.value],
    [effectEstimate.unitOfMeasure.id],
    [effectEstimate.unitOfMeasure.extension],
    [effectEstimate.unitOfMeasure.coding],
    [effectEstimate.unitOfMeasure.text],
    [effectEstimate.precisionEstimate]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [effectEstimate.JSON]  VARCHAR(MAX) '$.effectEstimate'
    ) AS rowset
    CROSS APPLY openjson (rowset.[effectEstimate.JSON]) with (
        [effectEstimate.id]            NVARCHAR(4000)      '$.id',
        [effectEstimate.extension]     NVARCHAR(MAX)       '$.extension',
        [effectEstimate.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [effectEstimate.description]   NVARCHAR(4000)      '$.description',
        [effectEstimate.type.id]       NVARCHAR(4000)      '$.type.id',
        [effectEstimate.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [effectEstimate.type.coding]   NVARCHAR(MAX)       '$.type.coding',
        [effectEstimate.type.text]     NVARCHAR(4000)      '$.type.text',
        [effectEstimate.variantState.id] NVARCHAR(4000)      '$.variantState.id',
        [effectEstimate.variantState.extension] NVARCHAR(MAX)       '$.variantState.extension',
        [effectEstimate.variantState.coding] NVARCHAR(MAX)       '$.variantState.coding',
        [effectEstimate.variantState.text] NVARCHAR(4000)      '$.variantState.text',
        [effectEstimate.value]         float               '$.value',
        [effectEstimate.unitOfMeasure.id] NVARCHAR(4000)      '$.unitOfMeasure.id',
        [effectEstimate.unitOfMeasure.extension] NVARCHAR(MAX)       '$.unitOfMeasure.extension',
        [effectEstimate.unitOfMeasure.coding] NVARCHAR(MAX)       '$.unitOfMeasure.coding',
        [effectEstimate.unitOfMeasure.text] NVARCHAR(4000)      '$.unitOfMeasure.text',
        [effectEstimate.precisionEstimate] NVARCHAR(MAX)       '$.precisionEstimate' AS JSON
    ) j

GO

CREATE VIEW fhir.EffectEvidenceSynthesisCertainty AS
SELECT
    [id],
    [certainty.JSON],
    [certainty.id],
    [certainty.extension],
    [certainty.modifierExtension],
    [certainty.rating],
    [certainty.note],
    [certainty.certaintySubcomponent]
FROM openrowset (
        BULK 'EffectEvidenceSynthesis/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [certainty.JSON]  VARCHAR(MAX) '$.certainty'
    ) AS rowset
    CROSS APPLY openjson (rowset.[certainty.JSON]) with (
        [certainty.id]                 NVARCHAR(4000)      '$.id',
        [certainty.extension]          NVARCHAR(MAX)       '$.extension',
        [certainty.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [certainty.rating]             NVARCHAR(MAX)       '$.rating' AS JSON,
        [certainty.note]               NVARCHAR(MAX)       '$.note' AS JSON,
        [certainty.certaintySubcomponent] NVARCHAR(MAX)       '$.certaintySubcomponent' AS JSON
    ) j
