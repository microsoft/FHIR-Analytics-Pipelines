CREATE EXTERNAL TABLE [fhir].[ResearchElementDefinition] (
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
    [shortTitle] NVARCHAR(4000),
    [subtitle] NVARCHAR(4000),
    [status] NVARCHAR(64),
    [experimental] bit,
    [date] VARCHAR(64),
    [publisher] NVARCHAR(500),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [comment] VARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [purpose] NVARCHAR(MAX),
    [usage] NVARCHAR(4000),
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
    [library] VARCHAR(MAX),
    [type] NVARCHAR(64),
    [variableType] NVARCHAR(64),
    [characteristic] VARCHAR(MAX),
    [subject.codeableConcept.id] NVARCHAR(100),
    [subject.codeableConcept.extension] NVARCHAR(MAX),
    [subject.codeableConcept.coding] VARCHAR(MAX),
    [subject.codeableConcept.text] NVARCHAR(4000),
    [subject.reference.id] NVARCHAR(100),
    [subject.reference.extension] NVARCHAR(MAX),
    [subject.reference.reference] NVARCHAR(4000),
    [subject.reference.type] VARCHAR(256),
    [subject.reference.identifier.id] NVARCHAR(100),
    [subject.reference.identifier.extension] NVARCHAR(MAX),
    [subject.reference.identifier.use] NVARCHAR(64),
    [subject.reference.identifier.type] NVARCHAR(MAX),
    [subject.reference.identifier.system] VARCHAR(256),
    [subject.reference.identifier.value] NVARCHAR(4000),
    [subject.reference.identifier.period] NVARCHAR(MAX),
    [subject.reference.identifier.assigner] NVARCHAR(MAX),
    [subject.reference.display] NVARCHAR(4000),
) WITH (
    LOCATION='/ResearchElementDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ResearchElementDefinitionIdentifier AS
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
        BULK 'ResearchElementDefinition/**',
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

CREATE VIEW fhir.ResearchElementDefinitionContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'ResearchElementDefinition/**',
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

CREATE VIEW fhir.ResearchElementDefinitionComment AS
SELECT
    [id],
    [comment.JSON],
    [comment]
FROM openrowset (
        BULK 'ResearchElementDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [comment.JSON]  VARCHAR(MAX) '$.comment'
    ) AS rowset
    CROSS APPLY openjson (rowset.[comment.JSON]) with (
        [comment]                      NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ResearchElementDefinitionUseContext AS
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
        BULK 'ResearchElementDefinition/**',
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

CREATE VIEW fhir.ResearchElementDefinitionJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'ResearchElementDefinition/**',
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

CREATE VIEW fhir.ResearchElementDefinitionTopic AS
SELECT
    [id],
    [topic.JSON],
    [topic.id],
    [topic.extension],
    [topic.coding],
    [topic.text]
FROM openrowset (
        BULK 'ResearchElementDefinition/**',
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

CREATE VIEW fhir.ResearchElementDefinitionAuthor AS
SELECT
    [id],
    [author.JSON],
    [author.id],
    [author.extension],
    [author.name],
    [author.telecom]
FROM openrowset (
        BULK 'ResearchElementDefinition/**',
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

CREATE VIEW fhir.ResearchElementDefinitionEditor AS
SELECT
    [id],
    [editor.JSON],
    [editor.id],
    [editor.extension],
    [editor.name],
    [editor.telecom]
FROM openrowset (
        BULK 'ResearchElementDefinition/**',
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

CREATE VIEW fhir.ResearchElementDefinitionReviewer AS
SELECT
    [id],
    [reviewer.JSON],
    [reviewer.id],
    [reviewer.extension],
    [reviewer.name],
    [reviewer.telecom]
FROM openrowset (
        BULK 'ResearchElementDefinition/**',
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

CREATE VIEW fhir.ResearchElementDefinitionEndorser AS
SELECT
    [id],
    [endorser.JSON],
    [endorser.id],
    [endorser.extension],
    [endorser.name],
    [endorser.telecom]
FROM openrowset (
        BULK 'ResearchElementDefinition/**',
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

CREATE VIEW fhir.ResearchElementDefinitionRelatedArtifact AS
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
        BULK 'ResearchElementDefinition/**',
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
        [relatedArtifact.label]        NVARCHAR(100)       '$.label',
        [relatedArtifact.display]      NVARCHAR(4000)      '$.display',
        [relatedArtifact.citation]     NVARCHAR(MAX)       '$.citation',
        [relatedArtifact.url]          VARCHAR(256)        '$.url',
        [relatedArtifact.document.id]  NVARCHAR(100)       '$.document.id',
        [relatedArtifact.document.extension] NVARCHAR(MAX)       '$.document.extension',
        [relatedArtifact.document.contentType] NVARCHAR(100)       '$.document.contentType',
        [relatedArtifact.document.language] NVARCHAR(100)       '$.document.language',
        [relatedArtifact.document.data] NVARCHAR(MAX)       '$.document.data',
        [relatedArtifact.document.url] VARCHAR(256)        '$.document.url',
        [relatedArtifact.document.size] bigint              '$.document.size',
        [relatedArtifact.document.hash] NVARCHAR(MAX)       '$.document.hash',
        [relatedArtifact.document.title] NVARCHAR(4000)      '$.document.title',
        [relatedArtifact.document.creation] VARCHAR(64)         '$.document.creation',
        [relatedArtifact.resource]     VARCHAR(256)        '$.resource'
    ) j

GO

CREATE VIEW fhir.ResearchElementDefinitionLibrary AS
SELECT
    [id],
    [library.JSON],
    [library]
FROM openrowset (
        BULK 'ResearchElementDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [library.JSON]  VARCHAR(MAX) '$.library'
    ) AS rowset
    CROSS APPLY openjson (rowset.[library.JSON]) with (
        [library]                      NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ResearchElementDefinitionCharacteristic AS
SELECT
    [id],
    [characteristic.JSON],
    [characteristic.id],
    [characteristic.extension],
    [characteristic.modifierExtension],
    [characteristic.usageContext],
    [characteristic.exclude],
    [characteristic.unitOfMeasure.id],
    [characteristic.unitOfMeasure.extension],
    [characteristic.unitOfMeasure.coding],
    [characteristic.unitOfMeasure.text],
    [characteristic.studyEffectiveDescription],
    [characteristic.studyEffectiveTimeFromStart.id],
    [characteristic.studyEffectiveTimeFromStart.extension],
    [characteristic.studyEffectiveTimeFromStart.value],
    [characteristic.studyEffectiveTimeFromStart.comparator],
    [characteristic.studyEffectiveTimeFromStart.unit],
    [characteristic.studyEffectiveTimeFromStart.system],
    [characteristic.studyEffectiveTimeFromStart.code],
    [characteristic.studyEffectiveGroupMeasure],
    [characteristic.participantEffectiveDescription],
    [characteristic.participantEffectiveTimeFromStart.id],
    [characteristic.participantEffectiveTimeFromStart.extension],
    [characteristic.participantEffectiveTimeFromStart.value],
    [characteristic.participantEffectiveTimeFromStart.comparator],
    [characteristic.participantEffectiveTimeFromStart.unit],
    [characteristic.participantEffectiveTimeFromStart.system],
    [characteristic.participantEffectiveTimeFromStart.code],
    [characteristic.participantEffectiveGroupMeasure],
    [characteristic.definition.codeableConcept.id],
    [characteristic.definition.codeableConcept.extension],
    [characteristic.definition.codeableConcept.coding],
    [characteristic.definition.codeableConcept.text],
    [characteristic.definition.canonical],
    [characteristic.definition.expression.id],
    [characteristic.definition.expression.extension],
    [characteristic.definition.expression.description],
    [characteristic.definition.expression.name],
    [characteristic.definition.expression.language],
    [characteristic.definition.expression.expression],
    [characteristic.definition.expression.reference],
    [characteristic.definition.dataRequirement.id],
    [characteristic.definition.dataRequirement.extension],
    [characteristic.definition.dataRequirement.type],
    [characteristic.definition.dataRequirement.profile],
    [characteristic.definition.dataRequirement.mustSupport],
    [characteristic.definition.dataRequirement.codeFilter],
    [characteristic.definition.dataRequirement.dateFilter],
    [characteristic.definition.dataRequirement.limit],
    [characteristic.definition.dataRequirement.sort],
    [characteristic.definition.dataRequirement.subject.codeableConcept],
    [characteristic.definition.dataRequirement.subject.reference],
    [characteristic.studyEffective.dateTime],
    [characteristic.studyEffective.period.id],
    [characteristic.studyEffective.period.extension],
    [characteristic.studyEffective.period.start],
    [characteristic.studyEffective.period.end],
    [characteristic.studyEffective.duration.id],
    [characteristic.studyEffective.duration.extension],
    [characteristic.studyEffective.duration.value],
    [characteristic.studyEffective.duration.comparator],
    [characteristic.studyEffective.duration.unit],
    [characteristic.studyEffective.duration.system],
    [characteristic.studyEffective.duration.code],
    [characteristic.studyEffective.timing.id],
    [characteristic.studyEffective.timing.extension],
    [characteristic.studyEffective.timing.modifierExtension],
    [characteristic.studyEffective.timing.event],
    [characteristic.studyEffective.timing.repeat],
    [characteristic.studyEffective.timing.code],
    [characteristic.participantEffective.dateTime],
    [characteristic.participantEffective.period.id],
    [characteristic.participantEffective.period.extension],
    [characteristic.participantEffective.period.start],
    [characteristic.participantEffective.period.end],
    [characteristic.participantEffective.duration.id],
    [characteristic.participantEffective.duration.extension],
    [characteristic.participantEffective.duration.value],
    [characteristic.participantEffective.duration.comparator],
    [characteristic.participantEffective.duration.unit],
    [characteristic.participantEffective.duration.system],
    [characteristic.participantEffective.duration.code],
    [characteristic.participantEffective.timing.id],
    [characteristic.participantEffective.timing.extension],
    [characteristic.participantEffective.timing.modifierExtension],
    [characteristic.participantEffective.timing.event],
    [characteristic.participantEffective.timing.repeat],
    [characteristic.participantEffective.timing.code]
FROM openrowset (
        BULK 'ResearchElementDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [characteristic.JSON]  VARCHAR(MAX) '$.characteristic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[characteristic.JSON]) with (
        [characteristic.id]            NVARCHAR(100)       '$.id',
        [characteristic.extension]     NVARCHAR(MAX)       '$.extension',
        [characteristic.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [characteristic.usageContext]  NVARCHAR(MAX)       '$.usageContext' AS JSON,
        [characteristic.exclude]       bit                 '$.exclude',
        [characteristic.unitOfMeasure.id] NVARCHAR(100)       '$.unitOfMeasure.id',
        [characteristic.unitOfMeasure.extension] NVARCHAR(MAX)       '$.unitOfMeasure.extension',
        [characteristic.unitOfMeasure.coding] NVARCHAR(MAX)       '$.unitOfMeasure.coding',
        [characteristic.unitOfMeasure.text] NVARCHAR(4000)      '$.unitOfMeasure.text',
        [characteristic.studyEffectiveDescription] NVARCHAR(4000)      '$.studyEffectiveDescription',
        [characteristic.studyEffectiveTimeFromStart.id] NVARCHAR(100)       '$.studyEffectiveTimeFromStart.id',
        [characteristic.studyEffectiveTimeFromStart.extension] NVARCHAR(MAX)       '$.studyEffectiveTimeFromStart.extension',
        [characteristic.studyEffectiveTimeFromStart.value] float               '$.studyEffectiveTimeFromStart.value',
        [characteristic.studyEffectiveTimeFromStart.comparator] NVARCHAR(64)        '$.studyEffectiveTimeFromStart.comparator',
        [characteristic.studyEffectiveTimeFromStart.unit] NVARCHAR(100)       '$.studyEffectiveTimeFromStart.unit',
        [characteristic.studyEffectiveTimeFromStart.system] VARCHAR(256)        '$.studyEffectiveTimeFromStart.system',
        [characteristic.studyEffectiveTimeFromStart.code] NVARCHAR(4000)      '$.studyEffectiveTimeFromStart.code',
        [characteristic.studyEffectiveGroupMeasure] NVARCHAR(64)        '$.studyEffectiveGroupMeasure',
        [characteristic.participantEffectiveDescription] NVARCHAR(4000)      '$.participantEffectiveDescription',
        [characteristic.participantEffectiveTimeFromStart.id] NVARCHAR(100)       '$.participantEffectiveTimeFromStart.id',
        [characteristic.participantEffectiveTimeFromStart.extension] NVARCHAR(MAX)       '$.participantEffectiveTimeFromStart.extension',
        [characteristic.participantEffectiveTimeFromStart.value] float               '$.participantEffectiveTimeFromStart.value',
        [characteristic.participantEffectiveTimeFromStart.comparator] NVARCHAR(64)        '$.participantEffectiveTimeFromStart.comparator',
        [characteristic.participantEffectiveTimeFromStart.unit] NVARCHAR(100)       '$.participantEffectiveTimeFromStart.unit',
        [characteristic.participantEffectiveTimeFromStart.system] VARCHAR(256)        '$.participantEffectiveTimeFromStart.system',
        [characteristic.participantEffectiveTimeFromStart.code] NVARCHAR(4000)      '$.participantEffectiveTimeFromStart.code',
        [characteristic.participantEffectiveGroupMeasure] NVARCHAR(64)        '$.participantEffectiveGroupMeasure',
        [characteristic.definition.codeableConcept.id] NVARCHAR(100)       '$.definition.codeableConcept.id',
        [characteristic.definition.codeableConcept.extension] NVARCHAR(MAX)       '$.definition.codeableConcept.extension',
        [characteristic.definition.codeableConcept.coding] NVARCHAR(MAX)       '$.definition.codeableConcept.coding',
        [characteristic.definition.codeableConcept.text] NVARCHAR(4000)      '$.definition.codeableConcept.text',
        [characteristic.definition.canonical] VARCHAR(256)        '$.definition.canonical',
        [characteristic.definition.expression.id] NVARCHAR(100)       '$.definition.expression.id',
        [characteristic.definition.expression.extension] NVARCHAR(MAX)       '$.definition.expression.extension',
        [characteristic.definition.expression.description] NVARCHAR(4000)      '$.definition.expression.description',
        [characteristic.definition.expression.name] VARCHAR(64)         '$.definition.expression.name',
        [characteristic.definition.expression.language] NVARCHAR(64)        '$.definition.expression.language',
        [characteristic.definition.expression.expression] NVARCHAR(4000)      '$.definition.expression.expression',
        [characteristic.definition.expression.reference] VARCHAR(256)        '$.definition.expression.reference',
        [characteristic.definition.dataRequirement.id] NVARCHAR(100)       '$.definition.dataRequirement.id',
        [characteristic.definition.dataRequirement.extension] NVARCHAR(MAX)       '$.definition.dataRequirement.extension',
        [characteristic.definition.dataRequirement.type] NVARCHAR(100)       '$.definition.dataRequirement.type',
        [characteristic.definition.dataRequirement.profile] NVARCHAR(MAX)       '$.definition.dataRequirement.profile',
        [characteristic.definition.dataRequirement.mustSupport] NVARCHAR(MAX)       '$.definition.dataRequirement.mustSupport',
        [characteristic.definition.dataRequirement.codeFilter] NVARCHAR(MAX)       '$.definition.dataRequirement.codeFilter',
        [characteristic.definition.dataRequirement.dateFilter] NVARCHAR(MAX)       '$.definition.dataRequirement.dateFilter',
        [characteristic.definition.dataRequirement.limit] bigint              '$.definition.dataRequirement.limit',
        [characteristic.definition.dataRequirement.sort] NVARCHAR(MAX)       '$.definition.dataRequirement.sort',
        [characteristic.definition.dataRequirement.subject.codeableConcept] NVARCHAR(MAX)       '$.definition.dataRequirement.subject.codeableConcept',
        [characteristic.definition.dataRequirement.subject.reference] NVARCHAR(MAX)       '$.definition.dataRequirement.subject.reference',
        [characteristic.studyEffective.dateTime] VARCHAR(64)         '$.studyEffective.dateTime',
        [characteristic.studyEffective.period.id] NVARCHAR(100)       '$.studyEffective.period.id',
        [characteristic.studyEffective.period.extension] NVARCHAR(MAX)       '$.studyEffective.period.extension',
        [characteristic.studyEffective.period.start] VARCHAR(64)         '$.studyEffective.period.start',
        [characteristic.studyEffective.period.end] VARCHAR(64)         '$.studyEffective.period.end',
        [characteristic.studyEffective.duration.id] NVARCHAR(100)       '$.studyEffective.duration.id',
        [characteristic.studyEffective.duration.extension] NVARCHAR(MAX)       '$.studyEffective.duration.extension',
        [characteristic.studyEffective.duration.value] float               '$.studyEffective.duration.value',
        [characteristic.studyEffective.duration.comparator] NVARCHAR(64)        '$.studyEffective.duration.comparator',
        [characteristic.studyEffective.duration.unit] NVARCHAR(100)       '$.studyEffective.duration.unit',
        [characteristic.studyEffective.duration.system] VARCHAR(256)        '$.studyEffective.duration.system',
        [characteristic.studyEffective.duration.code] NVARCHAR(4000)      '$.studyEffective.duration.code',
        [characteristic.studyEffective.timing.id] NVARCHAR(100)       '$.studyEffective.timing.id',
        [characteristic.studyEffective.timing.extension] NVARCHAR(MAX)       '$.studyEffective.timing.extension',
        [characteristic.studyEffective.timing.modifierExtension] NVARCHAR(MAX)       '$.studyEffective.timing.modifierExtension',
        [characteristic.studyEffective.timing.event] NVARCHAR(MAX)       '$.studyEffective.timing.event',
        [characteristic.studyEffective.timing.repeat] NVARCHAR(MAX)       '$.studyEffective.timing.repeat',
        [characteristic.studyEffective.timing.code] NVARCHAR(MAX)       '$.studyEffective.timing.code',
        [characteristic.participantEffective.dateTime] VARCHAR(64)         '$.participantEffective.dateTime',
        [characteristic.participantEffective.period.id] NVARCHAR(100)       '$.participantEffective.period.id',
        [characteristic.participantEffective.period.extension] NVARCHAR(MAX)       '$.participantEffective.period.extension',
        [characteristic.participantEffective.period.start] VARCHAR(64)         '$.participantEffective.period.start',
        [characteristic.participantEffective.period.end] VARCHAR(64)         '$.participantEffective.period.end',
        [characteristic.participantEffective.duration.id] NVARCHAR(100)       '$.participantEffective.duration.id',
        [characteristic.participantEffective.duration.extension] NVARCHAR(MAX)       '$.participantEffective.duration.extension',
        [characteristic.participantEffective.duration.value] float               '$.participantEffective.duration.value',
        [characteristic.participantEffective.duration.comparator] NVARCHAR(64)        '$.participantEffective.duration.comparator',
        [characteristic.participantEffective.duration.unit] NVARCHAR(100)       '$.participantEffective.duration.unit',
        [characteristic.participantEffective.duration.system] VARCHAR(256)        '$.participantEffective.duration.system',
        [characteristic.participantEffective.duration.code] NVARCHAR(4000)      '$.participantEffective.duration.code',
        [characteristic.participantEffective.timing.id] NVARCHAR(100)       '$.participantEffective.timing.id',
        [characteristic.participantEffective.timing.extension] NVARCHAR(MAX)       '$.participantEffective.timing.extension',
        [characteristic.participantEffective.timing.modifierExtension] NVARCHAR(MAX)       '$.participantEffective.timing.modifierExtension',
        [characteristic.participantEffective.timing.event] NVARCHAR(MAX)       '$.participantEffective.timing.event',
        [characteristic.participantEffective.timing.repeat] NVARCHAR(MAX)       '$.participantEffective.timing.repeat',
        [characteristic.participantEffective.timing.code] NVARCHAR(MAX)       '$.participantEffective.timing.code'
    ) j
