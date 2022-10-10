CREATE EXTERNAL TABLE [fhir].[ConditionDefinition] (
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
    [subtitle] NVARCHAR(4000),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [severity.id] NVARCHAR(100),
    [severity.extension] NVARCHAR(MAX),
    [severity.coding] VARCHAR(MAX),
    [severity.text] NVARCHAR(4000),
    [bodySite.id] NVARCHAR(100),
    [bodySite.extension] NVARCHAR(MAX),
    [bodySite.coding] VARCHAR(MAX),
    [bodySite.text] NVARCHAR(4000),
    [stage.id] NVARCHAR(100),
    [stage.extension] NVARCHAR(MAX),
    [stage.coding] VARCHAR(MAX),
    [stage.text] NVARCHAR(4000),
    [hasSeverity] bit,
    [hasBodySite] bit,
    [hasStage] bit,
    [definition] VARCHAR(MAX),
    [observation] VARCHAR(MAX),
    [medication] VARCHAR(MAX),
    [precondition] VARCHAR(MAX),
    [team] VARCHAR(MAX),
    [questionnaire] VARCHAR(MAX),
    [plan] VARCHAR(MAX),
) WITH (
    LOCATION='/ConditionDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ConditionDefinitionIdentifier AS
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
        BULK 'ConditionDefinition/**',
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

CREATE VIEW fhir.ConditionDefinitionContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'ConditionDefinition/**',
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

CREATE VIEW fhir.ConditionDefinitionUseContext AS
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
        BULK 'ConditionDefinition/**',
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

CREATE VIEW fhir.ConditionDefinitionJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'ConditionDefinition/**',
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

CREATE VIEW fhir.ConditionDefinitionTopic AS
SELECT
    [id],
    [topic.JSON],
    [topic.id],
    [topic.extension],
    [topic.coding],
    [topic.text]
FROM openrowset (
        BULK 'ConditionDefinition/**',
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

CREATE VIEW fhir.ConditionDefinitionAuthor AS
SELECT
    [id],
    [author.JSON],
    [author.id],
    [author.extension],
    [author.name],
    [author.telecom]
FROM openrowset (
        BULK 'ConditionDefinition/**',
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

CREATE VIEW fhir.ConditionDefinitionEditor AS
SELECT
    [id],
    [editor.JSON],
    [editor.id],
    [editor.extension],
    [editor.name],
    [editor.telecom]
FROM openrowset (
        BULK 'ConditionDefinition/**',
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

CREATE VIEW fhir.ConditionDefinitionReviewer AS
SELECT
    [id],
    [reviewer.JSON],
    [reviewer.id],
    [reviewer.extension],
    [reviewer.name],
    [reviewer.telecom]
FROM openrowset (
        BULK 'ConditionDefinition/**',
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

CREATE VIEW fhir.ConditionDefinitionEndorser AS
SELECT
    [id],
    [endorser.JSON],
    [endorser.id],
    [endorser.extension],
    [endorser.name],
    [endorser.telecom]
FROM openrowset (
        BULK 'ConditionDefinition/**',
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

CREATE VIEW fhir.ConditionDefinitionRelatedArtifact AS
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
        BULK 'ConditionDefinition/**',
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

CREATE VIEW fhir.ConditionDefinitionDefinition AS
SELECT
    [id],
    [definition.JSON],
    [definition]
FROM openrowset (
        BULK 'ConditionDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [definition.JSON]  VARCHAR(MAX) '$.definition'
    ) AS rowset
    CROSS APPLY openjson (rowset.[definition.JSON]) with (
        [definition]                   NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ConditionDefinitionObservation AS
SELECT
    [id],
    [observation.JSON],
    [observation.id],
    [observation.extension],
    [observation.modifierExtension],
    [observation.category.id],
    [observation.category.extension],
    [observation.category.coding],
    [observation.category.text],
    [observation.code.id],
    [observation.code.extension],
    [observation.code.coding],
    [observation.code.text]
FROM openrowset (
        BULK 'ConditionDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [observation.JSON]  VARCHAR(MAX) '$.observation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[observation.JSON]) with (
        [observation.id]               NVARCHAR(100)       '$.id',
        [observation.extension]        NVARCHAR(MAX)       '$.extension',
        [observation.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [observation.category.id]      NVARCHAR(100)       '$.category.id',
        [observation.category.extension] NVARCHAR(MAX)       '$.category.extension',
        [observation.category.coding]  NVARCHAR(MAX)       '$.category.coding',
        [observation.category.text]    NVARCHAR(4000)      '$.category.text',
        [observation.code.id]          NVARCHAR(100)       '$.code.id',
        [observation.code.extension]   NVARCHAR(MAX)       '$.code.extension',
        [observation.code.coding]      NVARCHAR(MAX)       '$.code.coding',
        [observation.code.text]        NVARCHAR(4000)      '$.code.text'
    ) j

GO

CREATE VIEW fhir.ConditionDefinitionMedication AS
SELECT
    [id],
    [medication.JSON],
    [medication.id],
    [medication.extension],
    [medication.modifierExtension],
    [medication.category.id],
    [medication.category.extension],
    [medication.category.coding],
    [medication.category.text],
    [medication.code.id],
    [medication.code.extension],
    [medication.code.coding],
    [medication.code.text]
FROM openrowset (
        BULK 'ConditionDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [medication.JSON]  VARCHAR(MAX) '$.medication'
    ) AS rowset
    CROSS APPLY openjson (rowset.[medication.JSON]) with (
        [medication.id]                NVARCHAR(100)       '$.id',
        [medication.extension]         NVARCHAR(MAX)       '$.extension',
        [medication.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [medication.category.id]       NVARCHAR(100)       '$.category.id',
        [medication.category.extension] NVARCHAR(MAX)       '$.category.extension',
        [medication.category.coding]   NVARCHAR(MAX)       '$.category.coding',
        [medication.category.text]     NVARCHAR(4000)      '$.category.text',
        [medication.code.id]           NVARCHAR(100)       '$.code.id',
        [medication.code.extension]    NVARCHAR(MAX)       '$.code.extension',
        [medication.code.coding]       NVARCHAR(MAX)       '$.code.coding',
        [medication.code.text]         NVARCHAR(4000)      '$.code.text'
    ) j

GO

CREATE VIEW fhir.ConditionDefinitionPrecondition AS
SELECT
    [id],
    [precondition.JSON],
    [precondition.id],
    [precondition.extension],
    [precondition.modifierExtension],
    [precondition.type],
    [precondition.code.id],
    [precondition.code.extension],
    [precondition.code.coding],
    [precondition.code.text],
    [precondition.value.codeableConcept.id],
    [precondition.value.codeableConcept.extension],
    [precondition.value.codeableConcept.coding],
    [precondition.value.codeableConcept.text],
    [precondition.value.quantity.id],
    [precondition.value.quantity.extension],
    [precondition.value.quantity.value],
    [precondition.value.quantity.comparator],
    [precondition.value.quantity.unit],
    [precondition.value.quantity.system],
    [precondition.value.quantity.code]
FROM openrowset (
        BULK 'ConditionDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [precondition.JSON]  VARCHAR(MAX) '$.precondition'
    ) AS rowset
    CROSS APPLY openjson (rowset.[precondition.JSON]) with (
        [precondition.id]              NVARCHAR(100)       '$.id',
        [precondition.extension]       NVARCHAR(MAX)       '$.extension',
        [precondition.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [precondition.type]            NVARCHAR(100)       '$.type',
        [precondition.code.id]         NVARCHAR(100)       '$.code.id',
        [precondition.code.extension]  NVARCHAR(MAX)       '$.code.extension',
        [precondition.code.coding]     NVARCHAR(MAX)       '$.code.coding',
        [precondition.code.text]       NVARCHAR(4000)      '$.code.text',
        [precondition.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [precondition.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [precondition.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [precondition.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [precondition.value.quantity.id] NVARCHAR(100)       '$.value.quantity.id',
        [precondition.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [precondition.value.quantity.value] float               '$.value.quantity.value',
        [precondition.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [precondition.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [precondition.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [precondition.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code'
    ) j

GO

CREATE VIEW fhir.ConditionDefinitionTeam AS
SELECT
    [id],
    [team.JSON],
    [team.id],
    [team.extension],
    [team.reference],
    [team.type],
    [team.identifier.id],
    [team.identifier.extension],
    [team.identifier.use],
    [team.identifier.type],
    [team.identifier.system],
    [team.identifier.value],
    [team.identifier.period],
    [team.identifier.assigner],
    [team.display]
FROM openrowset (
        BULK 'ConditionDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [team.JSON]  VARCHAR(MAX) '$.team'
    ) AS rowset
    CROSS APPLY openjson (rowset.[team.JSON]) with (
        [team.id]                      NVARCHAR(100)       '$.id',
        [team.extension]               NVARCHAR(MAX)       '$.extension',
        [team.reference]               NVARCHAR(4000)      '$.reference',
        [team.type]                    VARCHAR(256)        '$.type',
        [team.identifier.id]           NVARCHAR(100)       '$.identifier.id',
        [team.identifier.extension]    NVARCHAR(MAX)       '$.identifier.extension',
        [team.identifier.use]          NVARCHAR(64)        '$.identifier.use',
        [team.identifier.type]         NVARCHAR(MAX)       '$.identifier.type',
        [team.identifier.system]       VARCHAR(256)        '$.identifier.system',
        [team.identifier.value]        NVARCHAR(4000)      '$.identifier.value',
        [team.identifier.period]       NVARCHAR(MAX)       '$.identifier.period',
        [team.identifier.assigner]     NVARCHAR(MAX)       '$.identifier.assigner',
        [team.display]                 NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ConditionDefinitionQuestionnaire AS
SELECT
    [id],
    [questionnaire.JSON],
    [questionnaire.id],
    [questionnaire.extension],
    [questionnaire.modifierExtension],
    [questionnaire.purpose],
    [questionnaire.reference.id],
    [questionnaire.reference.extension],
    [questionnaire.reference.reference],
    [questionnaire.reference.type],
    [questionnaire.reference.identifier],
    [questionnaire.reference.display]
FROM openrowset (
        BULK 'ConditionDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [questionnaire.JSON]  VARCHAR(MAX) '$.questionnaire'
    ) AS rowset
    CROSS APPLY openjson (rowset.[questionnaire.JSON]) with (
        [questionnaire.id]             NVARCHAR(100)       '$.id',
        [questionnaire.extension]      NVARCHAR(MAX)       '$.extension',
        [questionnaire.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [questionnaire.purpose]        NVARCHAR(4000)      '$.purpose',
        [questionnaire.reference.id]   NVARCHAR(100)       '$.reference.id',
        [questionnaire.reference.extension] NVARCHAR(MAX)       '$.reference.extension',
        [questionnaire.reference.reference] NVARCHAR(4000)      '$.reference.reference',
        [questionnaire.reference.type] VARCHAR(256)        '$.reference.type',
        [questionnaire.reference.identifier] NVARCHAR(MAX)       '$.reference.identifier',
        [questionnaire.reference.display] NVARCHAR(4000)      '$.reference.display'
    ) j

GO

CREATE VIEW fhir.ConditionDefinitionPlan AS
SELECT
    [id],
    [plan.JSON],
    [plan.id],
    [plan.extension],
    [plan.modifierExtension],
    [plan.role.id],
    [plan.role.extension],
    [plan.role.coding],
    [plan.role.text],
    [plan.reference.id],
    [plan.reference.extension],
    [plan.reference.reference],
    [plan.reference.type],
    [plan.reference.identifier],
    [plan.reference.display]
FROM openrowset (
        BULK 'ConditionDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [plan.JSON]  VARCHAR(MAX) '$.plan'
    ) AS rowset
    CROSS APPLY openjson (rowset.[plan.JSON]) with (
        [plan.id]                      NVARCHAR(100)       '$.id',
        [plan.extension]               NVARCHAR(MAX)       '$.extension',
        [plan.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [plan.role.id]                 NVARCHAR(100)       '$.role.id',
        [plan.role.extension]          NVARCHAR(MAX)       '$.role.extension',
        [plan.role.coding]             NVARCHAR(MAX)       '$.role.coding',
        [plan.role.text]               NVARCHAR(4000)      '$.role.text',
        [plan.reference.id]            NVARCHAR(100)       '$.reference.id',
        [plan.reference.extension]     NVARCHAR(MAX)       '$.reference.extension',
        [plan.reference.reference]     NVARCHAR(4000)      '$.reference.reference',
        [plan.reference.type]          VARCHAR(256)        '$.reference.type',
        [plan.reference.identifier]    NVARCHAR(MAX)       '$.reference.identifier',
        [plan.reference.display]       NVARCHAR(4000)      '$.reference.display'
    ) j
