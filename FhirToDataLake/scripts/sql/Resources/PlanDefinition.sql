CREATE EXTERNAL TABLE [fhir].[PlanDefinition] (
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
    [subtitle] NVARCHAR(4000),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [status] NVARCHAR(64),
    [experimental] bit,
    [date] VARCHAR(64),
    [publisher] NVARCHAR(500),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
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
    [goal] VARCHAR(MAX),
    [action] VARCHAR(MAX),
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
    LOCATION='/PlanDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.PlanDefinitionIdentifier AS
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
        BULK 'PlanDefinition/**',
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

CREATE VIEW fhir.PlanDefinitionContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'PlanDefinition/**',
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

CREATE VIEW fhir.PlanDefinitionUseContext AS
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
        BULK 'PlanDefinition/**',
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

CREATE VIEW fhir.PlanDefinitionJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'PlanDefinition/**',
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

CREATE VIEW fhir.PlanDefinitionTopic AS
SELECT
    [id],
    [topic.JSON],
    [topic.id],
    [topic.extension],
    [topic.coding],
    [topic.text]
FROM openrowset (
        BULK 'PlanDefinition/**',
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

CREATE VIEW fhir.PlanDefinitionAuthor AS
SELECT
    [id],
    [author.JSON],
    [author.id],
    [author.extension],
    [author.name],
    [author.telecom]
FROM openrowset (
        BULK 'PlanDefinition/**',
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

CREATE VIEW fhir.PlanDefinitionEditor AS
SELECT
    [id],
    [editor.JSON],
    [editor.id],
    [editor.extension],
    [editor.name],
    [editor.telecom]
FROM openrowset (
        BULK 'PlanDefinition/**',
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

CREATE VIEW fhir.PlanDefinitionReviewer AS
SELECT
    [id],
    [reviewer.JSON],
    [reviewer.id],
    [reviewer.extension],
    [reviewer.name],
    [reviewer.telecom]
FROM openrowset (
        BULK 'PlanDefinition/**',
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

CREATE VIEW fhir.PlanDefinitionEndorser AS
SELECT
    [id],
    [endorser.JSON],
    [endorser.id],
    [endorser.extension],
    [endorser.name],
    [endorser.telecom]
FROM openrowset (
        BULK 'PlanDefinition/**',
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

CREATE VIEW fhir.PlanDefinitionRelatedArtifact AS
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
        BULK 'PlanDefinition/**',
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

CREATE VIEW fhir.PlanDefinitionLibrary AS
SELECT
    [id],
    [library.JSON],
    [library]
FROM openrowset (
        BULK 'PlanDefinition/**',
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

CREATE VIEW fhir.PlanDefinitionGoal AS
SELECT
    [id],
    [goal.JSON],
    [goal.id],
    [goal.extension],
    [goal.modifierExtension],
    [goal.category.id],
    [goal.category.extension],
    [goal.category.coding],
    [goal.category.text],
    [goal.description.id],
    [goal.description.extension],
    [goal.description.coding],
    [goal.description.text],
    [goal.priority.id],
    [goal.priority.extension],
    [goal.priority.coding],
    [goal.priority.text],
    [goal.start.id],
    [goal.start.extension],
    [goal.start.coding],
    [goal.start.text],
    [goal.addresses],
    [goal.documentation],
    [goal.target]
FROM openrowset (
        BULK 'PlanDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [goal.JSON]  VARCHAR(MAX) '$.goal'
    ) AS rowset
    CROSS APPLY openjson (rowset.[goal.JSON]) with (
        [goal.id]                      NVARCHAR(100)       '$.id',
        [goal.extension]               NVARCHAR(MAX)       '$.extension',
        [goal.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [goal.category.id]             NVARCHAR(100)       '$.category.id',
        [goal.category.extension]      NVARCHAR(MAX)       '$.category.extension',
        [goal.category.coding]         NVARCHAR(MAX)       '$.category.coding',
        [goal.category.text]           NVARCHAR(4000)      '$.category.text',
        [goal.description.id]          NVARCHAR(100)       '$.description.id',
        [goal.description.extension]   NVARCHAR(MAX)       '$.description.extension',
        [goal.description.coding]      NVARCHAR(MAX)       '$.description.coding',
        [goal.description.text]        NVARCHAR(4000)      '$.description.text',
        [goal.priority.id]             NVARCHAR(100)       '$.priority.id',
        [goal.priority.extension]      NVARCHAR(MAX)       '$.priority.extension',
        [goal.priority.coding]         NVARCHAR(MAX)       '$.priority.coding',
        [goal.priority.text]           NVARCHAR(4000)      '$.priority.text',
        [goal.start.id]                NVARCHAR(100)       '$.start.id',
        [goal.start.extension]         NVARCHAR(MAX)       '$.start.extension',
        [goal.start.coding]            NVARCHAR(MAX)       '$.start.coding',
        [goal.start.text]              NVARCHAR(4000)      '$.start.text',
        [goal.addresses]               NVARCHAR(MAX)       '$.addresses' AS JSON,
        [goal.documentation]           NVARCHAR(MAX)       '$.documentation' AS JSON,
        [goal.target]                  NVARCHAR(MAX)       '$.target' AS JSON
    ) j

GO

CREATE VIEW fhir.PlanDefinitionAction AS
SELECT
    [id],
    [action.JSON],
    [action.id],
    [action.extension],
    [action.modifierExtension],
    [action.prefix],
    [action.title],
    [action.description],
    [action.textEquivalent],
    [action.priority],
    [action.code],
    [action.reason],
    [action.documentation],
    [action.goalId],
    [action.trigger],
    [action.condition],
    [action.input],
    [action.output],
    [action.relatedAction],
    [action.participant],
    [action.type.id],
    [action.type.extension],
    [action.type.coding],
    [action.type.text],
    [action.groupingBehavior],
    [action.selectionBehavior],
    [action.requiredBehavior],
    [action.precheckBehavior],
    [action.cardinalityBehavior],
    [action.transform],
    [action.dynamicValue],
    [action.action],
    [action.subject.codeableConcept.id],
    [action.subject.codeableConcept.extension],
    [action.subject.codeableConcept.coding],
    [action.subject.codeableConcept.text],
    [action.subject.reference.id],
    [action.subject.reference.extension],
    [action.subject.reference.reference],
    [action.subject.reference.type],
    [action.subject.reference.identifier],
    [action.subject.reference.display],
    [action.timing.dateTime],
    [action.timing.age.id],
    [action.timing.age.extension],
    [action.timing.age.value],
    [action.timing.age.comparator],
    [action.timing.age.unit],
    [action.timing.age.system],
    [action.timing.age.code],
    [action.timing.period.id],
    [action.timing.period.extension],
    [action.timing.period.start],
    [action.timing.period.end],
    [action.timing.duration.id],
    [action.timing.duration.extension],
    [action.timing.duration.value],
    [action.timing.duration.comparator],
    [action.timing.duration.unit],
    [action.timing.duration.system],
    [action.timing.duration.code],
    [action.timing.range.id],
    [action.timing.range.extension],
    [action.timing.range.low],
    [action.timing.range.high],
    [action.timing.timing.id],
    [action.timing.timing.extension],
    [action.timing.timing.modifierExtension],
    [action.timing.timing.event],
    [action.timing.timing.repeat],
    [action.timing.timing.code],
    [action.definition.canonical],
    [action.definition.uri]
FROM openrowset (
        BULK 'PlanDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [action.JSON]  VARCHAR(MAX) '$.action'
    ) AS rowset
    CROSS APPLY openjson (rowset.[action.JSON]) with (
        [action.id]                    NVARCHAR(100)       '$.id',
        [action.extension]             NVARCHAR(MAX)       '$.extension',
        [action.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [action.prefix]                NVARCHAR(500)       '$.prefix',
        [action.title]                 NVARCHAR(4000)      '$.title',
        [action.description]           NVARCHAR(4000)      '$.description',
        [action.textEquivalent]        NVARCHAR(4000)      '$.textEquivalent',
        [action.priority]              NVARCHAR(100)       '$.priority',
        [action.code]                  NVARCHAR(MAX)       '$.code' AS JSON,
        [action.reason]                NVARCHAR(MAX)       '$.reason' AS JSON,
        [action.documentation]         NVARCHAR(MAX)       '$.documentation' AS JSON,
        [action.goalId]                NVARCHAR(MAX)       '$.goalId' AS JSON,
        [action.trigger]               NVARCHAR(MAX)       '$.trigger' AS JSON,
        [action.condition]             NVARCHAR(MAX)       '$.condition' AS JSON,
        [action.input]                 NVARCHAR(MAX)       '$.input' AS JSON,
        [action.output]                NVARCHAR(MAX)       '$.output' AS JSON,
        [action.relatedAction]         NVARCHAR(MAX)       '$.relatedAction' AS JSON,
        [action.participant]           NVARCHAR(MAX)       '$.participant' AS JSON,
        [action.type.id]               NVARCHAR(100)       '$.type.id',
        [action.type.extension]        NVARCHAR(MAX)       '$.type.extension',
        [action.type.coding]           NVARCHAR(MAX)       '$.type.coding',
        [action.type.text]             NVARCHAR(4000)      '$.type.text',
        [action.groupingBehavior]      NVARCHAR(64)        '$.groupingBehavior',
        [action.selectionBehavior]     NVARCHAR(64)        '$.selectionBehavior',
        [action.requiredBehavior]      NVARCHAR(64)        '$.requiredBehavior',
        [action.precheckBehavior]      NVARCHAR(64)        '$.precheckBehavior',
        [action.cardinalityBehavior]   NVARCHAR(64)        '$.cardinalityBehavior',
        [action.transform]             VARCHAR(256)        '$.transform',
        [action.dynamicValue]          NVARCHAR(MAX)       '$.dynamicValue' AS JSON,
        [action.action]                NVARCHAR(MAX)       '$.action' AS JSON,
        [action.subject.codeableConcept.id] NVARCHAR(100)       '$.subject.codeableConcept.id',
        [action.subject.codeableConcept.extension] NVARCHAR(MAX)       '$.subject.codeableConcept.extension',
        [action.subject.codeableConcept.coding] NVARCHAR(MAX)       '$.subject.codeableConcept.coding',
        [action.subject.codeableConcept.text] NVARCHAR(4000)      '$.subject.codeableConcept.text',
        [action.subject.reference.id]  NVARCHAR(100)       '$.subject.reference.id',
        [action.subject.reference.extension] NVARCHAR(MAX)       '$.subject.reference.extension',
        [action.subject.reference.reference] NVARCHAR(4000)      '$.subject.reference.reference',
        [action.subject.reference.type] VARCHAR(256)        '$.subject.reference.type',
        [action.subject.reference.identifier] NVARCHAR(MAX)       '$.subject.reference.identifier',
        [action.subject.reference.display] NVARCHAR(4000)      '$.subject.reference.display',
        [action.timing.dateTime]       VARCHAR(64)         '$.timing.dateTime',
        [action.timing.age.id]         NVARCHAR(100)       '$.timing.age.id',
        [action.timing.age.extension]  NVARCHAR(MAX)       '$.timing.age.extension',
        [action.timing.age.value]      float               '$.timing.age.value',
        [action.timing.age.comparator] NVARCHAR(64)        '$.timing.age.comparator',
        [action.timing.age.unit]       NVARCHAR(100)       '$.timing.age.unit',
        [action.timing.age.system]     VARCHAR(256)        '$.timing.age.system',
        [action.timing.age.code]       NVARCHAR(4000)      '$.timing.age.code',
        [action.timing.period.id]      NVARCHAR(100)       '$.timing.period.id',
        [action.timing.period.extension] NVARCHAR(MAX)       '$.timing.period.extension',
        [action.timing.period.start]   VARCHAR(64)         '$.timing.period.start',
        [action.timing.period.end]     VARCHAR(64)         '$.timing.period.end',
        [action.timing.duration.id]    NVARCHAR(100)       '$.timing.duration.id',
        [action.timing.duration.extension] NVARCHAR(MAX)       '$.timing.duration.extension',
        [action.timing.duration.value] float               '$.timing.duration.value',
        [action.timing.duration.comparator] NVARCHAR(64)        '$.timing.duration.comparator',
        [action.timing.duration.unit]  NVARCHAR(100)       '$.timing.duration.unit',
        [action.timing.duration.system] VARCHAR(256)        '$.timing.duration.system',
        [action.timing.duration.code]  NVARCHAR(4000)      '$.timing.duration.code',
        [action.timing.range.id]       NVARCHAR(100)       '$.timing.range.id',
        [action.timing.range.extension] NVARCHAR(MAX)       '$.timing.range.extension',
        [action.timing.range.low]      NVARCHAR(MAX)       '$.timing.range.low',
        [action.timing.range.high]     NVARCHAR(MAX)       '$.timing.range.high',
        [action.timing.timing.id]      NVARCHAR(100)       '$.timing.timing.id',
        [action.timing.timing.extension] NVARCHAR(MAX)       '$.timing.timing.extension',
        [action.timing.timing.modifierExtension] NVARCHAR(MAX)       '$.timing.timing.modifierExtension',
        [action.timing.timing.event]   NVARCHAR(MAX)       '$.timing.timing.event',
        [action.timing.timing.repeat]  NVARCHAR(MAX)       '$.timing.timing.repeat',
        [action.timing.timing.code]    NVARCHAR(MAX)       '$.timing.timing.code',
        [action.definition.canonical]  VARCHAR(256)        '$.definition.canonical',
        [action.definition.uri]        VARCHAR(256)        '$.definition.uri'
    ) j
