CREATE EXTERNAL TABLE [fhir].[EvidenceVariable] (
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
    [shortTitle] NVARCHAR(4000),
    [subtitle] NVARCHAR(4000),
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
    [type] NVARCHAR(64),
    [characteristic] VARCHAR(MAX),
) WITH (
    LOCATION='/EvidenceVariable/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.EvidenceVariableIdentifier AS
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
        BULK 'EvidenceVariable/**',
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

CREATE VIEW fhir.EvidenceVariableContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'EvidenceVariable/**',
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

CREATE VIEW fhir.EvidenceVariableNote AS
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
        BULK 'EvidenceVariable/**',
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

CREATE VIEW fhir.EvidenceVariableUseContext AS
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
        BULK 'EvidenceVariable/**',
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

CREATE VIEW fhir.EvidenceVariableJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'EvidenceVariable/**',
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

CREATE VIEW fhir.EvidenceVariableTopic AS
SELECT
    [id],
    [topic.JSON],
    [topic.id],
    [topic.extension],
    [topic.coding],
    [topic.text]
FROM openrowset (
        BULK 'EvidenceVariable/**',
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

CREATE VIEW fhir.EvidenceVariableAuthor AS
SELECT
    [id],
    [author.JSON],
    [author.id],
    [author.extension],
    [author.name],
    [author.telecom]
FROM openrowset (
        BULK 'EvidenceVariable/**',
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

CREATE VIEW fhir.EvidenceVariableEditor AS
SELECT
    [id],
    [editor.JSON],
    [editor.id],
    [editor.extension],
    [editor.name],
    [editor.telecom]
FROM openrowset (
        BULK 'EvidenceVariable/**',
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

CREATE VIEW fhir.EvidenceVariableReviewer AS
SELECT
    [id],
    [reviewer.JSON],
    [reviewer.id],
    [reviewer.extension],
    [reviewer.name],
    [reviewer.telecom]
FROM openrowset (
        BULK 'EvidenceVariable/**',
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

CREATE VIEW fhir.EvidenceVariableEndorser AS
SELECT
    [id],
    [endorser.JSON],
    [endorser.id],
    [endorser.extension],
    [endorser.name],
    [endorser.telecom]
FROM openrowset (
        BULK 'EvidenceVariable/**',
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

CREATE VIEW fhir.EvidenceVariableRelatedArtifact AS
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
        BULK 'EvidenceVariable/**',
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

CREATE VIEW fhir.EvidenceVariableCharacteristic AS
SELECT
    [id],
    [characteristic.JSON],
    [characteristic.id],
    [characteristic.extension],
    [characteristic.modifierExtension],
    [characteristic.description],
    [characteristic.usageContext],
    [characteristic.exclude],
    [characteristic.timeFromStart.id],
    [characteristic.timeFromStart.extension],
    [characteristic.timeFromStart.value],
    [characteristic.timeFromStart.comparator],
    [characteristic.timeFromStart.unit],
    [characteristic.timeFromStart.system],
    [characteristic.timeFromStart.code],
    [characteristic.groupMeasure],
    [characteristic.definition.Reference.id],
    [characteristic.definition.Reference.extension],
    [characteristic.definition.Reference.reference],
    [characteristic.definition.Reference.type],
    [characteristic.definition.Reference.identifier],
    [characteristic.definition.Reference.display],
    [characteristic.definition.canonical],
    [characteristic.definition.CodeableConcept.id],
    [characteristic.definition.CodeableConcept.extension],
    [characteristic.definition.CodeableConcept.coding],
    [characteristic.definition.CodeableConcept.text],
    [characteristic.definition.Expression.id],
    [characteristic.definition.Expression.extension],
    [characteristic.definition.Expression.description],
    [characteristic.definition.Expression.name],
    [characteristic.definition.Expression.language],
    [characteristic.definition.Expression.expression],
    [characteristic.definition.Expression.reference],
    [characteristic.definition.DataRequirement.id],
    [characteristic.definition.DataRequirement.extension],
    [characteristic.definition.DataRequirement.type],
    [characteristic.definition.DataRequirement.profile],
    [characteristic.definition.DataRequirement.mustSupport],
    [characteristic.definition.DataRequirement.codeFilter],
    [characteristic.definition.DataRequirement.dateFilter],
    [characteristic.definition.DataRequirement.limit],
    [characteristic.definition.DataRequirement.sort],
    [characteristic.definition.DataRequirement.subject.CodeableConcept],
    [characteristic.definition.DataRequirement.subject.Reference],
    [characteristic.definition.TriggerDefinition.id],
    [characteristic.definition.TriggerDefinition.extension],
    [characteristic.definition.TriggerDefinition.type],
    [characteristic.definition.TriggerDefinition.name],
    [characteristic.definition.TriggerDefinition.data],
    [characteristic.definition.TriggerDefinition.condition],
    [characteristic.definition.TriggerDefinition.timing.Timing],
    [characteristic.definition.TriggerDefinition.timing.Reference],
    [characteristic.definition.TriggerDefinition.timing.date],
    [characteristic.definition.TriggerDefinition.timing.dateTime],
    [characteristic.participantEffective.dateTime],
    [characteristic.participantEffective.Period.id],
    [characteristic.participantEffective.Period.extension],
    [characteristic.participantEffective.Period.start],
    [characteristic.participantEffective.Period.end],
    [characteristic.participantEffective.Duration.id],
    [characteristic.participantEffective.Duration.extension],
    [characteristic.participantEffective.Duration.value],
    [characteristic.participantEffective.Duration.comparator],
    [characteristic.participantEffective.Duration.unit],
    [characteristic.participantEffective.Duration.system],
    [characteristic.participantEffective.Duration.code],
    [characteristic.participantEffective.Timing.id],
    [characteristic.participantEffective.Timing.extension],
    [characteristic.participantEffective.Timing.modifierExtension],
    [characteristic.participantEffective.Timing.event],
    [characteristic.participantEffective.Timing.repeat],
    [characteristic.participantEffective.Timing.code]
FROM openrowset (
        BULK 'EvidenceVariable/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [characteristic.JSON]  VARCHAR(MAX) '$.characteristic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[characteristic.JSON]) with (
        [characteristic.id]            NVARCHAR(4000)      '$.id',
        [characteristic.extension]     NVARCHAR(MAX)       '$.extension',
        [characteristic.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [characteristic.description]   NVARCHAR(4000)      '$.description',
        [characteristic.usageContext]  NVARCHAR(MAX)       '$.usageContext' AS JSON,
        [characteristic.exclude]       bit                 '$.exclude',
        [characteristic.timeFromStart.id] NVARCHAR(4000)      '$.timeFromStart.id',
        [characteristic.timeFromStart.extension] NVARCHAR(MAX)       '$.timeFromStart.extension',
        [characteristic.timeFromStart.value] float               '$.timeFromStart.value',
        [characteristic.timeFromStart.comparator] NVARCHAR(64)        '$.timeFromStart.comparator',
        [characteristic.timeFromStart.unit] NVARCHAR(4000)      '$.timeFromStart.unit',
        [characteristic.timeFromStart.system] VARCHAR(256)        '$.timeFromStart.system',
        [characteristic.timeFromStart.code] NVARCHAR(4000)      '$.timeFromStart.code',
        [characteristic.groupMeasure]  NVARCHAR(64)        '$.groupMeasure',
        [characteristic.definition.Reference.id] NVARCHAR(4000)      '$.definition.Reference.id',
        [characteristic.definition.Reference.extension] NVARCHAR(MAX)       '$.definition.Reference.extension',
        [characteristic.definition.Reference.reference] NVARCHAR(4000)      '$.definition.Reference.reference',
        [characteristic.definition.Reference.type] VARCHAR(256)        '$.definition.Reference.type',
        [characteristic.definition.Reference.identifier] NVARCHAR(MAX)       '$.definition.Reference.identifier',
        [characteristic.definition.Reference.display] NVARCHAR(4000)      '$.definition.Reference.display',
        [characteristic.definition.canonical] VARCHAR(256)        '$.definition.canonical',
        [characteristic.definition.CodeableConcept.id] NVARCHAR(4000)      '$.definition.CodeableConcept.id',
        [characteristic.definition.CodeableConcept.extension] NVARCHAR(MAX)       '$.definition.CodeableConcept.extension',
        [characteristic.definition.CodeableConcept.coding] NVARCHAR(MAX)       '$.definition.CodeableConcept.coding',
        [characteristic.definition.CodeableConcept.text] NVARCHAR(4000)      '$.definition.CodeableConcept.text',
        [characteristic.definition.Expression.id] NVARCHAR(4000)      '$.definition.Expression.id',
        [characteristic.definition.Expression.extension] NVARCHAR(MAX)       '$.definition.Expression.extension',
        [characteristic.definition.Expression.description] NVARCHAR(4000)      '$.definition.Expression.description',
        [characteristic.definition.Expression.name] VARCHAR(64)         '$.definition.Expression.name',
        [characteristic.definition.Expression.language] NVARCHAR(64)        '$.definition.Expression.language',
        [characteristic.definition.Expression.expression] NVARCHAR(4000)      '$.definition.Expression.expression',
        [characteristic.definition.Expression.reference] VARCHAR(256)        '$.definition.Expression.reference',
        [characteristic.definition.DataRequirement.id] NVARCHAR(4000)      '$.definition.DataRequirement.id',
        [characteristic.definition.DataRequirement.extension] NVARCHAR(MAX)       '$.definition.DataRequirement.extension',
        [characteristic.definition.DataRequirement.type] NVARCHAR(4000)      '$.definition.DataRequirement.type',
        [characteristic.definition.DataRequirement.profile] NVARCHAR(MAX)       '$.definition.DataRequirement.profile',
        [characteristic.definition.DataRequirement.mustSupport] NVARCHAR(MAX)       '$.definition.DataRequirement.mustSupport',
        [characteristic.definition.DataRequirement.codeFilter] NVARCHAR(MAX)       '$.definition.DataRequirement.codeFilter',
        [characteristic.definition.DataRequirement.dateFilter] NVARCHAR(MAX)       '$.definition.DataRequirement.dateFilter',
        [characteristic.definition.DataRequirement.limit] bigint              '$.definition.DataRequirement.limit',
        [characteristic.definition.DataRequirement.sort] NVARCHAR(MAX)       '$.definition.DataRequirement.sort',
        [characteristic.definition.DataRequirement.subject.CodeableConcept] NVARCHAR(MAX)       '$.definition.DataRequirement.subject.CodeableConcept',
        [characteristic.definition.DataRequirement.subject.Reference] NVARCHAR(MAX)       '$.definition.DataRequirement.subject.Reference',
        [characteristic.definition.TriggerDefinition.id] NVARCHAR(4000)      '$.definition.TriggerDefinition.id',
        [characteristic.definition.TriggerDefinition.extension] NVARCHAR(MAX)       '$.definition.TriggerDefinition.extension',
        [characteristic.definition.TriggerDefinition.type] NVARCHAR(64)        '$.definition.TriggerDefinition.type',
        [characteristic.definition.TriggerDefinition.name] NVARCHAR(4000)      '$.definition.TriggerDefinition.name',
        [characteristic.definition.TriggerDefinition.data] NVARCHAR(MAX)       '$.definition.TriggerDefinition.data',
        [characteristic.definition.TriggerDefinition.condition] NVARCHAR(MAX)       '$.definition.TriggerDefinition.condition',
        [characteristic.definition.TriggerDefinition.timing.Timing] NVARCHAR(MAX)       '$.definition.TriggerDefinition.timing.Timing',
        [characteristic.definition.TriggerDefinition.timing.Reference] NVARCHAR(MAX)       '$.definition.TriggerDefinition.timing.Reference',
        [characteristic.definition.TriggerDefinition.timing.date] VARCHAR(10)         '$.definition.TriggerDefinition.timing.date',
        [characteristic.definition.TriggerDefinition.timing.dateTime] VARCHAR(30)         '$.definition.TriggerDefinition.timing.dateTime',
        [characteristic.participantEffective.dateTime] VARCHAR(30)         '$.participantEffective.dateTime',
        [characteristic.participantEffective.Period.id] NVARCHAR(4000)      '$.participantEffective.Period.id',
        [characteristic.participantEffective.Period.extension] NVARCHAR(MAX)       '$.participantEffective.Period.extension',
        [characteristic.participantEffective.Period.start] VARCHAR(30)         '$.participantEffective.Period.start',
        [characteristic.participantEffective.Period.end] VARCHAR(30)         '$.participantEffective.Period.end',
        [characteristic.participantEffective.Duration.id] NVARCHAR(4000)      '$.participantEffective.Duration.id',
        [characteristic.participantEffective.Duration.extension] NVARCHAR(MAX)       '$.participantEffective.Duration.extension',
        [characteristic.participantEffective.Duration.value] float               '$.participantEffective.Duration.value',
        [characteristic.participantEffective.Duration.comparator] NVARCHAR(64)        '$.participantEffective.Duration.comparator',
        [characteristic.participantEffective.Duration.unit] NVARCHAR(4000)      '$.participantEffective.Duration.unit',
        [characteristic.participantEffective.Duration.system] VARCHAR(256)        '$.participantEffective.Duration.system',
        [characteristic.participantEffective.Duration.code] NVARCHAR(4000)      '$.participantEffective.Duration.code',
        [characteristic.participantEffective.Timing.id] NVARCHAR(4000)      '$.participantEffective.Timing.id',
        [characteristic.participantEffective.Timing.extension] NVARCHAR(MAX)       '$.participantEffective.Timing.extension',
        [characteristic.participantEffective.Timing.modifierExtension] NVARCHAR(MAX)       '$.participantEffective.Timing.modifierExtension',
        [characteristic.participantEffective.Timing.event] NVARCHAR(MAX)       '$.participantEffective.Timing.event',
        [characteristic.participantEffective.Timing.repeat] NVARCHAR(MAX)       '$.participantEffective.Timing.repeat',
        [characteristic.participantEffective.Timing.code] NVARCHAR(MAX)       '$.participantEffective.Timing.code'
    ) j
