CREATE EXTERNAL TABLE [fhir].[ActivityDefinition] (
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
    [subtitle] NVARCHAR(4000),
    [status] NVARCHAR(64),
    [experimental] bit,
    [date] VARCHAR(30),
    [publisher] NVARCHAR(4000),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [purpose] NVARCHAR(MAX),
    [usage] NVARCHAR(4000),
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
    [library] VARCHAR(MAX),
    [kind] NVARCHAR(4000),
    [profile] VARCHAR(256),
    [code.id] NVARCHAR(4000),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [intent] NVARCHAR(4000),
    [priority] NVARCHAR(4000),
    [doNotPerform] bit,
    [location.id] NVARCHAR(4000),
    [location.extension] NVARCHAR(MAX),
    [location.reference] NVARCHAR(4000),
    [location.type] VARCHAR(256),
    [location.identifier.id] NVARCHAR(4000),
    [location.identifier.extension] NVARCHAR(MAX),
    [location.identifier.use] NVARCHAR(64),
    [location.identifier.type] NVARCHAR(MAX),
    [location.identifier.system] VARCHAR(256),
    [location.identifier.value] NVARCHAR(4000),
    [location.identifier.period] NVARCHAR(MAX),
    [location.identifier.assigner] NVARCHAR(MAX),
    [location.display] NVARCHAR(4000),
    [participant] VARCHAR(MAX),
    [quantity.id] NVARCHAR(4000),
    [quantity.extension] NVARCHAR(MAX),
    [quantity.value] float,
    [quantity.comparator] NVARCHAR(64),
    [quantity.unit] NVARCHAR(4000),
    [quantity.system] VARCHAR(256),
    [quantity.code] NVARCHAR(4000),
    [dosage] VARCHAR(MAX),
    [bodySite] VARCHAR(MAX),
    [specimenRequirement] VARCHAR(MAX),
    [observationRequirement] VARCHAR(MAX),
    [observationResultRequirement] VARCHAR(MAX),
    [transform] VARCHAR(256),
    [dynamicValue] VARCHAR(MAX),
    [subject.CodeableConcept.id] NVARCHAR(4000),
    [subject.CodeableConcept.extension] NVARCHAR(MAX),
    [subject.CodeableConcept.coding] VARCHAR(MAX),
    [subject.CodeableConcept.text] NVARCHAR(4000),
    [subject.Reference.id] NVARCHAR(4000),
    [subject.Reference.extension] NVARCHAR(MAX),
    [subject.Reference.reference] NVARCHAR(4000),
    [subject.Reference.type] VARCHAR(256),
    [subject.Reference.identifier.id] NVARCHAR(4000),
    [subject.Reference.identifier.extension] NVARCHAR(MAX),
    [subject.Reference.identifier.use] NVARCHAR(64),
    [subject.Reference.identifier.type] NVARCHAR(MAX),
    [subject.Reference.identifier.system] VARCHAR(256),
    [subject.Reference.identifier.value] NVARCHAR(4000),
    [subject.Reference.identifier.period] NVARCHAR(MAX),
    [subject.Reference.identifier.assigner] NVARCHAR(MAX),
    [subject.Reference.display] NVARCHAR(4000),
    [timing.Timing.id] NVARCHAR(4000),
    [timing.Timing.extension] NVARCHAR(MAX),
    [timing.Timing.modifierExtension] NVARCHAR(MAX),
    [timing.Timing.event] VARCHAR(MAX),
    [timing.Timing.repeat.id] NVARCHAR(4000),
    [timing.Timing.repeat.extension] NVARCHAR(MAX),
    [timing.Timing.repeat.modifierExtension] NVARCHAR(MAX),
    [timing.Timing.repeat.count] bigint,
    [timing.Timing.repeat.countMax] bigint,
    [timing.Timing.repeat.duration] float,
    [timing.Timing.repeat.durationMax] float,
    [timing.Timing.repeat.durationUnit] NVARCHAR(64),
    [timing.Timing.repeat.frequency] bigint,
    [timing.Timing.repeat.frequencyMax] bigint,
    [timing.Timing.repeat.period] float,
    [timing.Timing.repeat.periodMax] float,
    [timing.Timing.repeat.periodUnit] NVARCHAR(64),
    [timing.Timing.repeat.dayOfWeek] NVARCHAR(MAX),
    [timing.Timing.repeat.timeOfDay] NVARCHAR(MAX),
    [timing.Timing.repeat.when] NVARCHAR(MAX),
    [timing.Timing.repeat.offset] bigint,
    [timing.Timing.repeat.bounds.Duration] NVARCHAR(MAX),
    [timing.Timing.repeat.bounds.Range] NVARCHAR(MAX),
    [timing.Timing.repeat.bounds.Period] NVARCHAR(MAX),
    [timing.Timing.code.id] NVARCHAR(4000),
    [timing.Timing.code.extension] NVARCHAR(MAX),
    [timing.Timing.code.coding] NVARCHAR(MAX),
    [timing.Timing.code.text] NVARCHAR(4000),
    [timing.dateTime] VARCHAR(30),
    [timing.Age.id] NVARCHAR(4000),
    [timing.Age.extension] NVARCHAR(MAX),
    [timing.Age.value] float,
    [timing.Age.comparator] NVARCHAR(64),
    [timing.Age.unit] NVARCHAR(4000),
    [timing.Age.system] VARCHAR(256),
    [timing.Age.code] NVARCHAR(4000),
    [timing.Period.id] NVARCHAR(4000),
    [timing.Period.extension] NVARCHAR(MAX),
    [timing.Period.start] VARCHAR(30),
    [timing.Period.end] VARCHAR(30),
    [timing.Range.id] NVARCHAR(4000),
    [timing.Range.extension] NVARCHAR(MAX),
    [timing.Range.low.id] NVARCHAR(4000),
    [timing.Range.low.extension] NVARCHAR(MAX),
    [timing.Range.low.value] float,
    [timing.Range.low.comparator] NVARCHAR(64),
    [timing.Range.low.unit] NVARCHAR(4000),
    [timing.Range.low.system] VARCHAR(256),
    [timing.Range.low.code] NVARCHAR(4000),
    [timing.Range.high.id] NVARCHAR(4000),
    [timing.Range.high.extension] NVARCHAR(MAX),
    [timing.Range.high.value] float,
    [timing.Range.high.comparator] NVARCHAR(64),
    [timing.Range.high.unit] NVARCHAR(4000),
    [timing.Range.high.system] VARCHAR(256),
    [timing.Range.high.code] NVARCHAR(4000),
    [timing.Duration.id] NVARCHAR(4000),
    [timing.Duration.extension] NVARCHAR(MAX),
    [timing.Duration.value] float,
    [timing.Duration.comparator] NVARCHAR(64),
    [timing.Duration.unit] NVARCHAR(4000),
    [timing.Duration.system] VARCHAR(256),
    [timing.Duration.code] NVARCHAR(4000),
    [product.Reference.id] NVARCHAR(4000),
    [product.Reference.extension] NVARCHAR(MAX),
    [product.Reference.reference] NVARCHAR(4000),
    [product.Reference.type] VARCHAR(256),
    [product.Reference.identifier.id] NVARCHAR(4000),
    [product.Reference.identifier.extension] NVARCHAR(MAX),
    [product.Reference.identifier.use] NVARCHAR(64),
    [product.Reference.identifier.type] NVARCHAR(MAX),
    [product.Reference.identifier.system] VARCHAR(256),
    [product.Reference.identifier.value] NVARCHAR(4000),
    [product.Reference.identifier.period] NVARCHAR(MAX),
    [product.Reference.identifier.assigner] NVARCHAR(MAX),
    [product.Reference.display] NVARCHAR(4000),
    [product.CodeableConcept.id] NVARCHAR(4000),
    [product.CodeableConcept.extension] NVARCHAR(MAX),
    [product.CodeableConcept.coding] VARCHAR(MAX),
    [product.CodeableConcept.text] NVARCHAR(4000),
) WITH (
    LOCATION='/ActivityDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ActivityDefinitionIdentifier AS
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
        BULK 'ActivityDefinition/**',
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

CREATE VIEW fhir.ActivityDefinitionContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'ActivityDefinition/**',
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

CREATE VIEW fhir.ActivityDefinitionUseContext AS
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
        BULK 'ActivityDefinition/**',
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

CREATE VIEW fhir.ActivityDefinitionJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'ActivityDefinition/**',
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

CREATE VIEW fhir.ActivityDefinitionTopic AS
SELECT
    [id],
    [topic.JSON],
    [topic.id],
    [topic.extension],
    [topic.coding],
    [topic.text]
FROM openrowset (
        BULK 'ActivityDefinition/**',
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

CREATE VIEW fhir.ActivityDefinitionAuthor AS
SELECT
    [id],
    [author.JSON],
    [author.id],
    [author.extension],
    [author.name],
    [author.telecom]
FROM openrowset (
        BULK 'ActivityDefinition/**',
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

CREATE VIEW fhir.ActivityDefinitionEditor AS
SELECT
    [id],
    [editor.JSON],
    [editor.id],
    [editor.extension],
    [editor.name],
    [editor.telecom]
FROM openrowset (
        BULK 'ActivityDefinition/**',
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

CREATE VIEW fhir.ActivityDefinitionReviewer AS
SELECT
    [id],
    [reviewer.JSON],
    [reviewer.id],
    [reviewer.extension],
    [reviewer.name],
    [reviewer.telecom]
FROM openrowset (
        BULK 'ActivityDefinition/**',
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

CREATE VIEW fhir.ActivityDefinitionEndorser AS
SELECT
    [id],
    [endorser.JSON],
    [endorser.id],
    [endorser.extension],
    [endorser.name],
    [endorser.telecom]
FROM openrowset (
        BULK 'ActivityDefinition/**',
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

CREATE VIEW fhir.ActivityDefinitionRelatedArtifact AS
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
        BULK 'ActivityDefinition/**',
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

CREATE VIEW fhir.ActivityDefinitionLibrary AS
SELECT
    [id],
    [library.JSON],
    [library]
FROM openrowset (
        BULK 'ActivityDefinition/**',
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

CREATE VIEW fhir.ActivityDefinitionParticipant AS
SELECT
    [id],
    [participant.JSON],
    [participant.id],
    [participant.extension],
    [participant.modifierExtension],
    [participant.type],
    [participant.role.id],
    [participant.role.extension],
    [participant.role.coding],
    [participant.role.text]
FROM openrowset (
        BULK 'ActivityDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [participant.JSON]  VARCHAR(MAX) '$.participant'
    ) AS rowset
    CROSS APPLY openjson (rowset.[participant.JSON]) with (
        [participant.id]               NVARCHAR(4000)      '$.id',
        [participant.extension]        NVARCHAR(MAX)       '$.extension',
        [participant.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [participant.type]             NVARCHAR(4000)      '$.type',
        [participant.role.id]          NVARCHAR(4000)      '$.role.id',
        [participant.role.extension]   NVARCHAR(MAX)       '$.role.extension',
        [participant.role.coding]      NVARCHAR(MAX)       '$.role.coding',
        [participant.role.text]        NVARCHAR(4000)      '$.role.text'
    ) j

GO

CREATE VIEW fhir.ActivityDefinitionDosage AS
SELECT
    [id],
    [dosage.JSON],
    [dosage.id],
    [dosage.extension],
    [dosage.modifierExtension],
    [dosage.sequence],
    [dosage.text],
    [dosage.additionalInstruction],
    [dosage.patientInstruction],
    [dosage.timing.id],
    [dosage.timing.extension],
    [dosage.timing.modifierExtension],
    [dosage.timing.event],
    [dosage.timing.repeat],
    [dosage.timing.code],
    [dosage.site.id],
    [dosage.site.extension],
    [dosage.site.coding],
    [dosage.site.text],
    [dosage.route.id],
    [dosage.route.extension],
    [dosage.route.coding],
    [dosage.route.text],
    [dosage.method.id],
    [dosage.method.extension],
    [dosage.method.coding],
    [dosage.method.text],
    [dosage.doseAndRate],
    [dosage.maxDosePerPeriod.id],
    [dosage.maxDosePerPeriod.extension],
    [dosage.maxDosePerPeriod.numerator],
    [dosage.maxDosePerPeriod.denominator],
    [dosage.maxDosePerAdministration.id],
    [dosage.maxDosePerAdministration.extension],
    [dosage.maxDosePerAdministration.value],
    [dosage.maxDosePerAdministration.comparator],
    [dosage.maxDosePerAdministration.unit],
    [dosage.maxDosePerAdministration.system],
    [dosage.maxDosePerAdministration.code],
    [dosage.maxDosePerLifetime.id],
    [dosage.maxDosePerLifetime.extension],
    [dosage.maxDosePerLifetime.value],
    [dosage.maxDosePerLifetime.comparator],
    [dosage.maxDosePerLifetime.unit],
    [dosage.maxDosePerLifetime.system],
    [dosage.maxDosePerLifetime.code],
    [dosage.asNeeded.boolean],
    [dosage.asNeeded.CodeableConcept.id],
    [dosage.asNeeded.CodeableConcept.extension],
    [dosage.asNeeded.CodeableConcept.coding],
    [dosage.asNeeded.CodeableConcept.text]
FROM openrowset (
        BULK 'ActivityDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [dosage.JSON]  VARCHAR(MAX) '$.dosage'
    ) AS rowset
    CROSS APPLY openjson (rowset.[dosage.JSON]) with (
        [dosage.id]                    NVARCHAR(4000)      '$.id',
        [dosage.extension]             NVARCHAR(MAX)       '$.extension',
        [dosage.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [dosage.sequence]              bigint              '$.sequence',
        [dosage.text]                  NVARCHAR(4000)      '$.text',
        [dosage.additionalInstruction] NVARCHAR(MAX)       '$.additionalInstruction' AS JSON,
        [dosage.patientInstruction]    NVARCHAR(4000)      '$.patientInstruction',
        [dosage.timing.id]             NVARCHAR(4000)      '$.timing.id',
        [dosage.timing.extension]      NVARCHAR(MAX)       '$.timing.extension',
        [dosage.timing.modifierExtension] NVARCHAR(MAX)       '$.timing.modifierExtension',
        [dosage.timing.event]          NVARCHAR(MAX)       '$.timing.event',
        [dosage.timing.repeat]         NVARCHAR(MAX)       '$.timing.repeat',
        [dosage.timing.code]           NVARCHAR(MAX)       '$.timing.code',
        [dosage.site.id]               NVARCHAR(4000)      '$.site.id',
        [dosage.site.extension]        NVARCHAR(MAX)       '$.site.extension',
        [dosage.site.coding]           NVARCHAR(MAX)       '$.site.coding',
        [dosage.site.text]             NVARCHAR(4000)      '$.site.text',
        [dosage.route.id]              NVARCHAR(4000)      '$.route.id',
        [dosage.route.extension]       NVARCHAR(MAX)       '$.route.extension',
        [dosage.route.coding]          NVARCHAR(MAX)       '$.route.coding',
        [dosage.route.text]            NVARCHAR(4000)      '$.route.text',
        [dosage.method.id]             NVARCHAR(4000)      '$.method.id',
        [dosage.method.extension]      NVARCHAR(MAX)       '$.method.extension',
        [dosage.method.coding]         NVARCHAR(MAX)       '$.method.coding',
        [dosage.method.text]           NVARCHAR(4000)      '$.method.text',
        [dosage.doseAndRate]           NVARCHAR(MAX)       '$.doseAndRate' AS JSON,
        [dosage.maxDosePerPeriod.id]   NVARCHAR(4000)      '$.maxDosePerPeriod.id',
        [dosage.maxDosePerPeriod.extension] NVARCHAR(MAX)       '$.maxDosePerPeriod.extension',
        [dosage.maxDosePerPeriod.numerator] NVARCHAR(MAX)       '$.maxDosePerPeriod.numerator',
        [dosage.maxDosePerPeriod.denominator] NVARCHAR(MAX)       '$.maxDosePerPeriod.denominator',
        [dosage.maxDosePerAdministration.id] NVARCHAR(4000)      '$.maxDosePerAdministration.id',
        [dosage.maxDosePerAdministration.extension] NVARCHAR(MAX)       '$.maxDosePerAdministration.extension',
        [dosage.maxDosePerAdministration.value] float               '$.maxDosePerAdministration.value',
        [dosage.maxDosePerAdministration.comparator] NVARCHAR(64)        '$.maxDosePerAdministration.comparator',
        [dosage.maxDosePerAdministration.unit] NVARCHAR(4000)      '$.maxDosePerAdministration.unit',
        [dosage.maxDosePerAdministration.system] VARCHAR(256)        '$.maxDosePerAdministration.system',
        [dosage.maxDosePerAdministration.code] NVARCHAR(4000)      '$.maxDosePerAdministration.code',
        [dosage.maxDosePerLifetime.id] NVARCHAR(4000)      '$.maxDosePerLifetime.id',
        [dosage.maxDosePerLifetime.extension] NVARCHAR(MAX)       '$.maxDosePerLifetime.extension',
        [dosage.maxDosePerLifetime.value] float               '$.maxDosePerLifetime.value',
        [dosage.maxDosePerLifetime.comparator] NVARCHAR(64)        '$.maxDosePerLifetime.comparator',
        [dosage.maxDosePerLifetime.unit] NVARCHAR(4000)      '$.maxDosePerLifetime.unit',
        [dosage.maxDosePerLifetime.system] VARCHAR(256)        '$.maxDosePerLifetime.system',
        [dosage.maxDosePerLifetime.code] NVARCHAR(4000)      '$.maxDosePerLifetime.code',
        [dosage.asNeeded.boolean]      bit                 '$.asNeeded.boolean',
        [dosage.asNeeded.CodeableConcept.id] NVARCHAR(4000)      '$.asNeeded.CodeableConcept.id',
        [dosage.asNeeded.CodeableConcept.extension] NVARCHAR(MAX)       '$.asNeeded.CodeableConcept.extension',
        [dosage.asNeeded.CodeableConcept.coding] NVARCHAR(MAX)       '$.asNeeded.CodeableConcept.coding',
        [dosage.asNeeded.CodeableConcept.text] NVARCHAR(4000)      '$.asNeeded.CodeableConcept.text'
    ) j

GO

CREATE VIEW fhir.ActivityDefinitionBodySite AS
SELECT
    [id],
    [bodySite.JSON],
    [bodySite.id],
    [bodySite.extension],
    [bodySite.coding],
    [bodySite.text]
FROM openrowset (
        BULK 'ActivityDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [bodySite.JSON]  VARCHAR(MAX) '$.bodySite'
    ) AS rowset
    CROSS APPLY openjson (rowset.[bodySite.JSON]) with (
        [bodySite.id]                  NVARCHAR(4000)      '$.id',
        [bodySite.extension]           NVARCHAR(MAX)       '$.extension',
        [bodySite.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [bodySite.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ActivityDefinitionSpecimenRequirement AS
SELECT
    [id],
    [specimenRequirement.JSON],
    [specimenRequirement.id],
    [specimenRequirement.extension],
    [specimenRequirement.reference],
    [specimenRequirement.type],
    [specimenRequirement.identifier.id],
    [specimenRequirement.identifier.extension],
    [specimenRequirement.identifier.use],
    [specimenRequirement.identifier.type],
    [specimenRequirement.identifier.system],
    [specimenRequirement.identifier.value],
    [specimenRequirement.identifier.period],
    [specimenRequirement.identifier.assigner],
    [specimenRequirement.display]
FROM openrowset (
        BULK 'ActivityDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specimenRequirement.JSON]  VARCHAR(MAX) '$.specimenRequirement'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specimenRequirement.JSON]) with (
        [specimenRequirement.id]       NVARCHAR(4000)      '$.id',
        [specimenRequirement.extension] NVARCHAR(MAX)       '$.extension',
        [specimenRequirement.reference] NVARCHAR(4000)      '$.reference',
        [specimenRequirement.type]     VARCHAR(256)        '$.type',
        [specimenRequirement.identifier.id] NVARCHAR(4000)      '$.identifier.id',
        [specimenRequirement.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [specimenRequirement.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [specimenRequirement.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [specimenRequirement.identifier.system] VARCHAR(256)        '$.identifier.system',
        [specimenRequirement.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [specimenRequirement.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [specimenRequirement.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [specimenRequirement.display]  NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ActivityDefinitionObservationRequirement AS
SELECT
    [id],
    [observationRequirement.JSON],
    [observationRequirement.id],
    [observationRequirement.extension],
    [observationRequirement.reference],
    [observationRequirement.type],
    [observationRequirement.identifier.id],
    [observationRequirement.identifier.extension],
    [observationRequirement.identifier.use],
    [observationRequirement.identifier.type],
    [observationRequirement.identifier.system],
    [observationRequirement.identifier.value],
    [observationRequirement.identifier.period],
    [observationRequirement.identifier.assigner],
    [observationRequirement.display]
FROM openrowset (
        BULK 'ActivityDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [observationRequirement.JSON]  VARCHAR(MAX) '$.observationRequirement'
    ) AS rowset
    CROSS APPLY openjson (rowset.[observationRequirement.JSON]) with (
        [observationRequirement.id]    NVARCHAR(4000)      '$.id',
        [observationRequirement.extension] NVARCHAR(MAX)       '$.extension',
        [observationRequirement.reference] NVARCHAR(4000)      '$.reference',
        [observationRequirement.type]  VARCHAR(256)        '$.type',
        [observationRequirement.identifier.id] NVARCHAR(4000)      '$.identifier.id',
        [observationRequirement.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [observationRequirement.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [observationRequirement.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [observationRequirement.identifier.system] VARCHAR(256)        '$.identifier.system',
        [observationRequirement.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [observationRequirement.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [observationRequirement.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [observationRequirement.display] NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ActivityDefinitionObservationResultRequirement AS
SELECT
    [id],
    [observationResultRequirement.JSON],
    [observationResultRequirement.id],
    [observationResultRequirement.extension],
    [observationResultRequirement.reference],
    [observationResultRequirement.type],
    [observationResultRequirement.identifier.id],
    [observationResultRequirement.identifier.extension],
    [observationResultRequirement.identifier.use],
    [observationResultRequirement.identifier.type],
    [observationResultRequirement.identifier.system],
    [observationResultRequirement.identifier.value],
    [observationResultRequirement.identifier.period],
    [observationResultRequirement.identifier.assigner],
    [observationResultRequirement.display]
FROM openrowset (
        BULK 'ActivityDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [observationResultRequirement.JSON]  VARCHAR(MAX) '$.observationResultRequirement'
    ) AS rowset
    CROSS APPLY openjson (rowset.[observationResultRequirement.JSON]) with (
        [observationResultRequirement.id] NVARCHAR(4000)      '$.id',
        [observationResultRequirement.extension] NVARCHAR(MAX)       '$.extension',
        [observationResultRequirement.reference] NVARCHAR(4000)      '$.reference',
        [observationResultRequirement.type] VARCHAR(256)        '$.type',
        [observationResultRequirement.identifier.id] NVARCHAR(4000)      '$.identifier.id',
        [observationResultRequirement.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [observationResultRequirement.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [observationResultRequirement.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [observationResultRequirement.identifier.system] VARCHAR(256)        '$.identifier.system',
        [observationResultRequirement.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [observationResultRequirement.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [observationResultRequirement.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [observationResultRequirement.display] NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ActivityDefinitionDynamicValue AS
SELECT
    [id],
    [dynamicValue.JSON],
    [dynamicValue.id],
    [dynamicValue.extension],
    [dynamicValue.modifierExtension],
    [dynamicValue.path],
    [dynamicValue.expression.id],
    [dynamicValue.expression.extension],
    [dynamicValue.expression.description],
    [dynamicValue.expression.name],
    [dynamicValue.expression.language],
    [dynamicValue.expression.expression],
    [dynamicValue.expression.reference]
FROM openrowset (
        BULK 'ActivityDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [dynamicValue.JSON]  VARCHAR(MAX) '$.dynamicValue'
    ) AS rowset
    CROSS APPLY openjson (rowset.[dynamicValue.JSON]) with (
        [dynamicValue.id]              NVARCHAR(4000)      '$.id',
        [dynamicValue.extension]       NVARCHAR(MAX)       '$.extension',
        [dynamicValue.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [dynamicValue.path]            NVARCHAR(4000)      '$.path',
        [dynamicValue.expression.id]   NVARCHAR(4000)      '$.expression.id',
        [dynamicValue.expression.extension] NVARCHAR(MAX)       '$.expression.extension',
        [dynamicValue.expression.description] NVARCHAR(4000)      '$.expression.description',
        [dynamicValue.expression.name] VARCHAR(64)         '$.expression.name',
        [dynamicValue.expression.language] NVARCHAR(64)        '$.expression.language',
        [dynamicValue.expression.expression] NVARCHAR(4000)      '$.expression.expression',
        [dynamicValue.expression.reference] VARCHAR(256)        '$.expression.reference'
    ) j
