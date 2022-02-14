CREATE EXTERNAL TABLE [fhir].[ActivityDefinition] (
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
    [kind] NVARCHAR(100),
    [profile] VARCHAR(256),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [intent] NVARCHAR(100),
    [priority] NVARCHAR(100),
    [doNotPerform] bit,
    [location.id] NVARCHAR(100),
    [location.extension] NVARCHAR(MAX),
    [location.reference] NVARCHAR(4000),
    [location.type] VARCHAR(256),
    [location.identifier.id] NVARCHAR(100),
    [location.identifier.extension] NVARCHAR(MAX),
    [location.identifier.use] NVARCHAR(64),
    [location.identifier.type] NVARCHAR(MAX),
    [location.identifier.system] VARCHAR(256),
    [location.identifier.value] NVARCHAR(4000),
    [location.identifier.period] NVARCHAR(MAX),
    [location.identifier.assigner] NVARCHAR(MAX),
    [location.display] NVARCHAR(4000),
    [participant] VARCHAR(MAX),
    [quantity.id] NVARCHAR(100),
    [quantity.extension] NVARCHAR(MAX),
    [quantity.value] float,
    [quantity.comparator] NVARCHAR(64),
    [quantity.unit] NVARCHAR(100),
    [quantity.system] VARCHAR(256),
    [quantity.code] NVARCHAR(4000),
    [dosage] VARCHAR(MAX),
    [bodySite] VARCHAR(MAX),
    [specimenRequirement] VARCHAR(MAX),
    [observationRequirement] VARCHAR(MAX),
    [observationResultRequirement] VARCHAR(MAX),
    [transform] VARCHAR(256),
    [dynamicValue] VARCHAR(MAX),
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
    [timing.timing.id] NVARCHAR(100),
    [timing.timing.extension] NVARCHAR(MAX),
    [timing.timing.modifierExtension] NVARCHAR(MAX),
    [timing.timing.event] VARCHAR(MAX),
    [timing.timing.repeat.id] NVARCHAR(100),
    [timing.timing.repeat.extension] NVARCHAR(MAX),
    [timing.timing.repeat.modifierExtension] NVARCHAR(MAX),
    [timing.timing.repeat.count] bigint,
    [timing.timing.repeat.countMax] bigint,
    [timing.timing.repeat.duration] float,
    [timing.timing.repeat.durationMax] float,
    [timing.timing.repeat.durationUnit] NVARCHAR(64),
    [timing.timing.repeat.frequency] bigint,
    [timing.timing.repeat.frequencyMax] bigint,
    [timing.timing.repeat.period] float,
    [timing.timing.repeat.periodMax] float,
    [timing.timing.repeat.periodUnit] NVARCHAR(64),
    [timing.timing.repeat.dayOfWeek] NVARCHAR(MAX),
    [timing.timing.repeat.timeOfDay] NVARCHAR(MAX),
    [timing.timing.repeat.when] NVARCHAR(MAX),
    [timing.timing.repeat.offset] bigint,
    [timing.timing.repeat.bounds.duration] NVARCHAR(MAX),
    [timing.timing.repeat.bounds.range] NVARCHAR(MAX),
    [timing.timing.repeat.bounds.period] NVARCHAR(MAX),
    [timing.timing.code.id] NVARCHAR(100),
    [timing.timing.code.extension] NVARCHAR(MAX),
    [timing.timing.code.coding] NVARCHAR(MAX),
    [timing.timing.code.text] NVARCHAR(4000),
    [timing.dateTime] VARCHAR(64),
    [timing.age.id] NVARCHAR(100),
    [timing.age.extension] NVARCHAR(MAX),
    [timing.age.value] float,
    [timing.age.comparator] NVARCHAR(64),
    [timing.age.unit] NVARCHAR(100),
    [timing.age.system] VARCHAR(256),
    [timing.age.code] NVARCHAR(4000),
    [timing.period.id] NVARCHAR(100),
    [timing.period.extension] NVARCHAR(MAX),
    [timing.period.start] VARCHAR(64),
    [timing.period.end] VARCHAR(64),
    [timing.range.id] NVARCHAR(100),
    [timing.range.extension] NVARCHAR(MAX),
    [timing.range.low.id] NVARCHAR(100),
    [timing.range.low.extension] NVARCHAR(MAX),
    [timing.range.low.value] float,
    [timing.range.low.comparator] NVARCHAR(64),
    [timing.range.low.unit] NVARCHAR(100),
    [timing.range.low.system] VARCHAR(256),
    [timing.range.low.code] NVARCHAR(4000),
    [timing.range.high.id] NVARCHAR(100),
    [timing.range.high.extension] NVARCHAR(MAX),
    [timing.range.high.value] float,
    [timing.range.high.comparator] NVARCHAR(64),
    [timing.range.high.unit] NVARCHAR(100),
    [timing.range.high.system] VARCHAR(256),
    [timing.range.high.code] NVARCHAR(4000),
    [timing.duration.id] NVARCHAR(100),
    [timing.duration.extension] NVARCHAR(MAX),
    [timing.duration.value] float,
    [timing.duration.comparator] NVARCHAR(64),
    [timing.duration.unit] NVARCHAR(100),
    [timing.duration.system] VARCHAR(256),
    [timing.duration.code] NVARCHAR(4000),
    [product.reference.id] NVARCHAR(100),
    [product.reference.extension] NVARCHAR(MAX),
    [product.reference.reference] NVARCHAR(4000),
    [product.reference.type] VARCHAR(256),
    [product.reference.identifier.id] NVARCHAR(100),
    [product.reference.identifier.extension] NVARCHAR(MAX),
    [product.reference.identifier.use] NVARCHAR(64),
    [product.reference.identifier.type] NVARCHAR(MAX),
    [product.reference.identifier.system] VARCHAR(256),
    [product.reference.identifier.value] NVARCHAR(4000),
    [product.reference.identifier.period] NVARCHAR(MAX),
    [product.reference.identifier.assigner] NVARCHAR(MAX),
    [product.reference.display] NVARCHAR(4000),
    [product.codeableConcept.id] NVARCHAR(100),
    [product.codeableConcept.extension] NVARCHAR(MAX),
    [product.codeableConcept.coding] VARCHAR(MAX),
    [product.codeableConcept.text] NVARCHAR(4000),
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
        [contact.id]                   NVARCHAR(100)       '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.name]                 NVARCHAR(500)       '$.name',
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
        BULK 'ActivityDefinition/**',
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
        [jurisdiction.id]              NVARCHAR(100)       '$.id',
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
        [topic.id]                     NVARCHAR(100)       '$.id',
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
        [author.id]                    NVARCHAR(100)       '$.id',
        [author.extension]             NVARCHAR(MAX)       '$.extension',
        [author.name]                  NVARCHAR(500)       '$.name',
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
        [editor.id]                    NVARCHAR(100)       '$.id',
        [editor.extension]             NVARCHAR(MAX)       '$.extension',
        [editor.name]                  NVARCHAR(500)       '$.name',
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
        [reviewer.id]                  NVARCHAR(100)       '$.id',
        [reviewer.extension]           NVARCHAR(MAX)       '$.extension',
        [reviewer.name]                NVARCHAR(500)       '$.name',
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
        [endorser.id]                  NVARCHAR(100)       '$.id',
        [endorser.extension]           NVARCHAR(MAX)       '$.extension',
        [endorser.name]                NVARCHAR(500)       '$.name',
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
        [participant.id]               NVARCHAR(100)       '$.id',
        [participant.extension]        NVARCHAR(MAX)       '$.extension',
        [participant.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [participant.type]             NVARCHAR(100)       '$.type',
        [participant.role.id]          NVARCHAR(100)       '$.role.id',
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
    [dosage.asNeeded.codeableConcept.id],
    [dosage.asNeeded.codeableConcept.extension],
    [dosage.asNeeded.codeableConcept.coding],
    [dosage.asNeeded.codeableConcept.text]
FROM openrowset (
        BULK 'ActivityDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [dosage.JSON]  VARCHAR(MAX) '$.dosage'
    ) AS rowset
    CROSS APPLY openjson (rowset.[dosage.JSON]) with (
        [dosage.id]                    NVARCHAR(100)       '$.id',
        [dosage.extension]             NVARCHAR(MAX)       '$.extension',
        [dosage.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [dosage.sequence]              bigint              '$.sequence',
        [dosage.text]                  NVARCHAR(4000)      '$.text',
        [dosage.additionalInstruction] NVARCHAR(MAX)       '$.additionalInstruction' AS JSON,
        [dosage.patientInstruction]    NVARCHAR(4000)      '$.patientInstruction',
        [dosage.timing.id]             NVARCHAR(100)       '$.timing.id',
        [dosage.timing.extension]      NVARCHAR(MAX)       '$.timing.extension',
        [dosage.timing.modifierExtension] NVARCHAR(MAX)       '$.timing.modifierExtension',
        [dosage.timing.event]          NVARCHAR(MAX)       '$.timing.event',
        [dosage.timing.repeat]         NVARCHAR(MAX)       '$.timing.repeat',
        [dosage.timing.code]           NVARCHAR(MAX)       '$.timing.code',
        [dosage.site.id]               NVARCHAR(100)       '$.site.id',
        [dosage.site.extension]        NVARCHAR(MAX)       '$.site.extension',
        [dosage.site.coding]           NVARCHAR(MAX)       '$.site.coding',
        [dosage.site.text]             NVARCHAR(4000)      '$.site.text',
        [dosage.route.id]              NVARCHAR(100)       '$.route.id',
        [dosage.route.extension]       NVARCHAR(MAX)       '$.route.extension',
        [dosage.route.coding]          NVARCHAR(MAX)       '$.route.coding',
        [dosage.route.text]            NVARCHAR(4000)      '$.route.text',
        [dosage.method.id]             NVARCHAR(100)       '$.method.id',
        [dosage.method.extension]      NVARCHAR(MAX)       '$.method.extension',
        [dosage.method.coding]         NVARCHAR(MAX)       '$.method.coding',
        [dosage.method.text]           NVARCHAR(4000)      '$.method.text',
        [dosage.doseAndRate]           NVARCHAR(MAX)       '$.doseAndRate' AS JSON,
        [dosage.maxDosePerPeriod.id]   NVARCHAR(100)       '$.maxDosePerPeriod.id',
        [dosage.maxDosePerPeriod.extension] NVARCHAR(MAX)       '$.maxDosePerPeriod.extension',
        [dosage.maxDosePerPeriod.numerator] NVARCHAR(MAX)       '$.maxDosePerPeriod.numerator',
        [dosage.maxDosePerPeriod.denominator] NVARCHAR(MAX)       '$.maxDosePerPeriod.denominator',
        [dosage.maxDosePerAdministration.id] NVARCHAR(100)       '$.maxDosePerAdministration.id',
        [dosage.maxDosePerAdministration.extension] NVARCHAR(MAX)       '$.maxDosePerAdministration.extension',
        [dosage.maxDosePerAdministration.value] float               '$.maxDosePerAdministration.value',
        [dosage.maxDosePerAdministration.comparator] NVARCHAR(64)        '$.maxDosePerAdministration.comparator',
        [dosage.maxDosePerAdministration.unit] NVARCHAR(100)       '$.maxDosePerAdministration.unit',
        [dosage.maxDosePerAdministration.system] VARCHAR(256)        '$.maxDosePerAdministration.system',
        [dosage.maxDosePerAdministration.code] NVARCHAR(4000)      '$.maxDosePerAdministration.code',
        [dosage.maxDosePerLifetime.id] NVARCHAR(100)       '$.maxDosePerLifetime.id',
        [dosage.maxDosePerLifetime.extension] NVARCHAR(MAX)       '$.maxDosePerLifetime.extension',
        [dosage.maxDosePerLifetime.value] float               '$.maxDosePerLifetime.value',
        [dosage.maxDosePerLifetime.comparator] NVARCHAR(64)        '$.maxDosePerLifetime.comparator',
        [dosage.maxDosePerLifetime.unit] NVARCHAR(100)       '$.maxDosePerLifetime.unit',
        [dosage.maxDosePerLifetime.system] VARCHAR(256)        '$.maxDosePerLifetime.system',
        [dosage.maxDosePerLifetime.code] NVARCHAR(4000)      '$.maxDosePerLifetime.code',
        [dosage.asNeeded.boolean]      bit                 '$.asNeeded.boolean',
        [dosage.asNeeded.codeableConcept.id] NVARCHAR(100)       '$.asNeeded.codeableConcept.id',
        [dosage.asNeeded.codeableConcept.extension] NVARCHAR(MAX)       '$.asNeeded.codeableConcept.extension',
        [dosage.asNeeded.codeableConcept.coding] NVARCHAR(MAX)       '$.asNeeded.codeableConcept.coding',
        [dosage.asNeeded.codeableConcept.text] NVARCHAR(4000)      '$.asNeeded.codeableConcept.text'
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
        [bodySite.id]                  NVARCHAR(100)       '$.id',
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
        [specimenRequirement.id]       NVARCHAR(100)       '$.id',
        [specimenRequirement.extension] NVARCHAR(MAX)       '$.extension',
        [specimenRequirement.reference] NVARCHAR(4000)      '$.reference',
        [specimenRequirement.type]     VARCHAR(256)        '$.type',
        [specimenRequirement.identifier.id] NVARCHAR(100)       '$.identifier.id',
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
        [observationRequirement.id]    NVARCHAR(100)       '$.id',
        [observationRequirement.extension] NVARCHAR(MAX)       '$.extension',
        [observationRequirement.reference] NVARCHAR(4000)      '$.reference',
        [observationRequirement.type]  VARCHAR(256)        '$.type',
        [observationRequirement.identifier.id] NVARCHAR(100)       '$.identifier.id',
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
        [observationResultRequirement.id] NVARCHAR(100)       '$.id',
        [observationResultRequirement.extension] NVARCHAR(MAX)       '$.extension',
        [observationResultRequirement.reference] NVARCHAR(4000)      '$.reference',
        [observationResultRequirement.type] VARCHAR(256)        '$.type',
        [observationResultRequirement.identifier.id] NVARCHAR(100)       '$.identifier.id',
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
        [dynamicValue.id]              NVARCHAR(100)       '$.id',
        [dynamicValue.extension]       NVARCHAR(MAX)       '$.extension',
        [dynamicValue.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [dynamicValue.path]            NVARCHAR(4000)      '$.path',
        [dynamicValue.expression.id]   NVARCHAR(100)       '$.expression.id',
        [dynamicValue.expression.extension] NVARCHAR(MAX)       '$.expression.extension',
        [dynamicValue.expression.description] NVARCHAR(4000)      '$.expression.description',
        [dynamicValue.expression.name] VARCHAR(64)         '$.expression.name',
        [dynamicValue.expression.language] NVARCHAR(64)        '$.expression.language',
        [dynamicValue.expression.expression] NVARCHAR(4000)      '$.expression.expression',
        [dynamicValue.expression.reference] VARCHAR(256)        '$.expression.reference'
    ) j
