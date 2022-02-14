CREATE EXTERNAL TABLE [fhir].[ResearchStudy] (
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
    [title] NVARCHAR(4000),
    [protocol] VARCHAR(MAX),
    [partOf] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [primaryPurposeType.id] NVARCHAR(100),
    [primaryPurposeType.extension] NVARCHAR(MAX),
    [primaryPurposeType.coding] VARCHAR(MAX),
    [primaryPurposeType.text] NVARCHAR(4000),
    [phase.id] NVARCHAR(100),
    [phase.extension] NVARCHAR(MAX),
    [phase.coding] VARCHAR(MAX),
    [phase.text] NVARCHAR(4000),
    [category] VARCHAR(MAX),
    [focus] VARCHAR(MAX),
    [condition] VARCHAR(MAX),
    [contact] VARCHAR(MAX),
    [relatedArtifact] VARCHAR(MAX),
    [keyword] VARCHAR(MAX),
    [location] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [enrollment] VARCHAR(MAX),
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [sponsor.id] NVARCHAR(100),
    [sponsor.extension] NVARCHAR(MAX),
    [sponsor.reference] NVARCHAR(4000),
    [sponsor.type] VARCHAR(256),
    [sponsor.identifier.id] NVARCHAR(100),
    [sponsor.identifier.extension] NVARCHAR(MAX),
    [sponsor.identifier.use] NVARCHAR(64),
    [sponsor.identifier.type] NVARCHAR(MAX),
    [sponsor.identifier.system] VARCHAR(256),
    [sponsor.identifier.value] NVARCHAR(4000),
    [sponsor.identifier.period] NVARCHAR(MAX),
    [sponsor.identifier.assigner] NVARCHAR(MAX),
    [sponsor.display] NVARCHAR(4000),
    [principalInvestigator.id] NVARCHAR(100),
    [principalInvestigator.extension] NVARCHAR(MAX),
    [principalInvestigator.reference] NVARCHAR(4000),
    [principalInvestigator.type] VARCHAR(256),
    [principalInvestigator.identifier.id] NVARCHAR(100),
    [principalInvestigator.identifier.extension] NVARCHAR(MAX),
    [principalInvestigator.identifier.use] NVARCHAR(64),
    [principalInvestigator.identifier.type] NVARCHAR(MAX),
    [principalInvestigator.identifier.system] VARCHAR(256),
    [principalInvestigator.identifier.value] NVARCHAR(4000),
    [principalInvestigator.identifier.period] NVARCHAR(MAX),
    [principalInvestigator.identifier.assigner] NVARCHAR(MAX),
    [principalInvestigator.display] NVARCHAR(4000),
    [site] VARCHAR(MAX),
    [reasonStopped.id] NVARCHAR(100),
    [reasonStopped.extension] NVARCHAR(MAX),
    [reasonStopped.coding] VARCHAR(MAX),
    [reasonStopped.text] NVARCHAR(4000),
    [note] VARCHAR(MAX),
    [arm] VARCHAR(MAX),
    [objective] VARCHAR(MAX),
) WITH (
    LOCATION='/ResearchStudy/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ResearchStudyIdentifier AS
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
        BULK 'ResearchStudy/**',
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

CREATE VIEW fhir.ResearchStudyProtocol AS
SELECT
    [id],
    [protocol.JSON],
    [protocol.id],
    [protocol.extension],
    [protocol.reference],
    [protocol.type],
    [protocol.identifier.id],
    [protocol.identifier.extension],
    [protocol.identifier.use],
    [protocol.identifier.type],
    [protocol.identifier.system],
    [protocol.identifier.value],
    [protocol.identifier.period],
    [protocol.identifier.assigner],
    [protocol.display]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [protocol.JSON]  VARCHAR(MAX) '$.protocol'
    ) AS rowset
    CROSS APPLY openjson (rowset.[protocol.JSON]) with (
        [protocol.id]                  NVARCHAR(100)       '$.id',
        [protocol.extension]           NVARCHAR(MAX)       '$.extension',
        [protocol.reference]           NVARCHAR(4000)      '$.reference',
        [protocol.type]                VARCHAR(256)        '$.type',
        [protocol.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [protocol.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [protocol.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [protocol.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [protocol.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [protocol.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [protocol.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [protocol.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [protocol.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ResearchStudyPartOf AS
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
        BULK 'ResearchStudy/**',
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

CREATE VIEW fhir.ResearchStudyCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'ResearchStudy/**',
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

CREATE VIEW fhir.ResearchStudyFocus AS
SELECT
    [id],
    [focus.JSON],
    [focus.id],
    [focus.extension],
    [focus.coding],
    [focus.text]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [focus.JSON]  VARCHAR(MAX) '$.focus'
    ) AS rowset
    CROSS APPLY openjson (rowset.[focus.JSON]) with (
        [focus.id]                     NVARCHAR(100)       '$.id',
        [focus.extension]              NVARCHAR(MAX)       '$.extension',
        [focus.coding]                 NVARCHAR(MAX)       '$.coding' AS JSON,
        [focus.text]                   NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ResearchStudyCondition AS
SELECT
    [id],
    [condition.JSON],
    [condition.id],
    [condition.extension],
    [condition.coding],
    [condition.text]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [condition.JSON]  VARCHAR(MAX) '$.condition'
    ) AS rowset
    CROSS APPLY openjson (rowset.[condition.JSON]) with (
        [condition.id]                 NVARCHAR(100)       '$.id',
        [condition.extension]          NVARCHAR(MAX)       '$.extension',
        [condition.coding]             NVARCHAR(MAX)       '$.coding' AS JSON,
        [condition.text]               NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ResearchStudyContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'ResearchStudy/**',
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

CREATE VIEW fhir.ResearchStudyRelatedArtifact AS
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
        BULK 'ResearchStudy/**',
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

CREATE VIEW fhir.ResearchStudyKeyword AS
SELECT
    [id],
    [keyword.JSON],
    [keyword.id],
    [keyword.extension],
    [keyword.coding],
    [keyword.text]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [keyword.JSON]  VARCHAR(MAX) '$.keyword'
    ) AS rowset
    CROSS APPLY openjson (rowset.[keyword.JSON]) with (
        [keyword.id]                   NVARCHAR(100)       '$.id',
        [keyword.extension]            NVARCHAR(MAX)       '$.extension',
        [keyword.coding]               NVARCHAR(MAX)       '$.coding' AS JSON,
        [keyword.text]                 NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ResearchStudyLocation AS
SELECT
    [id],
    [location.JSON],
    [location.id],
    [location.extension],
    [location.coding],
    [location.text]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [location.JSON]  VARCHAR(MAX) '$.location'
    ) AS rowset
    CROSS APPLY openjson (rowset.[location.JSON]) with (
        [location.id]                  NVARCHAR(100)       '$.id',
        [location.extension]           NVARCHAR(MAX)       '$.extension',
        [location.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [location.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ResearchStudyEnrollment AS
SELECT
    [id],
    [enrollment.JSON],
    [enrollment.id],
    [enrollment.extension],
    [enrollment.reference],
    [enrollment.type],
    [enrollment.identifier.id],
    [enrollment.identifier.extension],
    [enrollment.identifier.use],
    [enrollment.identifier.type],
    [enrollment.identifier.system],
    [enrollment.identifier.value],
    [enrollment.identifier.period],
    [enrollment.identifier.assigner],
    [enrollment.display]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [enrollment.JSON]  VARCHAR(MAX) '$.enrollment'
    ) AS rowset
    CROSS APPLY openjson (rowset.[enrollment.JSON]) with (
        [enrollment.id]                NVARCHAR(100)       '$.id',
        [enrollment.extension]         NVARCHAR(MAX)       '$.extension',
        [enrollment.reference]         NVARCHAR(4000)      '$.reference',
        [enrollment.type]              VARCHAR(256)        '$.type',
        [enrollment.identifier.id]     NVARCHAR(100)       '$.identifier.id',
        [enrollment.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [enrollment.identifier.use]    NVARCHAR(64)        '$.identifier.use',
        [enrollment.identifier.type]   NVARCHAR(MAX)       '$.identifier.type',
        [enrollment.identifier.system] VARCHAR(256)        '$.identifier.system',
        [enrollment.identifier.value]  NVARCHAR(4000)      '$.identifier.value',
        [enrollment.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [enrollment.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [enrollment.display]           NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ResearchStudySite AS
SELECT
    [id],
    [site.JSON],
    [site.id],
    [site.extension],
    [site.reference],
    [site.type],
    [site.identifier.id],
    [site.identifier.extension],
    [site.identifier.use],
    [site.identifier.type],
    [site.identifier.system],
    [site.identifier.value],
    [site.identifier.period],
    [site.identifier.assigner],
    [site.display]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [site.JSON]  VARCHAR(MAX) '$.site'
    ) AS rowset
    CROSS APPLY openjson (rowset.[site.JSON]) with (
        [site.id]                      NVARCHAR(100)       '$.id',
        [site.extension]               NVARCHAR(MAX)       '$.extension',
        [site.reference]               NVARCHAR(4000)      '$.reference',
        [site.type]                    VARCHAR(256)        '$.type',
        [site.identifier.id]           NVARCHAR(100)       '$.identifier.id',
        [site.identifier.extension]    NVARCHAR(MAX)       '$.identifier.extension',
        [site.identifier.use]          NVARCHAR(64)        '$.identifier.use',
        [site.identifier.type]         NVARCHAR(MAX)       '$.identifier.type',
        [site.identifier.system]       VARCHAR(256)        '$.identifier.system',
        [site.identifier.value]        NVARCHAR(4000)      '$.identifier.value',
        [site.identifier.period]       NVARCHAR(MAX)       '$.identifier.period',
        [site.identifier.assigner]     NVARCHAR(MAX)       '$.identifier.assigner',
        [site.display]                 NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ResearchStudyNote AS
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
        BULK 'ResearchStudy/**',
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

CREATE VIEW fhir.ResearchStudyArm AS
SELECT
    [id],
    [arm.JSON],
    [arm.id],
    [arm.extension],
    [arm.modifierExtension],
    [arm.name],
    [arm.type.id],
    [arm.type.extension],
    [arm.type.coding],
    [arm.type.text],
    [arm.description]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [arm.JSON]  VARCHAR(MAX) '$.arm'
    ) AS rowset
    CROSS APPLY openjson (rowset.[arm.JSON]) with (
        [arm.id]                       NVARCHAR(100)       '$.id',
        [arm.extension]                NVARCHAR(MAX)       '$.extension',
        [arm.modifierExtension]        NVARCHAR(MAX)       '$.modifierExtension',
        [arm.name]                     NVARCHAR(500)       '$.name',
        [arm.type.id]                  NVARCHAR(100)       '$.type.id',
        [arm.type.extension]           NVARCHAR(MAX)       '$.type.extension',
        [arm.type.coding]              NVARCHAR(MAX)       '$.type.coding',
        [arm.type.text]                NVARCHAR(4000)      '$.type.text',
        [arm.description]              NVARCHAR(4000)      '$.description'
    ) j

GO

CREATE VIEW fhir.ResearchStudyObjective AS
SELECT
    [id],
    [objective.JSON],
    [objective.id],
    [objective.extension],
    [objective.modifierExtension],
    [objective.name],
    [objective.type.id],
    [objective.type.extension],
    [objective.type.coding],
    [objective.type.text]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [objective.JSON]  VARCHAR(MAX) '$.objective'
    ) AS rowset
    CROSS APPLY openjson (rowset.[objective.JSON]) with (
        [objective.id]                 NVARCHAR(100)       '$.id',
        [objective.extension]          NVARCHAR(MAX)       '$.extension',
        [objective.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [objective.name]               NVARCHAR(500)       '$.name',
        [objective.type.id]            NVARCHAR(100)       '$.type.id',
        [objective.type.extension]     NVARCHAR(MAX)       '$.type.extension',
        [objective.type.coding]        NVARCHAR(MAX)       '$.type.coding',
        [objective.type.text]          NVARCHAR(4000)      '$.type.text'
    ) j
