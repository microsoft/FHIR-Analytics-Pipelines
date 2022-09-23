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
    [url] VARCHAR(256),
    [identifier] VARCHAR(MAX),
    [version] NVARCHAR(100),
    [name] NVARCHAR(500),
    [title] NVARCHAR(4000),
    [label] VARCHAR(MAX),
    [protocol] VARCHAR(MAX),
    [partOf] VARCHAR(MAX),
    [relatedArtifact] VARCHAR(MAX),
    [date] VARCHAR(64),
    [status] NVARCHAR(100),
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
    [keyword] VARCHAR(MAX),
    [location] VARCHAR(MAX),
    [descriptionSummary] NVARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [contact] VARCHAR(MAX),
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
    [note] VARCHAR(MAX),
    [classification] VARCHAR(MAX),
    [associatedParty] VARCHAR(MAX),
    [currentState] VARCHAR(MAX),
    [statusDate] VARCHAR(MAX),
    [whyStopped.id] NVARCHAR(100),
    [whyStopped.extension] NVARCHAR(MAX),
    [whyStopped.coding] VARCHAR(MAX),
    [whyStopped.text] NVARCHAR(4000),
    [recruitment.id] NVARCHAR(100),
    [recruitment.extension] NVARCHAR(MAX),
    [recruitment.modifierExtension] NVARCHAR(MAX),
    [recruitment.targetNumber] bigint,
    [recruitment.actualNumber] bigint,
    [recruitment.eligibility.id] NVARCHAR(100),
    [recruitment.eligibility.extension] NVARCHAR(MAX),
    [recruitment.eligibility.reference] NVARCHAR(4000),
    [recruitment.eligibility.type] VARCHAR(256),
    [recruitment.eligibility.identifier] NVARCHAR(MAX),
    [recruitment.eligibility.display] NVARCHAR(4000),
    [recruitment.actualGroup.id] NVARCHAR(100),
    [recruitment.actualGroup.extension] NVARCHAR(MAX),
    [recruitment.actualGroup.reference] NVARCHAR(4000),
    [recruitment.actualGroup.type] VARCHAR(256),
    [recruitment.actualGroup.identifier] NVARCHAR(MAX),
    [recruitment.actualGroup.display] NVARCHAR(4000),
    [comparisonGroup] VARCHAR(MAX),
    [objective] VARCHAR(MAX),
    [outcomeMeasure] VARCHAR(MAX),
    [result] VARCHAR(MAX),
    [webLocation] VARCHAR(MAX),
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

CREATE VIEW fhir.ResearchStudyLabel AS
SELECT
    [id],
    [label.JSON],
    [label.id],
    [label.extension],
    [label.modifierExtension],
    [label.type.id],
    [label.type.extension],
    [label.type.coding],
    [label.type.text],
    [label.value]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [label.JSON]  VARCHAR(MAX) '$.label'
    ) AS rowset
    CROSS APPLY openjson (rowset.[label.JSON]) with (
        [label.id]                     NVARCHAR(100)       '$.id',
        [label.extension]              NVARCHAR(MAX)       '$.extension',
        [label.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [label.type.id]                NVARCHAR(100)       '$.type.id',
        [label.type.extension]         NVARCHAR(MAX)       '$.type.extension',
        [label.type.coding]            NVARCHAR(MAX)       '$.type.coding',
        [label.type.text]              NVARCHAR(4000)      '$.type.text',
        [label.value]                  NVARCHAR(4000)      '$.value'
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

CREATE VIEW fhir.ResearchStudyRelatedArtifact AS
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
    [focus.modifierExtension],
    [focus.productCode.id],
    [focus.productCode.extension],
    [focus.productCode.coding],
    [focus.productCode.text],
    [focus.focusType],
    [focus.factor]
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
        [focus.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [focus.productCode.id]         NVARCHAR(100)       '$.productCode.id',
        [focus.productCode.extension]  NVARCHAR(MAX)       '$.productCode.extension',
        [focus.productCode.coding]     NVARCHAR(MAX)       '$.productCode.coding',
        [focus.productCode.text]       NVARCHAR(4000)      '$.productCode.text',
        [focus.focusType]              NVARCHAR(MAX)       '$.focusType' AS JSON,
        [focus.factor]                 NVARCHAR(MAX)       '$.factor'
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

CREATE VIEW fhir.ResearchStudyClassification AS
SELECT
    [id],
    [classification.JSON],
    [classification.id],
    [classification.extension],
    [classification.modifierExtension],
    [classification.type.id],
    [classification.type.extension],
    [classification.type.coding],
    [classification.type.text],
    [classification.classifier]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [classification.JSON]  VARCHAR(MAX) '$.classification'
    ) AS rowset
    CROSS APPLY openjson (rowset.[classification.JSON]) with (
        [classification.id]            NVARCHAR(100)       '$.id',
        [classification.extension]     NVARCHAR(MAX)       '$.extension',
        [classification.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [classification.type.id]       NVARCHAR(100)       '$.type.id',
        [classification.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [classification.type.coding]   NVARCHAR(MAX)       '$.type.coding',
        [classification.type.text]     NVARCHAR(4000)      '$.type.text',
        [classification.classifier]    NVARCHAR(MAX)       '$.classifier' AS JSON
    ) j

GO

CREATE VIEW fhir.ResearchStudyAssociatedParty AS
SELECT
    [id],
    [associatedParty.JSON],
    [associatedParty.id],
    [associatedParty.extension],
    [associatedParty.modifierExtension],
    [associatedParty.name],
    [associatedParty.role.id],
    [associatedParty.role.extension],
    [associatedParty.role.coding],
    [associatedParty.role.text],
    [associatedParty.classifier],
    [associatedParty.party.id],
    [associatedParty.party.extension],
    [associatedParty.party.reference],
    [associatedParty.party.type],
    [associatedParty.party.identifier],
    [associatedParty.party.display]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [associatedParty.JSON]  VARCHAR(MAX) '$.associatedParty'
    ) AS rowset
    CROSS APPLY openjson (rowset.[associatedParty.JSON]) with (
        [associatedParty.id]           NVARCHAR(100)       '$.id',
        [associatedParty.extension]    NVARCHAR(MAX)       '$.extension',
        [associatedParty.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [associatedParty.name]         NVARCHAR(500)       '$.name',
        [associatedParty.role.id]      NVARCHAR(100)       '$.role.id',
        [associatedParty.role.extension] NVARCHAR(MAX)       '$.role.extension',
        [associatedParty.role.coding]  NVARCHAR(MAX)       '$.role.coding',
        [associatedParty.role.text]    NVARCHAR(4000)      '$.role.text',
        [associatedParty.classifier]   NVARCHAR(MAX)       '$.classifier' AS JSON,
        [associatedParty.party.id]     NVARCHAR(100)       '$.party.id',
        [associatedParty.party.extension] NVARCHAR(MAX)       '$.party.extension',
        [associatedParty.party.reference] NVARCHAR(4000)      '$.party.reference',
        [associatedParty.party.type]   VARCHAR(256)        '$.party.type',
        [associatedParty.party.identifier] NVARCHAR(MAX)       '$.party.identifier',
        [associatedParty.party.display] NVARCHAR(4000)      '$.party.display'
    ) j

GO

CREATE VIEW fhir.ResearchStudyCurrentState AS
SELECT
    [id],
    [currentState.JSON],
    [currentState.id],
    [currentState.extension],
    [currentState.coding],
    [currentState.text]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [currentState.JSON]  VARCHAR(MAX) '$.currentState'
    ) AS rowset
    CROSS APPLY openjson (rowset.[currentState.JSON]) with (
        [currentState.id]              NVARCHAR(100)       '$.id',
        [currentState.extension]       NVARCHAR(MAX)       '$.extension',
        [currentState.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [currentState.text]            NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ResearchStudyStatusDate AS
SELECT
    [id],
    [statusDate.JSON],
    [statusDate.id],
    [statusDate.extension],
    [statusDate.modifierExtension],
    [statusDate.activity.id],
    [statusDate.activity.extension],
    [statusDate.activity.coding],
    [statusDate.activity.text],
    [statusDate.actual],
    [statusDate.period.id],
    [statusDate.period.extension],
    [statusDate.period.start],
    [statusDate.period.end]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [statusDate.JSON]  VARCHAR(MAX) '$.statusDate'
    ) AS rowset
    CROSS APPLY openjson (rowset.[statusDate.JSON]) with (
        [statusDate.id]                NVARCHAR(100)       '$.id',
        [statusDate.extension]         NVARCHAR(MAX)       '$.extension',
        [statusDate.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [statusDate.activity.id]       NVARCHAR(100)       '$.activity.id',
        [statusDate.activity.extension] NVARCHAR(MAX)       '$.activity.extension',
        [statusDate.activity.coding]   NVARCHAR(MAX)       '$.activity.coding',
        [statusDate.activity.text]     NVARCHAR(4000)      '$.activity.text',
        [statusDate.actual]            bit                 '$.actual',
        [statusDate.period.id]         NVARCHAR(100)       '$.period.id',
        [statusDate.period.extension]  NVARCHAR(MAX)       '$.period.extension',
        [statusDate.period.start]      VARCHAR(64)         '$.period.start',
        [statusDate.period.end]        VARCHAR(64)         '$.period.end'
    ) j

GO

CREATE VIEW fhir.ResearchStudyComparisonGroup AS
SELECT
    [id],
    [comparisonGroup.JSON],
    [comparisonGroup.id],
    [comparisonGroup.extension],
    [comparisonGroup.modifierExtension],
    [comparisonGroup.name],
    [comparisonGroup.type.id],
    [comparisonGroup.type.extension],
    [comparisonGroup.type.coding],
    [comparisonGroup.type.text],
    [comparisonGroup.description],
    [comparisonGroup.intendedExposure],
    [comparisonGroup.observedGroup.id],
    [comparisonGroup.observedGroup.extension],
    [comparisonGroup.observedGroup.reference],
    [comparisonGroup.observedGroup.type],
    [comparisonGroup.observedGroup.identifier],
    [comparisonGroup.observedGroup.display],
    [comparisonGroup.identifier.uri],
    [comparisonGroup.identifier.identifier.id],
    [comparisonGroup.identifier.identifier.extension],
    [comparisonGroup.identifier.identifier.use],
    [comparisonGroup.identifier.identifier.type],
    [comparisonGroup.identifier.identifier.system],
    [comparisonGroup.identifier.identifier.value],
    [comparisonGroup.identifier.identifier.period],
    [comparisonGroup.identifier.identifier.assigner]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [comparisonGroup.JSON]  VARCHAR(MAX) '$.comparisonGroup'
    ) AS rowset
    CROSS APPLY openjson (rowset.[comparisonGroup.JSON]) with (
        [comparisonGroup.id]           NVARCHAR(100)       '$.id',
        [comparisonGroup.extension]    NVARCHAR(MAX)       '$.extension',
        [comparisonGroup.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [comparisonGroup.name]         NVARCHAR(500)       '$.name',
        [comparisonGroup.type.id]      NVARCHAR(100)       '$.type.id',
        [comparisonGroup.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [comparisonGroup.type.coding]  NVARCHAR(MAX)       '$.type.coding',
        [comparisonGroup.type.text]    NVARCHAR(4000)      '$.type.text',
        [comparisonGroup.description]  NVARCHAR(MAX)       '$.description',
        [comparisonGroup.intendedExposure] NVARCHAR(MAX)       '$.intendedExposure' AS JSON,
        [comparisonGroup.observedGroup.id] NVARCHAR(100)       '$.observedGroup.id',
        [comparisonGroup.observedGroup.extension] NVARCHAR(MAX)       '$.observedGroup.extension',
        [comparisonGroup.observedGroup.reference] NVARCHAR(4000)      '$.observedGroup.reference',
        [comparisonGroup.observedGroup.type] VARCHAR(256)        '$.observedGroup.type',
        [comparisonGroup.observedGroup.identifier] NVARCHAR(MAX)       '$.observedGroup.identifier',
        [comparisonGroup.observedGroup.display] NVARCHAR(4000)      '$.observedGroup.display',
        [comparisonGroup.identifier.uri] VARCHAR(256)        '$.identifier.uri',
        [comparisonGroup.identifier.identifier.id] NVARCHAR(100)       '$.identifier.identifier.id',
        [comparisonGroup.identifier.identifier.extension] NVARCHAR(MAX)       '$.identifier.identifier.extension',
        [comparisonGroup.identifier.identifier.use] NVARCHAR(64)        '$.identifier.identifier.use',
        [comparisonGroup.identifier.identifier.type] NVARCHAR(MAX)       '$.identifier.identifier.type',
        [comparisonGroup.identifier.identifier.system] VARCHAR(256)        '$.identifier.identifier.system',
        [comparisonGroup.identifier.identifier.value] NVARCHAR(4000)      '$.identifier.identifier.value',
        [comparisonGroup.identifier.identifier.period] NVARCHAR(MAX)       '$.identifier.identifier.period',
        [comparisonGroup.identifier.identifier.assigner] NVARCHAR(MAX)       '$.identifier.identifier.assigner'
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
    [objective.type.text],
    [objective.description]
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
        [objective.type.text]          NVARCHAR(4000)      '$.type.text',
        [objective.description]        NVARCHAR(MAX)       '$.description'
    ) j

GO

CREATE VIEW fhir.ResearchStudyOutcomeMeasure AS
SELECT
    [id],
    [outcomeMeasure.JSON],
    [outcomeMeasure.id],
    [outcomeMeasure.extension],
    [outcomeMeasure.modifierExtension],
    [outcomeMeasure.name],
    [outcomeMeasure.type],
    [outcomeMeasure.description],
    [outcomeMeasure.reference.id],
    [outcomeMeasure.reference.extension],
    [outcomeMeasure.reference.reference],
    [outcomeMeasure.reference.type],
    [outcomeMeasure.reference.identifier],
    [outcomeMeasure.reference.display]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [outcomeMeasure.JSON]  VARCHAR(MAX) '$.outcomeMeasure'
    ) AS rowset
    CROSS APPLY openjson (rowset.[outcomeMeasure.JSON]) with (
        [outcomeMeasure.id]            NVARCHAR(100)       '$.id',
        [outcomeMeasure.extension]     NVARCHAR(MAX)       '$.extension',
        [outcomeMeasure.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [outcomeMeasure.name]          NVARCHAR(500)       '$.name',
        [outcomeMeasure.type]          NVARCHAR(MAX)       '$.type' AS JSON,
        [outcomeMeasure.description]   NVARCHAR(MAX)       '$.description',
        [outcomeMeasure.reference.id]  NVARCHAR(100)       '$.reference.id',
        [outcomeMeasure.reference.extension] NVARCHAR(MAX)       '$.reference.extension',
        [outcomeMeasure.reference.reference] NVARCHAR(4000)      '$.reference.reference',
        [outcomeMeasure.reference.type] VARCHAR(256)        '$.reference.type',
        [outcomeMeasure.reference.identifier] NVARCHAR(MAX)       '$.reference.identifier',
        [outcomeMeasure.reference.display] NVARCHAR(4000)      '$.reference.display'
    ) j

GO

CREATE VIEW fhir.ResearchStudyResult AS
SELECT
    [id],
    [result.JSON],
    [result.id],
    [result.extension],
    [result.reference],
    [result.type],
    [result.identifier.id],
    [result.identifier.extension],
    [result.identifier.use],
    [result.identifier.type],
    [result.identifier.system],
    [result.identifier.value],
    [result.identifier.period],
    [result.identifier.assigner],
    [result.display]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [result.JSON]  VARCHAR(MAX) '$.result'
    ) AS rowset
    CROSS APPLY openjson (rowset.[result.JSON]) with (
        [result.id]                    NVARCHAR(100)       '$.id',
        [result.extension]             NVARCHAR(MAX)       '$.extension',
        [result.reference]             NVARCHAR(4000)      '$.reference',
        [result.type]                  VARCHAR(256)        '$.type',
        [result.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [result.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [result.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [result.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [result.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [result.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [result.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [result.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [result.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ResearchStudyWebLocation AS
SELECT
    [id],
    [webLocation.JSON],
    [webLocation.id],
    [webLocation.extension],
    [webLocation.modifierExtension],
    [webLocation.type.id],
    [webLocation.type.extension],
    [webLocation.type.coding],
    [webLocation.type.text],
    [webLocation.url]
FROM openrowset (
        BULK 'ResearchStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [webLocation.JSON]  VARCHAR(MAX) '$.webLocation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[webLocation.JSON]) with (
        [webLocation.id]               NVARCHAR(100)       '$.id',
        [webLocation.extension]        NVARCHAR(MAX)       '$.extension',
        [webLocation.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [webLocation.type.id]          NVARCHAR(100)       '$.type.id',
        [webLocation.type.extension]   NVARCHAR(MAX)       '$.type.extension',
        [webLocation.type.coding]      NVARCHAR(MAX)       '$.type.coding',
        [webLocation.type.text]        NVARCHAR(4000)      '$.type.text',
        [webLocation.url]              VARCHAR(256)        '$.url'
    ) j
