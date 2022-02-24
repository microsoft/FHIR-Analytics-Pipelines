CREATE EXTERNAL TABLE [fhir].[AdverseEvent] (
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
    [identifier.id] NVARCHAR(100),
    [identifier.extension] NVARCHAR(MAX),
    [identifier.use] NVARCHAR(64),
    [identifier.type.id] NVARCHAR(100),
    [identifier.type.extension] NVARCHAR(MAX),
    [identifier.type.coding] NVARCHAR(MAX),
    [identifier.type.text] NVARCHAR(4000),
    [identifier.system] VARCHAR(256),
    [identifier.value] NVARCHAR(4000),
    [identifier.period.id] NVARCHAR(100),
    [identifier.period.extension] NVARCHAR(MAX),
    [identifier.period.start] VARCHAR(64),
    [identifier.period.end] VARCHAR(64),
    [identifier.assigner.id] NVARCHAR(100),
    [identifier.assigner.extension] NVARCHAR(MAX),
    [identifier.assigner.reference] NVARCHAR(4000),
    [identifier.assigner.type] VARCHAR(256),
    [identifier.assigner.identifier] NVARCHAR(MAX),
    [identifier.assigner.display] NVARCHAR(4000),
    [actuality] NVARCHAR(64),
    [category] VARCHAR(MAX),
    [event.id] NVARCHAR(100),
    [event.extension] NVARCHAR(MAX),
    [event.coding] VARCHAR(MAX),
    [event.text] NVARCHAR(4000),
    [subject.id] NVARCHAR(100),
    [subject.extension] NVARCHAR(MAX),
    [subject.reference] NVARCHAR(4000),
    [subject.type] VARCHAR(256),
    [subject.identifier.id] NVARCHAR(100),
    [subject.identifier.extension] NVARCHAR(MAX),
    [subject.identifier.use] NVARCHAR(64),
    [subject.identifier.type] NVARCHAR(MAX),
    [subject.identifier.system] VARCHAR(256),
    [subject.identifier.value] NVARCHAR(4000),
    [subject.identifier.period] NVARCHAR(MAX),
    [subject.identifier.assigner] NVARCHAR(MAX),
    [subject.display] NVARCHAR(4000),
    [encounter.id] NVARCHAR(100),
    [encounter.extension] NVARCHAR(MAX),
    [encounter.reference] NVARCHAR(4000),
    [encounter.type] VARCHAR(256),
    [encounter.identifier.id] NVARCHAR(100),
    [encounter.identifier.extension] NVARCHAR(MAX),
    [encounter.identifier.use] NVARCHAR(64),
    [encounter.identifier.type] NVARCHAR(MAX),
    [encounter.identifier.system] VARCHAR(256),
    [encounter.identifier.value] NVARCHAR(4000),
    [encounter.identifier.period] NVARCHAR(MAX),
    [encounter.identifier.assigner] NVARCHAR(MAX),
    [encounter.display] NVARCHAR(4000),
    [date] VARCHAR(64),
    [detected] VARCHAR(64),
    [recordedDate] VARCHAR(64),
    [resultingCondition] VARCHAR(MAX),
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
    [seriousness.id] NVARCHAR(100),
    [seriousness.extension] NVARCHAR(MAX),
    [seriousness.coding] VARCHAR(MAX),
    [seriousness.text] NVARCHAR(4000),
    [severity.id] NVARCHAR(100),
    [severity.extension] NVARCHAR(MAX),
    [severity.coding] VARCHAR(MAX),
    [severity.text] NVARCHAR(4000),
    [outcome.id] NVARCHAR(100),
    [outcome.extension] NVARCHAR(MAX),
    [outcome.coding] VARCHAR(MAX),
    [outcome.text] NVARCHAR(4000),
    [recorder.id] NVARCHAR(100),
    [recorder.extension] NVARCHAR(MAX),
    [recorder.reference] NVARCHAR(4000),
    [recorder.type] VARCHAR(256),
    [recorder.identifier.id] NVARCHAR(100),
    [recorder.identifier.extension] NVARCHAR(MAX),
    [recorder.identifier.use] NVARCHAR(64),
    [recorder.identifier.type] NVARCHAR(MAX),
    [recorder.identifier.system] VARCHAR(256),
    [recorder.identifier.value] NVARCHAR(4000),
    [recorder.identifier.period] NVARCHAR(MAX),
    [recorder.identifier.assigner] NVARCHAR(MAX),
    [recorder.display] NVARCHAR(4000),
    [contributor] VARCHAR(MAX),
    [suspectEntity] VARCHAR(MAX),
    [subjectMedicalHistory] VARCHAR(MAX),
    [referenceDocument] VARCHAR(MAX),
    [study] VARCHAR(MAX),
) WITH (
    LOCATION='/AdverseEvent/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.AdverseEventCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'AdverseEvent/**',
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

CREATE VIEW fhir.AdverseEventResultingCondition AS
SELECT
    [id],
    [resultingCondition.JSON],
    [resultingCondition.id],
    [resultingCondition.extension],
    [resultingCondition.reference],
    [resultingCondition.type],
    [resultingCondition.identifier.id],
    [resultingCondition.identifier.extension],
    [resultingCondition.identifier.use],
    [resultingCondition.identifier.type],
    [resultingCondition.identifier.system],
    [resultingCondition.identifier.value],
    [resultingCondition.identifier.period],
    [resultingCondition.identifier.assigner],
    [resultingCondition.display]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [resultingCondition.JSON]  VARCHAR(MAX) '$.resultingCondition'
    ) AS rowset
    CROSS APPLY openjson (rowset.[resultingCondition.JSON]) with (
        [resultingCondition.id]        NVARCHAR(100)       '$.id',
        [resultingCondition.extension] NVARCHAR(MAX)       '$.extension',
        [resultingCondition.reference] NVARCHAR(4000)      '$.reference',
        [resultingCondition.type]      VARCHAR(256)        '$.type',
        [resultingCondition.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [resultingCondition.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [resultingCondition.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [resultingCondition.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [resultingCondition.identifier.system] VARCHAR(256)        '$.identifier.system',
        [resultingCondition.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [resultingCondition.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [resultingCondition.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [resultingCondition.display]   NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.AdverseEventContributor AS
SELECT
    [id],
    [contributor.JSON],
    [contributor.id],
    [contributor.extension],
    [contributor.reference],
    [contributor.type],
    [contributor.identifier.id],
    [contributor.identifier.extension],
    [contributor.identifier.use],
    [contributor.identifier.type],
    [contributor.identifier.system],
    [contributor.identifier.value],
    [contributor.identifier.period],
    [contributor.identifier.assigner],
    [contributor.display]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contributor.JSON]  VARCHAR(MAX) '$.contributor'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contributor.JSON]) with (
        [contributor.id]               NVARCHAR(100)       '$.id',
        [contributor.extension]        NVARCHAR(MAX)       '$.extension',
        [contributor.reference]        NVARCHAR(4000)      '$.reference',
        [contributor.type]             VARCHAR(256)        '$.type',
        [contributor.identifier.id]    NVARCHAR(100)       '$.identifier.id',
        [contributor.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [contributor.identifier.use]   NVARCHAR(64)        '$.identifier.use',
        [contributor.identifier.type]  NVARCHAR(MAX)       '$.identifier.type',
        [contributor.identifier.system] VARCHAR(256)        '$.identifier.system',
        [contributor.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [contributor.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [contributor.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [contributor.display]          NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.AdverseEventSuspectEntity AS
SELECT
    [id],
    [suspectEntity.JSON],
    [suspectEntity.id],
    [suspectEntity.extension],
    [suspectEntity.modifierExtension],
    [suspectEntity.instance.id],
    [suspectEntity.instance.extension],
    [suspectEntity.instance.reference],
    [suspectEntity.instance.type],
    [suspectEntity.instance.identifier],
    [suspectEntity.instance.display],
    [suspectEntity.causality]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [suspectEntity.JSON]  VARCHAR(MAX) '$.suspectEntity'
    ) AS rowset
    CROSS APPLY openjson (rowset.[suspectEntity.JSON]) with (
        [suspectEntity.id]             NVARCHAR(100)       '$.id',
        [suspectEntity.extension]      NVARCHAR(MAX)       '$.extension',
        [suspectEntity.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [suspectEntity.instance.id]    NVARCHAR(100)       '$.instance.id',
        [suspectEntity.instance.extension] NVARCHAR(MAX)       '$.instance.extension',
        [suspectEntity.instance.reference] NVARCHAR(4000)      '$.instance.reference',
        [suspectEntity.instance.type]  VARCHAR(256)        '$.instance.type',
        [suspectEntity.instance.identifier] NVARCHAR(MAX)       '$.instance.identifier',
        [suspectEntity.instance.display] NVARCHAR(4000)      '$.instance.display',
        [suspectEntity.causality]      NVARCHAR(MAX)       '$.causality' AS JSON
    ) j

GO

CREATE VIEW fhir.AdverseEventSubjectMedicalHistory AS
SELECT
    [id],
    [subjectMedicalHistory.JSON],
    [subjectMedicalHistory.id],
    [subjectMedicalHistory.extension],
    [subjectMedicalHistory.reference],
    [subjectMedicalHistory.type],
    [subjectMedicalHistory.identifier.id],
    [subjectMedicalHistory.identifier.extension],
    [subjectMedicalHistory.identifier.use],
    [subjectMedicalHistory.identifier.type],
    [subjectMedicalHistory.identifier.system],
    [subjectMedicalHistory.identifier.value],
    [subjectMedicalHistory.identifier.period],
    [subjectMedicalHistory.identifier.assigner],
    [subjectMedicalHistory.display]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subjectMedicalHistory.JSON]  VARCHAR(MAX) '$.subjectMedicalHistory'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subjectMedicalHistory.JSON]) with (
        [subjectMedicalHistory.id]     NVARCHAR(100)       '$.id',
        [subjectMedicalHistory.extension] NVARCHAR(MAX)       '$.extension',
        [subjectMedicalHistory.reference] NVARCHAR(4000)      '$.reference',
        [subjectMedicalHistory.type]   VARCHAR(256)        '$.type',
        [subjectMedicalHistory.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [subjectMedicalHistory.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [subjectMedicalHistory.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [subjectMedicalHistory.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [subjectMedicalHistory.identifier.system] VARCHAR(256)        '$.identifier.system',
        [subjectMedicalHistory.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [subjectMedicalHistory.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [subjectMedicalHistory.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [subjectMedicalHistory.display] NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.AdverseEventReferenceDocument AS
SELECT
    [id],
    [referenceDocument.JSON],
    [referenceDocument.id],
    [referenceDocument.extension],
    [referenceDocument.reference],
    [referenceDocument.type],
    [referenceDocument.identifier.id],
    [referenceDocument.identifier.extension],
    [referenceDocument.identifier.use],
    [referenceDocument.identifier.type],
    [referenceDocument.identifier.system],
    [referenceDocument.identifier.value],
    [referenceDocument.identifier.period],
    [referenceDocument.identifier.assigner],
    [referenceDocument.display]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [referenceDocument.JSON]  VARCHAR(MAX) '$.referenceDocument'
    ) AS rowset
    CROSS APPLY openjson (rowset.[referenceDocument.JSON]) with (
        [referenceDocument.id]         NVARCHAR(100)       '$.id',
        [referenceDocument.extension]  NVARCHAR(MAX)       '$.extension',
        [referenceDocument.reference]  NVARCHAR(4000)      '$.reference',
        [referenceDocument.type]       VARCHAR(256)        '$.type',
        [referenceDocument.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [referenceDocument.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [referenceDocument.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [referenceDocument.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [referenceDocument.identifier.system] VARCHAR(256)        '$.identifier.system',
        [referenceDocument.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [referenceDocument.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [referenceDocument.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [referenceDocument.display]    NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.AdverseEventStudy AS
SELECT
    [id],
    [study.JSON],
    [study.id],
    [study.extension],
    [study.reference],
    [study.type],
    [study.identifier.id],
    [study.identifier.extension],
    [study.identifier.use],
    [study.identifier.type],
    [study.identifier.system],
    [study.identifier.value],
    [study.identifier.period],
    [study.identifier.assigner],
    [study.display]
FROM openrowset (
        BULK 'AdverseEvent/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [study.JSON]  VARCHAR(MAX) '$.study'
    ) AS rowset
    CROSS APPLY openjson (rowset.[study.JSON]) with (
        [study.id]                     NVARCHAR(100)       '$.id',
        [study.extension]              NVARCHAR(MAX)       '$.extension',
        [study.reference]              NVARCHAR(4000)      '$.reference',
        [study.type]                   VARCHAR(256)        '$.type',
        [study.identifier.id]          NVARCHAR(100)       '$.identifier.id',
        [study.identifier.extension]   NVARCHAR(MAX)       '$.identifier.extension',
        [study.identifier.use]         NVARCHAR(64)        '$.identifier.use',
        [study.identifier.type]        NVARCHAR(MAX)       '$.identifier.type',
        [study.identifier.system]      VARCHAR(256)        '$.identifier.system',
        [study.identifier.value]       NVARCHAR(4000)      '$.identifier.value',
        [study.identifier.period]      NVARCHAR(MAX)       '$.identifier.period',
        [study.identifier.assigner]    NVARCHAR(MAX)       '$.identifier.assigner',
        [study.display]                NVARCHAR(4000)      '$.display'
    ) j
