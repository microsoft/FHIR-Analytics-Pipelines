CREATE EXTERNAL TABLE [fhir].[ClinicalImpression] (
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
    [identifier] VARCHAR(MAX),
    [status] NVARCHAR(4000),
    [statusReason.id] NVARCHAR(4000),
    [statusReason.extension] NVARCHAR(MAX),
    [statusReason.coding] VARCHAR(MAX),
    [statusReason.text] NVARCHAR(4000),
    [code.id] NVARCHAR(4000),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [description] NVARCHAR(4000),
    [subject.id] NVARCHAR(4000),
    [subject.extension] NVARCHAR(MAX),
    [subject.reference] NVARCHAR(4000),
    [subject.type] VARCHAR(256),
    [subject.identifier.id] NVARCHAR(4000),
    [subject.identifier.extension] NVARCHAR(MAX),
    [subject.identifier.use] NVARCHAR(64),
    [subject.identifier.type] NVARCHAR(MAX),
    [subject.identifier.system] VARCHAR(256),
    [subject.identifier.value] NVARCHAR(4000),
    [subject.identifier.period] NVARCHAR(MAX),
    [subject.identifier.assigner] NVARCHAR(MAX),
    [subject.display] NVARCHAR(4000),
    [encounter.id] NVARCHAR(4000),
    [encounter.extension] NVARCHAR(MAX),
    [encounter.reference] NVARCHAR(4000),
    [encounter.type] VARCHAR(256),
    [encounter.identifier.id] NVARCHAR(4000),
    [encounter.identifier.extension] NVARCHAR(MAX),
    [encounter.identifier.use] NVARCHAR(64),
    [encounter.identifier.type] NVARCHAR(MAX),
    [encounter.identifier.system] VARCHAR(256),
    [encounter.identifier.value] NVARCHAR(4000),
    [encounter.identifier.period] NVARCHAR(MAX),
    [encounter.identifier.assigner] NVARCHAR(MAX),
    [encounter.display] NVARCHAR(4000),
    [date] VARCHAR(30),
    [assessor.id] NVARCHAR(4000),
    [assessor.extension] NVARCHAR(MAX),
    [assessor.reference] NVARCHAR(4000),
    [assessor.type] VARCHAR(256),
    [assessor.identifier.id] NVARCHAR(4000),
    [assessor.identifier.extension] NVARCHAR(MAX),
    [assessor.identifier.use] NVARCHAR(64),
    [assessor.identifier.type] NVARCHAR(MAX),
    [assessor.identifier.system] VARCHAR(256),
    [assessor.identifier.value] NVARCHAR(4000),
    [assessor.identifier.period] NVARCHAR(MAX),
    [assessor.identifier.assigner] NVARCHAR(MAX),
    [assessor.display] NVARCHAR(4000),
    [previous.id] NVARCHAR(4000),
    [previous.extension] NVARCHAR(MAX),
    [previous.reference] NVARCHAR(4000),
    [previous.type] VARCHAR(256),
    [previous.identifier.id] NVARCHAR(4000),
    [previous.identifier.extension] NVARCHAR(MAX),
    [previous.identifier.use] NVARCHAR(64),
    [previous.identifier.type] NVARCHAR(MAX),
    [previous.identifier.system] VARCHAR(256),
    [previous.identifier.value] NVARCHAR(4000),
    [previous.identifier.period] NVARCHAR(MAX),
    [previous.identifier.assigner] NVARCHAR(MAX),
    [previous.display] NVARCHAR(4000),
    [problem] VARCHAR(MAX),
    [investigation] VARCHAR(MAX),
    [protocol] VARCHAR(MAX),
    [summary] NVARCHAR(4000),
    [finding] VARCHAR(MAX),
    [prognosisCodeableConcept] VARCHAR(MAX),
    [prognosisReference] VARCHAR(MAX),
    [supportingInfo] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [effective.dateTime] VARCHAR(30),
    [effective.Period.id] NVARCHAR(4000),
    [effective.Period.extension] NVARCHAR(MAX),
    [effective.Period.start] VARCHAR(30),
    [effective.Period.end] VARCHAR(30),
) WITH (
    LOCATION='/ClinicalImpression/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ClinicalImpressionIdentifier AS
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
        BULK 'ClinicalImpression/**',
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

CREATE VIEW fhir.ClinicalImpressionProblem AS
SELECT
    [id],
    [problem.JSON],
    [problem.id],
    [problem.extension],
    [problem.reference],
    [problem.type],
    [problem.identifier.id],
    [problem.identifier.extension],
    [problem.identifier.use],
    [problem.identifier.type],
    [problem.identifier.system],
    [problem.identifier.value],
    [problem.identifier.period],
    [problem.identifier.assigner],
    [problem.display]
FROM openrowset (
        BULK 'ClinicalImpression/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [problem.JSON]  VARCHAR(MAX) '$.problem'
    ) AS rowset
    CROSS APPLY openjson (rowset.[problem.JSON]) with (
        [problem.id]                   NVARCHAR(4000)      '$.id',
        [problem.extension]            NVARCHAR(MAX)       '$.extension',
        [problem.reference]            NVARCHAR(4000)      '$.reference',
        [problem.type]                 VARCHAR(256)        '$.type',
        [problem.identifier.id]        NVARCHAR(4000)      '$.identifier.id',
        [problem.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [problem.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [problem.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [problem.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [problem.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [problem.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [problem.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [problem.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ClinicalImpressionInvestigation AS
SELECT
    [id],
    [investigation.JSON],
    [investigation.id],
    [investigation.extension],
    [investigation.modifierExtension],
    [investigation.code.id],
    [investigation.code.extension],
    [investigation.code.coding],
    [investigation.code.text],
    [investigation.item]
FROM openrowset (
        BULK 'ClinicalImpression/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [investigation.JSON]  VARCHAR(MAX) '$.investigation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[investigation.JSON]) with (
        [investigation.id]             NVARCHAR(4000)      '$.id',
        [investigation.extension]      NVARCHAR(MAX)       '$.extension',
        [investigation.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [investigation.code.id]        NVARCHAR(4000)      '$.code.id',
        [investigation.code.extension] NVARCHAR(MAX)       '$.code.extension',
        [investigation.code.coding]    NVARCHAR(MAX)       '$.code.coding',
        [investigation.code.text]      NVARCHAR(4000)      '$.code.text',
        [investigation.item]           NVARCHAR(MAX)       '$.item' AS JSON
    ) j

GO

CREATE VIEW fhir.ClinicalImpressionProtocol AS
SELECT
    [id],
    [protocol.JSON],
    [protocol]
FROM openrowset (
        BULK 'ClinicalImpression/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [protocol.JSON]  VARCHAR(MAX) '$.protocol'
    ) AS rowset
    CROSS APPLY openjson (rowset.[protocol.JSON]) with (
        [protocol]                     NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ClinicalImpressionFinding AS
SELECT
    [id],
    [finding.JSON],
    [finding.id],
    [finding.extension],
    [finding.modifierExtension],
    [finding.itemCodeableConcept.id],
    [finding.itemCodeableConcept.extension],
    [finding.itemCodeableConcept.coding],
    [finding.itemCodeableConcept.text],
    [finding.itemReference.id],
    [finding.itemReference.extension],
    [finding.itemReference.reference],
    [finding.itemReference.type],
    [finding.itemReference.identifier],
    [finding.itemReference.display],
    [finding.basis]
FROM openrowset (
        BULK 'ClinicalImpression/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [finding.JSON]  VARCHAR(MAX) '$.finding'
    ) AS rowset
    CROSS APPLY openjson (rowset.[finding.JSON]) with (
        [finding.id]                   NVARCHAR(4000)      '$.id',
        [finding.extension]            NVARCHAR(MAX)       '$.extension',
        [finding.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [finding.itemCodeableConcept.id] NVARCHAR(4000)      '$.itemCodeableConcept.id',
        [finding.itemCodeableConcept.extension] NVARCHAR(MAX)       '$.itemCodeableConcept.extension',
        [finding.itemCodeableConcept.coding] NVARCHAR(MAX)       '$.itemCodeableConcept.coding',
        [finding.itemCodeableConcept.text] NVARCHAR(4000)      '$.itemCodeableConcept.text',
        [finding.itemReference.id]     NVARCHAR(4000)      '$.itemReference.id',
        [finding.itemReference.extension] NVARCHAR(MAX)       '$.itemReference.extension',
        [finding.itemReference.reference] NVARCHAR(4000)      '$.itemReference.reference',
        [finding.itemReference.type]   VARCHAR(256)        '$.itemReference.type',
        [finding.itemReference.identifier] NVARCHAR(MAX)       '$.itemReference.identifier',
        [finding.itemReference.display] NVARCHAR(4000)      '$.itemReference.display',
        [finding.basis]                NVARCHAR(4000)      '$.basis'
    ) j

GO

CREATE VIEW fhir.ClinicalImpressionPrognosisCodeableConcept AS
SELECT
    [id],
    [prognosisCodeableConcept.JSON],
    [prognosisCodeableConcept.id],
    [prognosisCodeableConcept.extension],
    [prognosisCodeableConcept.coding],
    [prognosisCodeableConcept.text]
FROM openrowset (
        BULK 'ClinicalImpression/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [prognosisCodeableConcept.JSON]  VARCHAR(MAX) '$.prognosisCodeableConcept'
    ) AS rowset
    CROSS APPLY openjson (rowset.[prognosisCodeableConcept.JSON]) with (
        [prognosisCodeableConcept.id]  NVARCHAR(4000)      '$.id',
        [prognosisCodeableConcept.extension] NVARCHAR(MAX)       '$.extension',
        [prognosisCodeableConcept.coding] NVARCHAR(MAX)       '$.coding' AS JSON,
        [prognosisCodeableConcept.text] NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ClinicalImpressionPrognosisReference AS
SELECT
    [id],
    [prognosisReference.JSON],
    [prognosisReference.id],
    [prognosisReference.extension],
    [prognosisReference.reference],
    [prognosisReference.type],
    [prognosisReference.identifier.id],
    [prognosisReference.identifier.extension],
    [prognosisReference.identifier.use],
    [prognosisReference.identifier.type],
    [prognosisReference.identifier.system],
    [prognosisReference.identifier.value],
    [prognosisReference.identifier.period],
    [prognosisReference.identifier.assigner],
    [prognosisReference.display]
FROM openrowset (
        BULK 'ClinicalImpression/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [prognosisReference.JSON]  VARCHAR(MAX) '$.prognosisReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[prognosisReference.JSON]) with (
        [prognosisReference.id]        NVARCHAR(4000)      '$.id',
        [prognosisReference.extension] NVARCHAR(MAX)       '$.extension',
        [prognosisReference.reference] NVARCHAR(4000)      '$.reference',
        [prognosisReference.type]      VARCHAR(256)        '$.type',
        [prognosisReference.identifier.id] NVARCHAR(4000)      '$.identifier.id',
        [prognosisReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [prognosisReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [prognosisReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [prognosisReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [prognosisReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [prognosisReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [prognosisReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [prognosisReference.display]   NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ClinicalImpressionSupportingInfo AS
SELECT
    [id],
    [supportingInfo.JSON],
    [supportingInfo.id],
    [supportingInfo.extension],
    [supportingInfo.reference],
    [supportingInfo.type],
    [supportingInfo.identifier.id],
    [supportingInfo.identifier.extension],
    [supportingInfo.identifier.use],
    [supportingInfo.identifier.type],
    [supportingInfo.identifier.system],
    [supportingInfo.identifier.value],
    [supportingInfo.identifier.period],
    [supportingInfo.identifier.assigner],
    [supportingInfo.display]
FROM openrowset (
        BULK 'ClinicalImpression/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [supportingInfo.JSON]  VARCHAR(MAX) '$.supportingInfo'
    ) AS rowset
    CROSS APPLY openjson (rowset.[supportingInfo.JSON]) with (
        [supportingInfo.id]            NVARCHAR(4000)      '$.id',
        [supportingInfo.extension]     NVARCHAR(MAX)       '$.extension',
        [supportingInfo.reference]     NVARCHAR(4000)      '$.reference',
        [supportingInfo.type]          VARCHAR(256)        '$.type',
        [supportingInfo.identifier.id] NVARCHAR(4000)      '$.identifier.id',
        [supportingInfo.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [supportingInfo.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [supportingInfo.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [supportingInfo.identifier.system] VARCHAR(256)        '$.identifier.system',
        [supportingInfo.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [supportingInfo.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [supportingInfo.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [supportingInfo.display]       NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ClinicalImpressionNote AS
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
        BULK 'ClinicalImpression/**',
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
