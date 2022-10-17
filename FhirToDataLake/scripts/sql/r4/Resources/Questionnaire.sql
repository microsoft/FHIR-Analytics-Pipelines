CREATE EXTERNAL TABLE [fhir].[Questionnaire] (
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
    [derivedFrom] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [experimental] bit,
    [subjectType] VARCHAR(MAX),
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
    [code] VARCHAR(MAX),
    [item] VARCHAR(MAX),
) WITH (
    LOCATION='/Questionnaire/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.QuestionnaireIdentifier AS
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
        BULK 'Questionnaire/**',
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

CREATE VIEW fhir.QuestionnaireDerivedFrom AS
SELECT
    [id],
    [derivedFrom.JSON],
    [derivedFrom]
FROM openrowset (
        BULK 'Questionnaire/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [derivedFrom.JSON]  VARCHAR(MAX) '$.derivedFrom'
    ) AS rowset
    CROSS APPLY openjson (rowset.[derivedFrom.JSON]) with (
        [derivedFrom]                  NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.QuestionnaireSubjectType AS
SELECT
    [id],
    [subjectType.JSON],
    [subjectType]
FROM openrowset (
        BULK 'Questionnaire/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subjectType.JSON]  VARCHAR(MAX) '$.subjectType'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subjectType.JSON]) with (
        [subjectType]                  NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.QuestionnaireContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'Questionnaire/**',
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

CREATE VIEW fhir.QuestionnaireUseContext AS
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
        BULK 'Questionnaire/**',
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

CREATE VIEW fhir.QuestionnaireJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'Questionnaire/**',
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

CREATE VIEW fhir.QuestionnaireCode AS
SELECT
    [id],
    [code.JSON],
    [code.id],
    [code.extension],
    [code.system],
    [code.version],
    [code.code],
    [code.display],
    [code.userSelected]
FROM openrowset (
        BULK 'Questionnaire/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [code.JSON]  VARCHAR(MAX) '$.code'
    ) AS rowset
    CROSS APPLY openjson (rowset.[code.JSON]) with (
        [code.id]                      NVARCHAR(100)       '$.id',
        [code.extension]               NVARCHAR(MAX)       '$.extension',
        [code.system]                  VARCHAR(256)        '$.system',
        [code.version]                 NVARCHAR(100)       '$.version',
        [code.code]                    NVARCHAR(4000)      '$.code',
        [code.display]                 NVARCHAR(4000)      '$.display',
        [code.userSelected]            bit                 '$.userSelected'
    ) j

GO

CREATE VIEW fhir.QuestionnaireItem AS
SELECT
    [id],
    [item.JSON],
    [item.id],
    [item.extension],
    [item.modifierExtension],
    [item.linkId],
    [item.definition],
    [item.code],
    [item.prefix],
    [item.text],
    [item.type],
    [item.enableWhen],
    [item.enableBehavior],
    [item.required],
    [item.repeats],
    [item.readOnly],
    [item.maxLength],
    [item.answerValueSet],
    [item.answerOption],
    [item.initial],
    [item.item]
FROM openrowset (
        BULK 'Questionnaire/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [item.JSON]  VARCHAR(MAX) '$.item'
    ) AS rowset
    CROSS APPLY openjson (rowset.[item.JSON]) with (
        [item.id]                      NVARCHAR(100)       '$.id',
        [item.extension]               NVARCHAR(MAX)       '$.extension',
        [item.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [item.linkId]                  NVARCHAR(100)       '$.linkId',
        [item.definition]              VARCHAR(256)        '$.definition',
        [item.code]                    NVARCHAR(MAX)       '$.code' AS JSON,
        [item.prefix]                  NVARCHAR(500)       '$.prefix',
        [item.text]                    NVARCHAR(4000)      '$.text',
        [item.type]                    NVARCHAR(64)        '$.type',
        [item.enableWhen]              NVARCHAR(MAX)       '$.enableWhen' AS JSON,
        [item.enableBehavior]          NVARCHAR(64)        '$.enableBehavior',
        [item.required]                bit                 '$.required',
        [item.repeats]                 bit                 '$.repeats',
        [item.readOnly]                bit                 '$.readOnly',
        [item.maxLength]               bigint              '$.maxLength',
        [item.answerValueSet]          VARCHAR(256)        '$.answerValueSet',
        [item.answerOption]            NVARCHAR(MAX)       '$.answerOption' AS JSON,
        [item.initial]                 NVARCHAR(MAX)       '$.initial' AS JSON,
        [item.item]                    NVARCHAR(MAX)       '$.item' AS JSON
    ) j
