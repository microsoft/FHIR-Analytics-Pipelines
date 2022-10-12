CREATE EXTERNAL TABLE [fhir].[MedicinalProductUndesirableEffect] (
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
    [subject] VARCHAR(MAX),
    [symptomConditionEffect.id] NVARCHAR(100),
    [symptomConditionEffect.extension] NVARCHAR(MAX),
    [symptomConditionEffect.coding] VARCHAR(MAX),
    [symptomConditionEffect.text] NVARCHAR(4000),
    [classification.id] NVARCHAR(100),
    [classification.extension] NVARCHAR(MAX),
    [classification.coding] VARCHAR(MAX),
    [classification.text] NVARCHAR(4000),
    [frequencyOfOccurrence.id] NVARCHAR(100),
    [frequencyOfOccurrence.extension] NVARCHAR(MAX),
    [frequencyOfOccurrence.coding] VARCHAR(MAX),
    [frequencyOfOccurrence.text] NVARCHAR(4000),
    [population] VARCHAR(MAX),
) WITH (
    LOCATION='/MedicinalProductUndesirableEffect/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicinalProductUndesirableEffectSubject AS
SELECT
    [id],
    [subject.JSON],
    [subject.id],
    [subject.extension],
    [subject.reference],
    [subject.type],
    [subject.identifier.id],
    [subject.identifier.extension],
    [subject.identifier.use],
    [subject.identifier.type],
    [subject.identifier.system],
    [subject.identifier.value],
    [subject.identifier.period],
    [subject.identifier.assigner],
    [subject.display]
FROM openrowset (
        BULK 'MedicinalProductUndesirableEffect/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subject.JSON]  VARCHAR(MAX) '$.subject'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subject.JSON]) with (
        [subject.id]                   NVARCHAR(100)       '$.id',
        [subject.extension]            NVARCHAR(MAX)       '$.extension',
        [subject.reference]            NVARCHAR(4000)      '$.reference',
        [subject.type]                 VARCHAR(256)        '$.type',
        [subject.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [subject.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [subject.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [subject.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [subject.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [subject.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [subject.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [subject.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [subject.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductUndesirableEffectPopulation AS
SELECT
    [id],
    [population.JSON],
    [population.id],
    [population.extension],
    [population.modifierExtension],
    [population.gender.id],
    [population.gender.extension],
    [population.gender.coding],
    [population.gender.text],
    [population.race.id],
    [population.race.extension],
    [population.race.coding],
    [population.race.text],
    [population.physiologicalCondition.id],
    [population.physiologicalCondition.extension],
    [population.physiologicalCondition.coding],
    [population.physiologicalCondition.text],
    [population.age.range.id],
    [population.age.range.extension],
    [population.age.range.low],
    [population.age.range.high],
    [population.age.codeableConcept.id],
    [population.age.codeableConcept.extension],
    [population.age.codeableConcept.coding],
    [population.age.codeableConcept.text]
FROM openrowset (
        BULK 'MedicinalProductUndesirableEffect/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [population.JSON]  VARCHAR(MAX) '$.population'
    ) AS rowset
    CROSS APPLY openjson (rowset.[population.JSON]) with (
        [population.id]                NVARCHAR(100)       '$.id',
        [population.extension]         NVARCHAR(MAX)       '$.extension',
        [population.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [population.gender.id]         NVARCHAR(100)       '$.gender.id',
        [population.gender.extension]  NVARCHAR(MAX)       '$.gender.extension',
        [population.gender.coding]     NVARCHAR(MAX)       '$.gender.coding',
        [population.gender.text]       NVARCHAR(4000)      '$.gender.text',
        [population.race.id]           NVARCHAR(100)       '$.race.id',
        [population.race.extension]    NVARCHAR(MAX)       '$.race.extension',
        [population.race.coding]       NVARCHAR(MAX)       '$.race.coding',
        [population.race.text]         NVARCHAR(4000)      '$.race.text',
        [population.physiologicalCondition.id] NVARCHAR(100)       '$.physiologicalCondition.id',
        [population.physiologicalCondition.extension] NVARCHAR(MAX)       '$.physiologicalCondition.extension',
        [population.physiologicalCondition.coding] NVARCHAR(MAX)       '$.physiologicalCondition.coding',
        [population.physiologicalCondition.text] NVARCHAR(4000)      '$.physiologicalCondition.text',
        [population.age.range.id]      NVARCHAR(100)       '$.age.range.id',
        [population.age.range.extension] NVARCHAR(MAX)       '$.age.range.extension',
        [population.age.range.low]     NVARCHAR(MAX)       '$.age.range.low',
        [population.age.range.high]    NVARCHAR(MAX)       '$.age.range.high',
        [population.age.codeableConcept.id] NVARCHAR(100)       '$.age.codeableConcept.id',
        [population.age.codeableConcept.extension] NVARCHAR(MAX)       '$.age.codeableConcept.extension',
        [population.age.codeableConcept.coding] NVARCHAR(MAX)       '$.age.codeableConcept.coding',
        [population.age.codeableConcept.text] NVARCHAR(4000)      '$.age.codeableConcept.text'
    ) j
