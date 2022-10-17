CREATE EXTERNAL TABLE [fhir].[ClinicalUseDefinition] (
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
    [type] NVARCHAR(100),
    [category] VARCHAR(MAX),
    [subject] VARCHAR(MAX),
    [status.id] NVARCHAR(100),
    [status.extension] NVARCHAR(MAX),
    [status.coding] VARCHAR(MAX),
    [status.text] NVARCHAR(4000),
    [contraindication.id] NVARCHAR(100),
    [contraindication.extension] NVARCHAR(MAX),
    [contraindication.modifierExtension] NVARCHAR(MAX),
    [contraindication.diseaseSymptomProcedure.id] NVARCHAR(100),
    [contraindication.diseaseSymptomProcedure.extension] NVARCHAR(MAX),
    [contraindication.diseaseSymptomProcedure.concept] NVARCHAR(MAX),
    [contraindication.diseaseSymptomProcedure.reference] NVARCHAR(MAX),
    [contraindication.diseaseStatus.id] NVARCHAR(100),
    [contraindication.diseaseStatus.extension] NVARCHAR(MAX),
    [contraindication.diseaseStatus.concept] NVARCHAR(MAX),
    [contraindication.diseaseStatus.reference] NVARCHAR(MAX),
    [contraindication.comorbidity] VARCHAR(MAX),
    [contraindication.indication] VARCHAR(MAX),
    [contraindication.otherTherapy] VARCHAR(MAX),
    [indication.id] NVARCHAR(100),
    [indication.extension] NVARCHAR(MAX),
    [indication.modifierExtension] NVARCHAR(MAX),
    [indication.diseaseSymptomProcedure.id] NVARCHAR(100),
    [indication.diseaseSymptomProcedure.extension] NVARCHAR(MAX),
    [indication.diseaseSymptomProcedure.concept] NVARCHAR(MAX),
    [indication.diseaseSymptomProcedure.reference] NVARCHAR(MAX),
    [indication.diseaseStatus.id] NVARCHAR(100),
    [indication.diseaseStatus.extension] NVARCHAR(MAX),
    [indication.diseaseStatus.concept] NVARCHAR(MAX),
    [indication.diseaseStatus.reference] NVARCHAR(MAX),
    [indication.comorbidity] VARCHAR(MAX),
    [indication.intendedEffect.id] NVARCHAR(100),
    [indication.intendedEffect.extension] NVARCHAR(MAX),
    [indication.intendedEffect.concept] NVARCHAR(MAX),
    [indication.intendedEffect.reference] NVARCHAR(MAX),
    [indication.duration.id] NVARCHAR(100),
    [indication.duration.extension] NVARCHAR(MAX),
    [indication.duration.value] float,
    [indication.duration.comparator] NVARCHAR(64),
    [indication.duration.unit] NVARCHAR(100),
    [indication.duration.system] VARCHAR(256),
    [indication.duration.code] NVARCHAR(4000),
    [indication.undesirableEffect] VARCHAR(MAX),
    [indication.otherTherapy] VARCHAR(MAX),
    [interaction.id] NVARCHAR(100),
    [interaction.extension] NVARCHAR(MAX),
    [interaction.modifierExtension] NVARCHAR(MAX),
    [interaction.interactant] VARCHAR(MAX),
    [interaction.type.id] NVARCHAR(100),
    [interaction.type.extension] NVARCHAR(MAX),
    [interaction.type.coding] NVARCHAR(MAX),
    [interaction.type.text] NVARCHAR(4000),
    [interaction.effect.id] NVARCHAR(100),
    [interaction.effect.extension] NVARCHAR(MAX),
    [interaction.effect.concept] NVARCHAR(MAX),
    [interaction.effect.reference] NVARCHAR(MAX),
    [interaction.incidence.id] NVARCHAR(100),
    [interaction.incidence.extension] NVARCHAR(MAX),
    [interaction.incidence.coding] NVARCHAR(MAX),
    [interaction.incidence.text] NVARCHAR(4000),
    [interaction.management] VARCHAR(MAX),
    [population] VARCHAR(MAX),
    [undesirableEffect.id] NVARCHAR(100),
    [undesirableEffect.extension] NVARCHAR(MAX),
    [undesirableEffect.modifierExtension] NVARCHAR(MAX),
    [undesirableEffect.symptomConditionEffect.id] NVARCHAR(100),
    [undesirableEffect.symptomConditionEffect.extension] NVARCHAR(MAX),
    [undesirableEffect.symptomConditionEffect.concept] NVARCHAR(MAX),
    [undesirableEffect.symptomConditionEffect.reference] NVARCHAR(MAX),
    [undesirableEffect.classification.id] NVARCHAR(100),
    [undesirableEffect.classification.extension] NVARCHAR(MAX),
    [undesirableEffect.classification.coding] NVARCHAR(MAX),
    [undesirableEffect.classification.text] NVARCHAR(4000),
    [undesirableEffect.frequencyOfOccurrence.id] NVARCHAR(100),
    [undesirableEffect.frequencyOfOccurrence.extension] NVARCHAR(MAX),
    [undesirableEffect.frequencyOfOccurrence.coding] NVARCHAR(MAX),
    [undesirableEffect.frequencyOfOccurrence.text] NVARCHAR(4000),
    [warning.id] NVARCHAR(100),
    [warning.extension] NVARCHAR(MAX),
    [warning.modifierExtension] NVARCHAR(MAX),
    [warning.description] NVARCHAR(MAX),
    [warning.code.id] NVARCHAR(100),
    [warning.code.extension] NVARCHAR(MAX),
    [warning.code.coding] NVARCHAR(MAX),
    [warning.code.text] NVARCHAR(4000),
) WITH (
    LOCATION='/ClinicalUseDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ClinicalUseDefinitionIdentifier AS
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
        BULK 'ClinicalUseDefinition/**',
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

CREATE VIEW fhir.ClinicalUseDefinitionCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'ClinicalUseDefinition/**',
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

CREATE VIEW fhir.ClinicalUseDefinitionSubject AS
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
        BULK 'ClinicalUseDefinition/**',
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

CREATE VIEW fhir.ClinicalUseDefinitionPopulation AS
SELECT
    [id],
    [population.JSON],
    [population.id],
    [population.extension],
    [population.reference],
    [population.type],
    [population.identifier.id],
    [population.identifier.extension],
    [population.identifier.use],
    [population.identifier.type],
    [population.identifier.system],
    [population.identifier.value],
    [population.identifier.period],
    [population.identifier.assigner],
    [population.display]
FROM openrowset (
        BULK 'ClinicalUseDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [population.JSON]  VARCHAR(MAX) '$.population'
    ) AS rowset
    CROSS APPLY openjson (rowset.[population.JSON]) with (
        [population.id]                NVARCHAR(100)       '$.id',
        [population.extension]         NVARCHAR(MAX)       '$.extension',
        [population.reference]         NVARCHAR(4000)      '$.reference',
        [population.type]              VARCHAR(256)        '$.type',
        [population.identifier.id]     NVARCHAR(100)       '$.identifier.id',
        [population.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [population.identifier.use]    NVARCHAR(64)        '$.identifier.use',
        [population.identifier.type]   NVARCHAR(MAX)       '$.identifier.type',
        [population.identifier.system] VARCHAR(256)        '$.identifier.system',
        [population.identifier.value]  NVARCHAR(4000)      '$.identifier.value',
        [population.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [population.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [population.display]           NVARCHAR(4000)      '$.display'
    ) j
