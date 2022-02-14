CREATE EXTERNAL TABLE [fhir].[CarePlan] (
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
    [instantiatesCanonical] VARCHAR(MAX),
    [instantiatesUri] VARCHAR(MAX),
    [basedOn] VARCHAR(MAX),
    [replaces] VARCHAR(MAX),
    [partOf] VARCHAR(MAX),
    [status] NVARCHAR(100),
    [intent] NVARCHAR(100),
    [category] VARCHAR(MAX),
    [title] NVARCHAR(4000),
    [description] NVARCHAR(4000),
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
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [created] VARCHAR(64),
    [author.id] NVARCHAR(100),
    [author.extension] NVARCHAR(MAX),
    [author.reference] NVARCHAR(4000),
    [author.type] VARCHAR(256),
    [author.identifier.id] NVARCHAR(100),
    [author.identifier.extension] NVARCHAR(MAX),
    [author.identifier.use] NVARCHAR(64),
    [author.identifier.type] NVARCHAR(MAX),
    [author.identifier.system] VARCHAR(256),
    [author.identifier.value] NVARCHAR(4000),
    [author.identifier.period] NVARCHAR(MAX),
    [author.identifier.assigner] NVARCHAR(MAX),
    [author.display] NVARCHAR(4000),
    [contributor] VARCHAR(MAX),
    [careTeam] VARCHAR(MAX),
    [addresses] VARCHAR(MAX),
    [supportingInfo] VARCHAR(MAX),
    [goal] VARCHAR(MAX),
    [activity] VARCHAR(MAX),
    [note] VARCHAR(MAX),
) WITH (
    LOCATION='/CarePlan/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.CarePlanIdentifier AS
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
        BULK 'CarePlan/**',
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

CREATE VIEW fhir.CarePlanInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'CarePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesCanonical.JSON]  VARCHAR(MAX) '$.instantiatesCanonical'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesCanonical.JSON]) with (
        [instantiatesCanonical]        NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.CarePlanInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'CarePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesUri.JSON]  VARCHAR(MAX) '$.instantiatesUri'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesUri.JSON]) with (
        [instantiatesUri]              NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.CarePlanBasedOn AS
SELECT
    [id],
    [basedOn.JSON],
    [basedOn.id],
    [basedOn.extension],
    [basedOn.reference],
    [basedOn.type],
    [basedOn.identifier.id],
    [basedOn.identifier.extension],
    [basedOn.identifier.use],
    [basedOn.identifier.type],
    [basedOn.identifier.system],
    [basedOn.identifier.value],
    [basedOn.identifier.period],
    [basedOn.identifier.assigner],
    [basedOn.display]
FROM openrowset (
        BULK 'CarePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [basedOn.JSON]  VARCHAR(MAX) '$.basedOn'
    ) AS rowset
    CROSS APPLY openjson (rowset.[basedOn.JSON]) with (
        [basedOn.id]                   NVARCHAR(100)       '$.id',
        [basedOn.extension]            NVARCHAR(MAX)       '$.extension',
        [basedOn.reference]            NVARCHAR(4000)      '$.reference',
        [basedOn.type]                 VARCHAR(256)        '$.type',
        [basedOn.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [basedOn.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [basedOn.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [basedOn.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [basedOn.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [basedOn.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [basedOn.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [basedOn.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [basedOn.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CarePlanReplaces AS
SELECT
    [id],
    [replaces.JSON],
    [replaces.id],
    [replaces.extension],
    [replaces.reference],
    [replaces.type],
    [replaces.identifier.id],
    [replaces.identifier.extension],
    [replaces.identifier.use],
    [replaces.identifier.type],
    [replaces.identifier.system],
    [replaces.identifier.value],
    [replaces.identifier.period],
    [replaces.identifier.assigner],
    [replaces.display]
FROM openrowset (
        BULK 'CarePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [replaces.JSON]  VARCHAR(MAX) '$.replaces'
    ) AS rowset
    CROSS APPLY openjson (rowset.[replaces.JSON]) with (
        [replaces.id]                  NVARCHAR(100)       '$.id',
        [replaces.extension]           NVARCHAR(MAX)       '$.extension',
        [replaces.reference]           NVARCHAR(4000)      '$.reference',
        [replaces.type]                VARCHAR(256)        '$.type',
        [replaces.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [replaces.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [replaces.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [replaces.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [replaces.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [replaces.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [replaces.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [replaces.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [replaces.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CarePlanPartOf AS
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
        BULK 'CarePlan/**',
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

CREATE VIEW fhir.CarePlanCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'CarePlan/**',
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

CREATE VIEW fhir.CarePlanContributor AS
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
        BULK 'CarePlan/**',
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

CREATE VIEW fhir.CarePlanCareTeam AS
SELECT
    [id],
    [careTeam.JSON],
    [careTeam.id],
    [careTeam.extension],
    [careTeam.reference],
    [careTeam.type],
    [careTeam.identifier.id],
    [careTeam.identifier.extension],
    [careTeam.identifier.use],
    [careTeam.identifier.type],
    [careTeam.identifier.system],
    [careTeam.identifier.value],
    [careTeam.identifier.period],
    [careTeam.identifier.assigner],
    [careTeam.display]
FROM openrowset (
        BULK 'CarePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [careTeam.JSON]  VARCHAR(MAX) '$.careTeam'
    ) AS rowset
    CROSS APPLY openjson (rowset.[careTeam.JSON]) with (
        [careTeam.id]                  NVARCHAR(100)       '$.id',
        [careTeam.extension]           NVARCHAR(MAX)       '$.extension',
        [careTeam.reference]           NVARCHAR(4000)      '$.reference',
        [careTeam.type]                VARCHAR(256)        '$.type',
        [careTeam.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [careTeam.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [careTeam.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [careTeam.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [careTeam.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [careTeam.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [careTeam.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [careTeam.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [careTeam.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CarePlanAddresses AS
SELECT
    [id],
    [addresses.JSON],
    [addresses.id],
    [addresses.extension],
    [addresses.reference],
    [addresses.type],
    [addresses.identifier.id],
    [addresses.identifier.extension],
    [addresses.identifier.use],
    [addresses.identifier.type],
    [addresses.identifier.system],
    [addresses.identifier.value],
    [addresses.identifier.period],
    [addresses.identifier.assigner],
    [addresses.display]
FROM openrowset (
        BULK 'CarePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [addresses.JSON]  VARCHAR(MAX) '$.addresses'
    ) AS rowset
    CROSS APPLY openjson (rowset.[addresses.JSON]) with (
        [addresses.id]                 NVARCHAR(100)       '$.id',
        [addresses.extension]          NVARCHAR(MAX)       '$.extension',
        [addresses.reference]          NVARCHAR(4000)      '$.reference',
        [addresses.type]               VARCHAR(256)        '$.type',
        [addresses.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [addresses.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [addresses.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [addresses.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [addresses.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [addresses.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [addresses.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [addresses.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [addresses.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CarePlanSupportingInfo AS
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
        BULK 'CarePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [supportingInfo.JSON]  VARCHAR(MAX) '$.supportingInfo'
    ) AS rowset
    CROSS APPLY openjson (rowset.[supportingInfo.JSON]) with (
        [supportingInfo.id]            NVARCHAR(100)       '$.id',
        [supportingInfo.extension]     NVARCHAR(MAX)       '$.extension',
        [supportingInfo.reference]     NVARCHAR(4000)      '$.reference',
        [supportingInfo.type]          VARCHAR(256)        '$.type',
        [supportingInfo.identifier.id] NVARCHAR(100)       '$.identifier.id',
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

CREATE VIEW fhir.CarePlanGoal AS
SELECT
    [id],
    [goal.JSON],
    [goal.id],
    [goal.extension],
    [goal.reference],
    [goal.type],
    [goal.identifier.id],
    [goal.identifier.extension],
    [goal.identifier.use],
    [goal.identifier.type],
    [goal.identifier.system],
    [goal.identifier.value],
    [goal.identifier.period],
    [goal.identifier.assigner],
    [goal.display]
FROM openrowset (
        BULK 'CarePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [goal.JSON]  VARCHAR(MAX) '$.goal'
    ) AS rowset
    CROSS APPLY openjson (rowset.[goal.JSON]) with (
        [goal.id]                      NVARCHAR(100)       '$.id',
        [goal.extension]               NVARCHAR(MAX)       '$.extension',
        [goal.reference]               NVARCHAR(4000)      '$.reference',
        [goal.type]                    VARCHAR(256)        '$.type',
        [goal.identifier.id]           NVARCHAR(100)       '$.identifier.id',
        [goal.identifier.extension]    NVARCHAR(MAX)       '$.identifier.extension',
        [goal.identifier.use]          NVARCHAR(64)        '$.identifier.use',
        [goal.identifier.type]         NVARCHAR(MAX)       '$.identifier.type',
        [goal.identifier.system]       VARCHAR(256)        '$.identifier.system',
        [goal.identifier.value]        NVARCHAR(4000)      '$.identifier.value',
        [goal.identifier.period]       NVARCHAR(MAX)       '$.identifier.period',
        [goal.identifier.assigner]     NVARCHAR(MAX)       '$.identifier.assigner',
        [goal.display]                 NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CarePlanActivity AS
SELECT
    [id],
    [activity.JSON],
    [activity.id],
    [activity.extension],
    [activity.modifierExtension],
    [activity.outcomeCodeableConcept],
    [activity.outcomeReference],
    [activity.progress],
    [activity.reference.id],
    [activity.reference.extension],
    [activity.reference.reference],
    [activity.reference.type],
    [activity.reference.identifier],
    [activity.reference.display],
    [activity.detail.id],
    [activity.detail.extension],
    [activity.detail.modifierExtension],
    [activity.detail.kind],
    [activity.detail.instantiatesCanonical],
    [activity.detail.instantiatesUri],
    [activity.detail.code],
    [activity.detail.reasonCode],
    [activity.detail.reasonReference],
    [activity.detail.goal],
    [activity.detail.status],
    [activity.detail.statusReason],
    [activity.detail.doNotPerform],
    [activity.detail.location],
    [activity.detail.performer],
    [activity.detail.dailyAmount],
    [activity.detail.quantity],
    [activity.detail.description],
    [activity.detail.scheduled.timing],
    [activity.detail.scheduled.period],
    [activity.detail.scheduled.string],
    [activity.detail.product.codeableConcept],
    [activity.detail.product.reference]
FROM openrowset (
        BULK 'CarePlan/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [activity.JSON]  VARCHAR(MAX) '$.activity'
    ) AS rowset
    CROSS APPLY openjson (rowset.[activity.JSON]) with (
        [activity.id]                  NVARCHAR(100)       '$.id',
        [activity.extension]           NVARCHAR(MAX)       '$.extension',
        [activity.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [activity.outcomeCodeableConcept] NVARCHAR(MAX)       '$.outcomeCodeableConcept' AS JSON,
        [activity.outcomeReference]    NVARCHAR(MAX)       '$.outcomeReference' AS JSON,
        [activity.progress]            NVARCHAR(MAX)       '$.progress' AS JSON,
        [activity.reference.id]        NVARCHAR(100)       '$.reference.id',
        [activity.reference.extension] NVARCHAR(MAX)       '$.reference.extension',
        [activity.reference.reference] NVARCHAR(4000)      '$.reference.reference',
        [activity.reference.type]      VARCHAR(256)        '$.reference.type',
        [activity.reference.identifier] NVARCHAR(MAX)       '$.reference.identifier',
        [activity.reference.display]   NVARCHAR(4000)      '$.reference.display',
        [activity.detail.id]           NVARCHAR(100)       '$.detail.id',
        [activity.detail.extension]    NVARCHAR(MAX)       '$.detail.extension',
        [activity.detail.modifierExtension] NVARCHAR(MAX)       '$.detail.modifierExtension',
        [activity.detail.kind]         NVARCHAR(100)       '$.detail.kind',
        [activity.detail.instantiatesCanonical] NVARCHAR(MAX)       '$.detail.instantiatesCanonical',
        [activity.detail.instantiatesUri] NVARCHAR(MAX)       '$.detail.instantiatesUri',
        [activity.detail.code]         NVARCHAR(MAX)       '$.detail.code',
        [activity.detail.reasonCode]   NVARCHAR(MAX)       '$.detail.reasonCode',
        [activity.detail.reasonReference] NVARCHAR(MAX)       '$.detail.reasonReference',
        [activity.detail.goal]         NVARCHAR(MAX)       '$.detail.goal',
        [activity.detail.status]       NVARCHAR(64)        '$.detail.status',
        [activity.detail.statusReason] NVARCHAR(MAX)       '$.detail.statusReason',
        [activity.detail.doNotPerform] bit                 '$.detail.doNotPerform',
        [activity.detail.location]     NVARCHAR(MAX)       '$.detail.location',
        [activity.detail.performer]    NVARCHAR(MAX)       '$.detail.performer',
        [activity.detail.dailyAmount]  NVARCHAR(MAX)       '$.detail.dailyAmount',
        [activity.detail.quantity]     NVARCHAR(MAX)       '$.detail.quantity',
        [activity.detail.description]  NVARCHAR(4000)      '$.detail.description',
        [activity.detail.scheduled.timing] NVARCHAR(MAX)       '$.detail.scheduled.timing',
        [activity.detail.scheduled.period] NVARCHAR(MAX)       '$.detail.scheduled.period',
        [activity.detail.scheduled.string] NVARCHAR(4000)      '$.detail.scheduled.string',
        [activity.detail.product.codeableConcept] NVARCHAR(MAX)       '$.detail.product.codeableConcept',
        [activity.detail.product.reference] NVARCHAR(MAX)       '$.detail.product.reference'
    ) j

GO

CREATE VIEW fhir.CarePlanNote AS
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
        BULK 'CarePlan/**',
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
