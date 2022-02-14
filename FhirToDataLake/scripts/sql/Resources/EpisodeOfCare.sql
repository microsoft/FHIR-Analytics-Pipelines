CREATE EXTERNAL TABLE [fhir].[EpisodeOfCare] (
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
    [status] NVARCHAR(64),
    [statusHistory] VARCHAR(MAX),
    [type] VARCHAR(MAX),
    [diagnosis] VARCHAR(MAX),
    [patient.id] NVARCHAR(100),
    [patient.extension] NVARCHAR(MAX),
    [patient.reference] NVARCHAR(4000),
    [patient.type] VARCHAR(256),
    [patient.identifier.id] NVARCHAR(100),
    [patient.identifier.extension] NVARCHAR(MAX),
    [patient.identifier.use] NVARCHAR(64),
    [patient.identifier.type] NVARCHAR(MAX),
    [patient.identifier.system] VARCHAR(256),
    [patient.identifier.value] NVARCHAR(4000),
    [patient.identifier.period] NVARCHAR(MAX),
    [patient.identifier.assigner] NVARCHAR(MAX),
    [patient.display] NVARCHAR(4000),
    [managingOrganization.id] NVARCHAR(100),
    [managingOrganization.extension] NVARCHAR(MAX),
    [managingOrganization.reference] NVARCHAR(4000),
    [managingOrganization.type] VARCHAR(256),
    [managingOrganization.identifier.id] NVARCHAR(100),
    [managingOrganization.identifier.extension] NVARCHAR(MAX),
    [managingOrganization.identifier.use] NVARCHAR(64),
    [managingOrganization.identifier.type] NVARCHAR(MAX),
    [managingOrganization.identifier.system] VARCHAR(256),
    [managingOrganization.identifier.value] NVARCHAR(4000),
    [managingOrganization.identifier.period] NVARCHAR(MAX),
    [managingOrganization.identifier.assigner] NVARCHAR(MAX),
    [managingOrganization.display] NVARCHAR(4000),
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [referralRequest] VARCHAR(MAX),
    [careManager.id] NVARCHAR(100),
    [careManager.extension] NVARCHAR(MAX),
    [careManager.reference] NVARCHAR(4000),
    [careManager.type] VARCHAR(256),
    [careManager.identifier.id] NVARCHAR(100),
    [careManager.identifier.extension] NVARCHAR(MAX),
    [careManager.identifier.use] NVARCHAR(64),
    [careManager.identifier.type] NVARCHAR(MAX),
    [careManager.identifier.system] VARCHAR(256),
    [careManager.identifier.value] NVARCHAR(4000),
    [careManager.identifier.period] NVARCHAR(MAX),
    [careManager.identifier.assigner] NVARCHAR(MAX),
    [careManager.display] NVARCHAR(4000),
    [team] VARCHAR(MAX),
    [account] VARCHAR(MAX),
) WITH (
    LOCATION='/EpisodeOfCare/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.EpisodeOfCareIdentifier AS
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
        BULK 'EpisodeOfCare/**',
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

CREATE VIEW fhir.EpisodeOfCareStatusHistory AS
SELECT
    [id],
    [statusHistory.JSON],
    [statusHistory.id],
    [statusHistory.extension],
    [statusHistory.modifierExtension],
    [statusHistory.status],
    [statusHistory.period.id],
    [statusHistory.period.extension],
    [statusHistory.period.start],
    [statusHistory.period.end]
FROM openrowset (
        BULK 'EpisodeOfCare/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [statusHistory.JSON]  VARCHAR(MAX) '$.statusHistory'
    ) AS rowset
    CROSS APPLY openjson (rowset.[statusHistory.JSON]) with (
        [statusHistory.id]             NVARCHAR(100)       '$.id',
        [statusHistory.extension]      NVARCHAR(MAX)       '$.extension',
        [statusHistory.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [statusHistory.status]         NVARCHAR(64)        '$.status',
        [statusHistory.period.id]      NVARCHAR(100)       '$.period.id',
        [statusHistory.period.extension] NVARCHAR(MAX)       '$.period.extension',
        [statusHistory.period.start]   VARCHAR(64)         '$.period.start',
        [statusHistory.period.end]     VARCHAR(64)         '$.period.end'
    ) j

GO

CREATE VIEW fhir.EpisodeOfCareType AS
SELECT
    [id],
    [type.JSON],
    [type.id],
    [type.extension],
    [type.coding],
    [type.text]
FROM openrowset (
        BULK 'EpisodeOfCare/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [type.JSON]  VARCHAR(MAX) '$.type'
    ) AS rowset
    CROSS APPLY openjson (rowset.[type.JSON]) with (
        [type.id]                      NVARCHAR(100)       '$.id',
        [type.extension]               NVARCHAR(MAX)       '$.extension',
        [type.coding]                  NVARCHAR(MAX)       '$.coding' AS JSON,
        [type.text]                    NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.EpisodeOfCareDiagnosis AS
SELECT
    [id],
    [diagnosis.JSON],
    [diagnosis.id],
    [diagnosis.extension],
    [diagnosis.modifierExtension],
    [diagnosis.condition.id],
    [diagnosis.condition.extension],
    [diagnosis.condition.reference],
    [diagnosis.condition.type],
    [diagnosis.condition.identifier],
    [diagnosis.condition.display],
    [diagnosis.role.id],
    [diagnosis.role.extension],
    [diagnosis.role.coding],
    [diagnosis.role.text],
    [diagnosis.rank]
FROM openrowset (
        BULK 'EpisodeOfCare/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [diagnosis.JSON]  VARCHAR(MAX) '$.diagnosis'
    ) AS rowset
    CROSS APPLY openjson (rowset.[diagnosis.JSON]) with (
        [diagnosis.id]                 NVARCHAR(100)       '$.id',
        [diagnosis.extension]          NVARCHAR(MAX)       '$.extension',
        [diagnosis.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [diagnosis.condition.id]       NVARCHAR(100)       '$.condition.id',
        [diagnosis.condition.extension] NVARCHAR(MAX)       '$.condition.extension',
        [diagnosis.condition.reference] NVARCHAR(4000)      '$.condition.reference',
        [diagnosis.condition.type]     VARCHAR(256)        '$.condition.type',
        [diagnosis.condition.identifier] NVARCHAR(MAX)       '$.condition.identifier',
        [diagnosis.condition.display]  NVARCHAR(4000)      '$.condition.display',
        [diagnosis.role.id]            NVARCHAR(100)       '$.role.id',
        [diagnosis.role.extension]     NVARCHAR(MAX)       '$.role.extension',
        [diagnosis.role.coding]        NVARCHAR(MAX)       '$.role.coding',
        [diagnosis.role.text]          NVARCHAR(4000)      '$.role.text',
        [diagnosis.rank]               bigint              '$.rank'
    ) j

GO

CREATE VIEW fhir.EpisodeOfCareReferralRequest AS
SELECT
    [id],
    [referralRequest.JSON],
    [referralRequest.id],
    [referralRequest.extension],
    [referralRequest.reference],
    [referralRequest.type],
    [referralRequest.identifier.id],
    [referralRequest.identifier.extension],
    [referralRequest.identifier.use],
    [referralRequest.identifier.type],
    [referralRequest.identifier.system],
    [referralRequest.identifier.value],
    [referralRequest.identifier.period],
    [referralRequest.identifier.assigner],
    [referralRequest.display]
FROM openrowset (
        BULK 'EpisodeOfCare/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [referralRequest.JSON]  VARCHAR(MAX) '$.referralRequest'
    ) AS rowset
    CROSS APPLY openjson (rowset.[referralRequest.JSON]) with (
        [referralRequest.id]           NVARCHAR(100)       '$.id',
        [referralRequest.extension]    NVARCHAR(MAX)       '$.extension',
        [referralRequest.reference]    NVARCHAR(4000)      '$.reference',
        [referralRequest.type]         VARCHAR(256)        '$.type',
        [referralRequest.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [referralRequest.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [referralRequest.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [referralRequest.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [referralRequest.identifier.system] VARCHAR(256)        '$.identifier.system',
        [referralRequest.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [referralRequest.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [referralRequest.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [referralRequest.display]      NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.EpisodeOfCareTeam AS
SELECT
    [id],
    [team.JSON],
    [team.id],
    [team.extension],
    [team.reference],
    [team.type],
    [team.identifier.id],
    [team.identifier.extension],
    [team.identifier.use],
    [team.identifier.type],
    [team.identifier.system],
    [team.identifier.value],
    [team.identifier.period],
    [team.identifier.assigner],
    [team.display]
FROM openrowset (
        BULK 'EpisodeOfCare/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [team.JSON]  VARCHAR(MAX) '$.team'
    ) AS rowset
    CROSS APPLY openjson (rowset.[team.JSON]) with (
        [team.id]                      NVARCHAR(100)       '$.id',
        [team.extension]               NVARCHAR(MAX)       '$.extension',
        [team.reference]               NVARCHAR(4000)      '$.reference',
        [team.type]                    VARCHAR(256)        '$.type',
        [team.identifier.id]           NVARCHAR(100)       '$.identifier.id',
        [team.identifier.extension]    NVARCHAR(MAX)       '$.identifier.extension',
        [team.identifier.use]          NVARCHAR(64)        '$.identifier.use',
        [team.identifier.type]         NVARCHAR(MAX)       '$.identifier.type',
        [team.identifier.system]       VARCHAR(256)        '$.identifier.system',
        [team.identifier.value]        NVARCHAR(4000)      '$.identifier.value',
        [team.identifier.period]       NVARCHAR(MAX)       '$.identifier.period',
        [team.identifier.assigner]     NVARCHAR(MAX)       '$.identifier.assigner',
        [team.display]                 NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.EpisodeOfCareAccount AS
SELECT
    [id],
    [account.JSON],
    [account.id],
    [account.extension],
    [account.reference],
    [account.type],
    [account.identifier.id],
    [account.identifier.extension],
    [account.identifier.use],
    [account.identifier.type],
    [account.identifier.system],
    [account.identifier.value],
    [account.identifier.period],
    [account.identifier.assigner],
    [account.display]
FROM openrowset (
        BULK 'EpisodeOfCare/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [account.JSON]  VARCHAR(MAX) '$.account'
    ) AS rowset
    CROSS APPLY openjson (rowset.[account.JSON]) with (
        [account.id]                   NVARCHAR(100)       '$.id',
        [account.extension]            NVARCHAR(MAX)       '$.extension',
        [account.reference]            NVARCHAR(4000)      '$.reference',
        [account.type]                 VARCHAR(256)        '$.type',
        [account.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [account.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [account.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [account.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [account.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [account.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [account.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [account.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [account.display]              NVARCHAR(4000)      '$.display'
    ) j
