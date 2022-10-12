CREATE EXTERNAL TABLE [fhir].[CareTeam] (
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
    [status] NVARCHAR(100),
    [category] VARCHAR(MAX),
    [name] NVARCHAR(500),
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
    [period.id] NVARCHAR(100),
    [period.extension] NVARCHAR(MAX),
    [period.start] VARCHAR(64),
    [period.end] VARCHAR(64),
    [participant] VARCHAR(MAX),
    [reason] VARCHAR(MAX),
    [managingOrganization] VARCHAR(MAX),
    [telecom] VARCHAR(MAX),
    [note] VARCHAR(MAX),
) WITH (
    LOCATION='/CareTeam/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.CareTeamIdentifier AS
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
        BULK 'CareTeam/**',
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

CREATE VIEW fhir.CareTeamCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'CareTeam/**',
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

CREATE VIEW fhir.CareTeamParticipant AS
SELECT
    [id],
    [participant.JSON],
    [participant.id],
    [participant.extension],
    [participant.modifierExtension],
    [participant.role.id],
    [participant.role.extension],
    [participant.role.coding],
    [participant.role.text],
    [participant.member.id],
    [participant.member.extension],
    [participant.member.reference],
    [participant.member.type],
    [participant.member.identifier],
    [participant.member.display],
    [participant.onBehalfOf.id],
    [participant.onBehalfOf.extension],
    [participant.onBehalfOf.reference],
    [participant.onBehalfOf.type],
    [participant.onBehalfOf.identifier],
    [participant.onBehalfOf.display],
    [participant.coverage.period.id],
    [participant.coverage.period.extension],
    [participant.coverage.period.start],
    [participant.coverage.period.end],
    [participant.coverage.timing.id],
    [participant.coverage.timing.extension],
    [participant.coverage.timing.modifierExtension],
    [participant.coverage.timing.event],
    [participant.coverage.timing.repeat],
    [participant.coverage.timing.code]
FROM openrowset (
        BULK 'CareTeam/**',
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
        [participant.role.id]          NVARCHAR(100)       '$.role.id',
        [participant.role.extension]   NVARCHAR(MAX)       '$.role.extension',
        [participant.role.coding]      NVARCHAR(MAX)       '$.role.coding',
        [participant.role.text]        NVARCHAR(4000)      '$.role.text',
        [participant.member.id]        NVARCHAR(100)       '$.member.id',
        [participant.member.extension] NVARCHAR(MAX)       '$.member.extension',
        [participant.member.reference] NVARCHAR(4000)      '$.member.reference',
        [participant.member.type]      VARCHAR(256)        '$.member.type',
        [participant.member.identifier] NVARCHAR(MAX)       '$.member.identifier',
        [participant.member.display]   NVARCHAR(4000)      '$.member.display',
        [participant.onBehalfOf.id]    NVARCHAR(100)       '$.onBehalfOf.id',
        [participant.onBehalfOf.extension] NVARCHAR(MAX)       '$.onBehalfOf.extension',
        [participant.onBehalfOf.reference] NVARCHAR(4000)      '$.onBehalfOf.reference',
        [participant.onBehalfOf.type]  VARCHAR(256)        '$.onBehalfOf.type',
        [participant.onBehalfOf.identifier] NVARCHAR(MAX)       '$.onBehalfOf.identifier',
        [participant.onBehalfOf.display] NVARCHAR(4000)      '$.onBehalfOf.display',
        [participant.coverage.period.id] NVARCHAR(100)       '$.coverage.period.id',
        [participant.coverage.period.extension] NVARCHAR(MAX)       '$.coverage.period.extension',
        [participant.coverage.period.start] VARCHAR(64)         '$.coverage.period.start',
        [participant.coverage.period.end] VARCHAR(64)         '$.coverage.period.end',
        [participant.coverage.timing.id] NVARCHAR(100)       '$.coverage.timing.id',
        [participant.coverage.timing.extension] NVARCHAR(MAX)       '$.coverage.timing.extension',
        [participant.coverage.timing.modifierExtension] NVARCHAR(MAX)       '$.coverage.timing.modifierExtension',
        [participant.coverage.timing.event] NVARCHAR(MAX)       '$.coverage.timing.event',
        [participant.coverage.timing.repeat] NVARCHAR(MAX)       '$.coverage.timing.repeat',
        [participant.coverage.timing.code] NVARCHAR(MAX)       '$.coverage.timing.code'
    ) j

GO

CREATE VIEW fhir.CareTeamReason AS
SELECT
    [id],
    [reason.JSON],
    [reason.id],
    [reason.extension],
    [reason.concept.id],
    [reason.concept.extension],
    [reason.concept.coding],
    [reason.concept.text],
    [reason.reference.id],
    [reason.reference.extension],
    [reason.reference.reference],
    [reason.reference.type],
    [reason.reference.identifier],
    [reason.reference.display]
FROM openrowset (
        BULK 'CareTeam/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reason.JSON]  VARCHAR(MAX) '$.reason'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reason.JSON]) with (
        [reason.id]                    NVARCHAR(100)       '$.id',
        [reason.extension]             NVARCHAR(MAX)       '$.extension',
        [reason.concept.id]            NVARCHAR(100)       '$.concept.id',
        [reason.concept.extension]     NVARCHAR(MAX)       '$.concept.extension',
        [reason.concept.coding]        NVARCHAR(MAX)       '$.concept.coding',
        [reason.concept.text]          NVARCHAR(4000)      '$.concept.text',
        [reason.reference.id]          NVARCHAR(100)       '$.reference.id',
        [reason.reference.extension]   NVARCHAR(MAX)       '$.reference.extension',
        [reason.reference.reference]   NVARCHAR(4000)      '$.reference.reference',
        [reason.reference.type]        VARCHAR(256)        '$.reference.type',
        [reason.reference.identifier]  NVARCHAR(MAX)       '$.reference.identifier',
        [reason.reference.display]     NVARCHAR(4000)      '$.reference.display'
    ) j

GO

CREATE VIEW fhir.CareTeamManagingOrganization AS
SELECT
    [id],
    [managingOrganization.JSON],
    [managingOrganization.id],
    [managingOrganization.extension],
    [managingOrganization.reference],
    [managingOrganization.type],
    [managingOrganization.identifier.id],
    [managingOrganization.identifier.extension],
    [managingOrganization.identifier.use],
    [managingOrganization.identifier.type],
    [managingOrganization.identifier.system],
    [managingOrganization.identifier.value],
    [managingOrganization.identifier.period],
    [managingOrganization.identifier.assigner],
    [managingOrganization.display]
FROM openrowset (
        BULK 'CareTeam/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [managingOrganization.JSON]  VARCHAR(MAX) '$.managingOrganization'
    ) AS rowset
    CROSS APPLY openjson (rowset.[managingOrganization.JSON]) with (
        [managingOrganization.id]      NVARCHAR(100)       '$.id',
        [managingOrganization.extension] NVARCHAR(MAX)       '$.extension',
        [managingOrganization.reference] NVARCHAR(4000)      '$.reference',
        [managingOrganization.type]    VARCHAR(256)        '$.type',
        [managingOrganization.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [managingOrganization.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [managingOrganization.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [managingOrganization.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [managingOrganization.identifier.system] VARCHAR(256)        '$.identifier.system',
        [managingOrganization.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [managingOrganization.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [managingOrganization.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [managingOrganization.display] NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CareTeamTelecom AS
SELECT
    [id],
    [telecom.JSON],
    [telecom.id],
    [telecom.extension],
    [telecom.system],
    [telecom.value],
    [telecom.use],
    [telecom.rank],
    [telecom.period.id],
    [telecom.period.extension],
    [telecom.period.start],
    [telecom.period.end]
FROM openrowset (
        BULK 'CareTeam/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [telecom.JSON]  VARCHAR(MAX) '$.telecom'
    ) AS rowset
    CROSS APPLY openjson (rowset.[telecom.JSON]) with (
        [telecom.id]                   NVARCHAR(100)       '$.id',
        [telecom.extension]            NVARCHAR(MAX)       '$.extension',
        [telecom.system]               NVARCHAR(64)        '$.system',
        [telecom.value]                NVARCHAR(4000)      '$.value',
        [telecom.use]                  NVARCHAR(64)        '$.use',
        [telecom.rank]                 bigint              '$.rank',
        [telecom.period.id]            NVARCHAR(100)       '$.period.id',
        [telecom.period.extension]     NVARCHAR(MAX)       '$.period.extension',
        [telecom.period.start]         VARCHAR(64)         '$.period.start',
        [telecom.period.end]           VARCHAR(64)         '$.period.end'
    ) j

GO

CREATE VIEW fhir.CareTeamNote AS
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
        BULK 'CareTeam/**',
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
