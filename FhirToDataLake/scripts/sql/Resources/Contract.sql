CREATE EXTERNAL TABLE [fhir].[Contract] (
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
    [url] VARCHAR(256),
    [version] NVARCHAR(100),
    [status] NVARCHAR(100),
    [legalState.id] NVARCHAR(100),
    [legalState.extension] NVARCHAR(MAX),
    [legalState.coding] VARCHAR(MAX),
    [legalState.text] NVARCHAR(4000),
    [instantiatesCanonical.id] NVARCHAR(100),
    [instantiatesCanonical.extension] NVARCHAR(MAX),
    [instantiatesCanonical.reference] NVARCHAR(4000),
    [instantiatesCanonical.type] VARCHAR(256),
    [instantiatesCanonical.identifier.id] NVARCHAR(100),
    [instantiatesCanonical.identifier.extension] NVARCHAR(MAX),
    [instantiatesCanonical.identifier.use] NVARCHAR(64),
    [instantiatesCanonical.identifier.type] NVARCHAR(MAX),
    [instantiatesCanonical.identifier.system] VARCHAR(256),
    [instantiatesCanonical.identifier.value] NVARCHAR(4000),
    [instantiatesCanonical.identifier.period] NVARCHAR(MAX),
    [instantiatesCanonical.identifier.assigner] NVARCHAR(MAX),
    [instantiatesCanonical.display] NVARCHAR(4000),
    [instantiatesUri] VARCHAR(256),
    [contentDerivative.id] NVARCHAR(100),
    [contentDerivative.extension] NVARCHAR(MAX),
    [contentDerivative.coding] VARCHAR(MAX),
    [contentDerivative.text] NVARCHAR(4000),
    [issued] VARCHAR(64),
    [applies.id] NVARCHAR(100),
    [applies.extension] NVARCHAR(MAX),
    [applies.start] VARCHAR(64),
    [applies.end] VARCHAR(64),
    [expirationType.id] NVARCHAR(100),
    [expirationType.extension] NVARCHAR(MAX),
    [expirationType.coding] VARCHAR(MAX),
    [expirationType.text] NVARCHAR(4000),
    [subject] VARCHAR(MAX),
    [authority] VARCHAR(MAX),
    [domain] VARCHAR(MAX),
    [site] VARCHAR(MAX),
    [name] NVARCHAR(500),
    [title] NVARCHAR(4000),
    [subtitle] NVARCHAR(4000),
    [alias] VARCHAR(MAX),
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
    [scope.id] NVARCHAR(100),
    [scope.extension] NVARCHAR(MAX),
    [scope.coding] VARCHAR(MAX),
    [scope.text] NVARCHAR(4000),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [subType] VARCHAR(MAX),
    [contentDefinition.id] NVARCHAR(100),
    [contentDefinition.extension] NVARCHAR(MAX),
    [contentDefinition.modifierExtension] NVARCHAR(MAX),
    [contentDefinition.type.id] NVARCHAR(100),
    [contentDefinition.type.extension] NVARCHAR(MAX),
    [contentDefinition.type.coding] NVARCHAR(MAX),
    [contentDefinition.type.text] NVARCHAR(4000),
    [contentDefinition.subType.id] NVARCHAR(100),
    [contentDefinition.subType.extension] NVARCHAR(MAX),
    [contentDefinition.subType.coding] NVARCHAR(MAX),
    [contentDefinition.subType.text] NVARCHAR(4000),
    [contentDefinition.publisher.id] NVARCHAR(100),
    [contentDefinition.publisher.extension] NVARCHAR(MAX),
    [contentDefinition.publisher.reference] NVARCHAR(4000),
    [contentDefinition.publisher.type] VARCHAR(256),
    [contentDefinition.publisher.identifier] NVARCHAR(MAX),
    [contentDefinition.publisher.display] NVARCHAR(4000),
    [contentDefinition.publicationDate] VARCHAR(64),
    [contentDefinition.publicationStatus] NVARCHAR(100),
    [contentDefinition.copyright] NVARCHAR(MAX),
    [term] VARCHAR(MAX),
    [supportingInfo] VARCHAR(MAX),
    [relevantHistory] VARCHAR(MAX),
    [signer] VARCHAR(MAX),
    [friendly] VARCHAR(MAX),
    [legal] VARCHAR(MAX),
    [rule] VARCHAR(MAX),
    [topic.codeableConcept.id] NVARCHAR(100),
    [topic.codeableConcept.extension] NVARCHAR(MAX),
    [topic.codeableConcept.coding] VARCHAR(MAX),
    [topic.codeableConcept.text] NVARCHAR(4000),
    [topic.reference.id] NVARCHAR(100),
    [topic.reference.extension] NVARCHAR(MAX),
    [topic.reference.reference] NVARCHAR(4000),
    [topic.reference.type] VARCHAR(256),
    [topic.reference.identifier.id] NVARCHAR(100),
    [topic.reference.identifier.extension] NVARCHAR(MAX),
    [topic.reference.identifier.use] NVARCHAR(64),
    [topic.reference.identifier.type] NVARCHAR(MAX),
    [topic.reference.identifier.system] VARCHAR(256),
    [topic.reference.identifier.value] NVARCHAR(4000),
    [topic.reference.identifier.period] NVARCHAR(MAX),
    [topic.reference.identifier.assigner] NVARCHAR(MAX),
    [topic.reference.display] NVARCHAR(4000),
    [legallyBinding.attachment.id] NVARCHAR(100),
    [legallyBinding.attachment.extension] NVARCHAR(MAX),
    [legallyBinding.attachment.contentType] NVARCHAR(100),
    [legallyBinding.attachment.language] NVARCHAR(100),
    [legallyBinding.attachment.data] NVARCHAR(MAX),
    [legallyBinding.attachment.url] VARCHAR(256),
    [legallyBinding.attachment.size] bigint,
    [legallyBinding.attachment.hash] NVARCHAR(MAX),
    [legallyBinding.attachment.title] NVARCHAR(4000),
    [legallyBinding.attachment.creation] VARCHAR(64),
    [legallyBinding.reference.id] NVARCHAR(100),
    [legallyBinding.reference.extension] NVARCHAR(MAX),
    [legallyBinding.reference.reference] NVARCHAR(4000),
    [legallyBinding.reference.type] VARCHAR(256),
    [legallyBinding.reference.identifier.id] NVARCHAR(100),
    [legallyBinding.reference.identifier.extension] NVARCHAR(MAX),
    [legallyBinding.reference.identifier.use] NVARCHAR(64),
    [legallyBinding.reference.identifier.type] NVARCHAR(MAX),
    [legallyBinding.reference.identifier.system] VARCHAR(256),
    [legallyBinding.reference.identifier.value] NVARCHAR(4000),
    [legallyBinding.reference.identifier.period] NVARCHAR(MAX),
    [legallyBinding.reference.identifier.assigner] NVARCHAR(MAX),
    [legallyBinding.reference.display] NVARCHAR(4000),
) WITH (
    LOCATION='/Contract/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ContractIdentifier AS
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
        BULK 'Contract/**',
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

CREATE VIEW fhir.ContractSubject AS
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
        BULK 'Contract/**',
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

CREATE VIEW fhir.ContractAuthority AS
SELECT
    [id],
    [authority.JSON],
    [authority.id],
    [authority.extension],
    [authority.reference],
    [authority.type],
    [authority.identifier.id],
    [authority.identifier.extension],
    [authority.identifier.use],
    [authority.identifier.type],
    [authority.identifier.system],
    [authority.identifier.value],
    [authority.identifier.period],
    [authority.identifier.assigner],
    [authority.display]
FROM openrowset (
        BULK 'Contract/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [authority.JSON]  VARCHAR(MAX) '$.authority'
    ) AS rowset
    CROSS APPLY openjson (rowset.[authority.JSON]) with (
        [authority.id]                 NVARCHAR(100)       '$.id',
        [authority.extension]          NVARCHAR(MAX)       '$.extension',
        [authority.reference]          NVARCHAR(4000)      '$.reference',
        [authority.type]               VARCHAR(256)        '$.type',
        [authority.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [authority.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [authority.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [authority.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [authority.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [authority.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [authority.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [authority.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [authority.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ContractDomain AS
SELECT
    [id],
    [domain.JSON],
    [domain.id],
    [domain.extension],
    [domain.reference],
    [domain.type],
    [domain.identifier.id],
    [domain.identifier.extension],
    [domain.identifier.use],
    [domain.identifier.type],
    [domain.identifier.system],
    [domain.identifier.value],
    [domain.identifier.period],
    [domain.identifier.assigner],
    [domain.display]
FROM openrowset (
        BULK 'Contract/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [domain.JSON]  VARCHAR(MAX) '$.domain'
    ) AS rowset
    CROSS APPLY openjson (rowset.[domain.JSON]) with (
        [domain.id]                    NVARCHAR(100)       '$.id',
        [domain.extension]             NVARCHAR(MAX)       '$.extension',
        [domain.reference]             NVARCHAR(4000)      '$.reference',
        [domain.type]                  VARCHAR(256)        '$.type',
        [domain.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [domain.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [domain.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [domain.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [domain.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [domain.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [domain.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [domain.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [domain.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ContractSite AS
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
        BULK 'Contract/**',
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

CREATE VIEW fhir.ContractAlias AS
SELECT
    [id],
    [alias.JSON],
    [alias]
FROM openrowset (
        BULK 'Contract/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [alias.JSON]  VARCHAR(MAX) '$.alias'
    ) AS rowset
    CROSS APPLY openjson (rowset.[alias.JSON]) with (
        [alias]                        NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ContractSubType AS
SELECT
    [id],
    [subType.JSON],
    [subType.id],
    [subType.extension],
    [subType.coding],
    [subType.text]
FROM openrowset (
        BULK 'Contract/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subType.JSON]  VARCHAR(MAX) '$.subType'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subType.JSON]) with (
        [subType.id]                   NVARCHAR(100)       '$.id',
        [subType.extension]            NVARCHAR(MAX)       '$.extension',
        [subType.coding]               NVARCHAR(MAX)       '$.coding' AS JSON,
        [subType.text]                 NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ContractTerm AS
SELECT
    [id],
    [term.JSON],
    [term.id],
    [term.extension],
    [term.modifierExtension],
    [term.identifier.id],
    [term.identifier.extension],
    [term.identifier.use],
    [term.identifier.type],
    [term.identifier.system],
    [term.identifier.value],
    [term.identifier.period],
    [term.identifier.assigner],
    [term.issued],
    [term.applies.id],
    [term.applies.extension],
    [term.applies.start],
    [term.applies.end],
    [term.type.id],
    [term.type.extension],
    [term.type.coding],
    [term.type.text],
    [term.subType.id],
    [term.subType.extension],
    [term.subType.coding],
    [term.subType.text],
    [term.text],
    [term.securityLabel],
    [term.offer.id],
    [term.offer.extension],
    [term.offer.modifierExtension],
    [term.offer.identifier],
    [term.offer.party],
    [term.offer.topic],
    [term.offer.type],
    [term.offer.decision],
    [term.offer.decisionMode],
    [term.offer.answer],
    [term.offer.text],
    [term.offer.linkId],
    [term.offer.securityLabelNumber],
    [term.asset],
    [term.action],
    [term.group],
    [term.topic.codeableConcept.id],
    [term.topic.codeableConcept.extension],
    [term.topic.codeableConcept.coding],
    [term.topic.codeableConcept.text],
    [term.topic.reference.id],
    [term.topic.reference.extension],
    [term.topic.reference.reference],
    [term.topic.reference.type],
    [term.topic.reference.identifier],
    [term.topic.reference.display]
FROM openrowset (
        BULK 'Contract/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [term.JSON]  VARCHAR(MAX) '$.term'
    ) AS rowset
    CROSS APPLY openjson (rowset.[term.JSON]) with (
        [term.id]                      NVARCHAR(100)       '$.id',
        [term.extension]               NVARCHAR(MAX)       '$.extension',
        [term.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [term.identifier.id]           NVARCHAR(100)       '$.identifier.id',
        [term.identifier.extension]    NVARCHAR(MAX)       '$.identifier.extension',
        [term.identifier.use]          NVARCHAR(64)        '$.identifier.use',
        [term.identifier.type]         NVARCHAR(MAX)       '$.identifier.type',
        [term.identifier.system]       VARCHAR(256)        '$.identifier.system',
        [term.identifier.value]        NVARCHAR(4000)      '$.identifier.value',
        [term.identifier.period]       NVARCHAR(MAX)       '$.identifier.period',
        [term.identifier.assigner]     NVARCHAR(MAX)       '$.identifier.assigner',
        [term.issued]                  VARCHAR(64)         '$.issued',
        [term.applies.id]              NVARCHAR(100)       '$.applies.id',
        [term.applies.extension]       NVARCHAR(MAX)       '$.applies.extension',
        [term.applies.start]           VARCHAR(64)         '$.applies.start',
        [term.applies.end]             VARCHAR(64)         '$.applies.end',
        [term.type.id]                 NVARCHAR(100)       '$.type.id',
        [term.type.extension]          NVARCHAR(MAX)       '$.type.extension',
        [term.type.coding]             NVARCHAR(MAX)       '$.type.coding',
        [term.type.text]               NVARCHAR(4000)      '$.type.text',
        [term.subType.id]              NVARCHAR(100)       '$.subType.id',
        [term.subType.extension]       NVARCHAR(MAX)       '$.subType.extension',
        [term.subType.coding]          NVARCHAR(MAX)       '$.subType.coding',
        [term.subType.text]            NVARCHAR(4000)      '$.subType.text',
        [term.text]                    NVARCHAR(4000)      '$.text',
        [term.securityLabel]           NVARCHAR(MAX)       '$.securityLabel' AS JSON,
        [term.offer.id]                NVARCHAR(100)       '$.offer.id',
        [term.offer.extension]         NVARCHAR(MAX)       '$.offer.extension',
        [term.offer.modifierExtension] NVARCHAR(MAX)       '$.offer.modifierExtension',
        [term.offer.identifier]        NVARCHAR(MAX)       '$.offer.identifier',
        [term.offer.party]             NVARCHAR(MAX)       '$.offer.party',
        [term.offer.topic]             NVARCHAR(MAX)       '$.offer.topic',
        [term.offer.type]              NVARCHAR(MAX)       '$.offer.type',
        [term.offer.decision]          NVARCHAR(MAX)       '$.offer.decision',
        [term.offer.decisionMode]      NVARCHAR(MAX)       '$.offer.decisionMode',
        [term.offer.answer]            NVARCHAR(MAX)       '$.offer.answer',
        [term.offer.text]              NVARCHAR(4000)      '$.offer.text',
        [term.offer.linkId]            NVARCHAR(MAX)       '$.offer.linkId',
        [term.offer.securityLabelNumber] NVARCHAR(MAX)       '$.offer.securityLabelNumber',
        [term.asset]                   NVARCHAR(MAX)       '$.asset' AS JSON,
        [term.action]                  NVARCHAR(MAX)       '$.action' AS JSON,
        [term.group]                   NVARCHAR(MAX)       '$.group' AS JSON,
        [term.topic.codeableConcept.id] NVARCHAR(100)       '$.topic.codeableConcept.id',
        [term.topic.codeableConcept.extension] NVARCHAR(MAX)       '$.topic.codeableConcept.extension',
        [term.topic.codeableConcept.coding] NVARCHAR(MAX)       '$.topic.codeableConcept.coding',
        [term.topic.codeableConcept.text] NVARCHAR(4000)      '$.topic.codeableConcept.text',
        [term.topic.reference.id]      NVARCHAR(100)       '$.topic.reference.id',
        [term.topic.reference.extension] NVARCHAR(MAX)       '$.topic.reference.extension',
        [term.topic.reference.reference] NVARCHAR(4000)      '$.topic.reference.reference',
        [term.topic.reference.type]    VARCHAR(256)        '$.topic.reference.type',
        [term.topic.reference.identifier] NVARCHAR(MAX)       '$.topic.reference.identifier',
        [term.topic.reference.display] NVARCHAR(4000)      '$.topic.reference.display'
    ) j

GO

CREATE VIEW fhir.ContractSupportingInfo AS
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
        BULK 'Contract/**',
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

CREATE VIEW fhir.ContractRelevantHistory AS
SELECT
    [id],
    [relevantHistory.JSON],
    [relevantHistory.id],
    [relevantHistory.extension],
    [relevantHistory.reference],
    [relevantHistory.type],
    [relevantHistory.identifier.id],
    [relevantHistory.identifier.extension],
    [relevantHistory.identifier.use],
    [relevantHistory.identifier.type],
    [relevantHistory.identifier.system],
    [relevantHistory.identifier.value],
    [relevantHistory.identifier.period],
    [relevantHistory.identifier.assigner],
    [relevantHistory.display]
FROM openrowset (
        BULK 'Contract/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [relevantHistory.JSON]  VARCHAR(MAX) '$.relevantHistory'
    ) AS rowset
    CROSS APPLY openjson (rowset.[relevantHistory.JSON]) with (
        [relevantHistory.id]           NVARCHAR(100)       '$.id',
        [relevantHistory.extension]    NVARCHAR(MAX)       '$.extension',
        [relevantHistory.reference]    NVARCHAR(4000)      '$.reference',
        [relevantHistory.type]         VARCHAR(256)        '$.type',
        [relevantHistory.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [relevantHistory.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [relevantHistory.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [relevantHistory.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [relevantHistory.identifier.system] VARCHAR(256)        '$.identifier.system',
        [relevantHistory.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [relevantHistory.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [relevantHistory.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [relevantHistory.display]      NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ContractSigner AS
SELECT
    [id],
    [signer.JSON],
    [signer.id],
    [signer.extension],
    [signer.modifierExtension],
    [signer.type.id],
    [signer.type.extension],
    [signer.type.system],
    [signer.type.version],
    [signer.type.code],
    [signer.type.display],
    [signer.type.userSelected],
    [signer.party.id],
    [signer.party.extension],
    [signer.party.reference],
    [signer.party.type],
    [signer.party.identifier],
    [signer.party.display],
    [signer.signature]
FROM openrowset (
        BULK 'Contract/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [signer.JSON]  VARCHAR(MAX) '$.signer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[signer.JSON]) with (
        [signer.id]                    NVARCHAR(100)       '$.id',
        [signer.extension]             NVARCHAR(MAX)       '$.extension',
        [signer.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [signer.type.id]               NVARCHAR(100)       '$.type.id',
        [signer.type.extension]        NVARCHAR(MAX)       '$.type.extension',
        [signer.type.system]           VARCHAR(256)        '$.type.system',
        [signer.type.version]          NVARCHAR(100)       '$.type.version',
        [signer.type.code]             NVARCHAR(4000)      '$.type.code',
        [signer.type.display]          NVARCHAR(4000)      '$.type.display',
        [signer.type.userSelected]     bit                 '$.type.userSelected',
        [signer.party.id]              NVARCHAR(100)       '$.party.id',
        [signer.party.extension]       NVARCHAR(MAX)       '$.party.extension',
        [signer.party.reference]       NVARCHAR(4000)      '$.party.reference',
        [signer.party.type]            VARCHAR(256)        '$.party.type',
        [signer.party.identifier]      NVARCHAR(MAX)       '$.party.identifier',
        [signer.party.display]         NVARCHAR(4000)      '$.party.display',
        [signer.signature]             NVARCHAR(MAX)       '$.signature' AS JSON
    ) j

GO

CREATE VIEW fhir.ContractFriendly AS
SELECT
    [id],
    [friendly.JSON],
    [friendly.id],
    [friendly.extension],
    [friendly.modifierExtension],
    [friendly.content.attachment.id],
    [friendly.content.attachment.extension],
    [friendly.content.attachment.contentType],
    [friendly.content.attachment.language],
    [friendly.content.attachment.data],
    [friendly.content.attachment.url],
    [friendly.content.attachment.size],
    [friendly.content.attachment.hash],
    [friendly.content.attachment.title],
    [friendly.content.attachment.creation],
    [friendly.content.reference.id],
    [friendly.content.reference.extension],
    [friendly.content.reference.reference],
    [friendly.content.reference.type],
    [friendly.content.reference.identifier],
    [friendly.content.reference.display]
FROM openrowset (
        BULK 'Contract/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [friendly.JSON]  VARCHAR(MAX) '$.friendly'
    ) AS rowset
    CROSS APPLY openjson (rowset.[friendly.JSON]) with (
        [friendly.id]                  NVARCHAR(100)       '$.id',
        [friendly.extension]           NVARCHAR(MAX)       '$.extension',
        [friendly.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [friendly.content.attachment.id] NVARCHAR(100)       '$.content.attachment.id',
        [friendly.content.attachment.extension] NVARCHAR(MAX)       '$.content.attachment.extension',
        [friendly.content.attachment.contentType] NVARCHAR(100)       '$.content.attachment.contentType',
        [friendly.content.attachment.language] NVARCHAR(100)       '$.content.attachment.language',
        [friendly.content.attachment.data] NVARCHAR(MAX)       '$.content.attachment.data',
        [friendly.content.attachment.url] VARCHAR(256)        '$.content.attachment.url',
        [friendly.content.attachment.size] bigint              '$.content.attachment.size',
        [friendly.content.attachment.hash] NVARCHAR(MAX)       '$.content.attachment.hash',
        [friendly.content.attachment.title] NVARCHAR(4000)      '$.content.attachment.title',
        [friendly.content.attachment.creation] VARCHAR(64)         '$.content.attachment.creation',
        [friendly.content.reference.id] NVARCHAR(100)       '$.content.reference.id',
        [friendly.content.reference.extension] NVARCHAR(MAX)       '$.content.reference.extension',
        [friendly.content.reference.reference] NVARCHAR(4000)      '$.content.reference.reference',
        [friendly.content.reference.type] VARCHAR(256)        '$.content.reference.type',
        [friendly.content.reference.identifier] NVARCHAR(MAX)       '$.content.reference.identifier',
        [friendly.content.reference.display] NVARCHAR(4000)      '$.content.reference.display'
    ) j

GO

CREATE VIEW fhir.ContractLegal AS
SELECT
    [id],
    [legal.JSON],
    [legal.id],
    [legal.extension],
    [legal.modifierExtension],
    [legal.content.attachment.id],
    [legal.content.attachment.extension],
    [legal.content.attachment.contentType],
    [legal.content.attachment.language],
    [legal.content.attachment.data],
    [legal.content.attachment.url],
    [legal.content.attachment.size],
    [legal.content.attachment.hash],
    [legal.content.attachment.title],
    [legal.content.attachment.creation],
    [legal.content.reference.id],
    [legal.content.reference.extension],
    [legal.content.reference.reference],
    [legal.content.reference.type],
    [legal.content.reference.identifier],
    [legal.content.reference.display]
FROM openrowset (
        BULK 'Contract/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [legal.JSON]  VARCHAR(MAX) '$.legal'
    ) AS rowset
    CROSS APPLY openjson (rowset.[legal.JSON]) with (
        [legal.id]                     NVARCHAR(100)       '$.id',
        [legal.extension]              NVARCHAR(MAX)       '$.extension',
        [legal.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [legal.content.attachment.id]  NVARCHAR(100)       '$.content.attachment.id',
        [legal.content.attachment.extension] NVARCHAR(MAX)       '$.content.attachment.extension',
        [legal.content.attachment.contentType] NVARCHAR(100)       '$.content.attachment.contentType',
        [legal.content.attachment.language] NVARCHAR(100)       '$.content.attachment.language',
        [legal.content.attachment.data] NVARCHAR(MAX)       '$.content.attachment.data',
        [legal.content.attachment.url] VARCHAR(256)        '$.content.attachment.url',
        [legal.content.attachment.size] bigint              '$.content.attachment.size',
        [legal.content.attachment.hash] NVARCHAR(MAX)       '$.content.attachment.hash',
        [legal.content.attachment.title] NVARCHAR(4000)      '$.content.attachment.title',
        [legal.content.attachment.creation] VARCHAR(64)         '$.content.attachment.creation',
        [legal.content.reference.id]   NVARCHAR(100)       '$.content.reference.id',
        [legal.content.reference.extension] NVARCHAR(MAX)       '$.content.reference.extension',
        [legal.content.reference.reference] NVARCHAR(4000)      '$.content.reference.reference',
        [legal.content.reference.type] VARCHAR(256)        '$.content.reference.type',
        [legal.content.reference.identifier] NVARCHAR(MAX)       '$.content.reference.identifier',
        [legal.content.reference.display] NVARCHAR(4000)      '$.content.reference.display'
    ) j

GO

CREATE VIEW fhir.ContractRule AS
SELECT
    [id],
    [rule.JSON],
    [rule.id],
    [rule.extension],
    [rule.modifierExtension],
    [rule.content.attachment.id],
    [rule.content.attachment.extension],
    [rule.content.attachment.contentType],
    [rule.content.attachment.language],
    [rule.content.attachment.data],
    [rule.content.attachment.url],
    [rule.content.attachment.size],
    [rule.content.attachment.hash],
    [rule.content.attachment.title],
    [rule.content.attachment.creation],
    [rule.content.reference.id],
    [rule.content.reference.extension],
    [rule.content.reference.reference],
    [rule.content.reference.type],
    [rule.content.reference.identifier],
    [rule.content.reference.display]
FROM openrowset (
        BULK 'Contract/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [rule.JSON]  VARCHAR(MAX) '$.rule'
    ) AS rowset
    CROSS APPLY openjson (rowset.[rule.JSON]) with (
        [rule.id]                      NVARCHAR(100)       '$.id',
        [rule.extension]               NVARCHAR(MAX)       '$.extension',
        [rule.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [rule.content.attachment.id]   NVARCHAR(100)       '$.content.attachment.id',
        [rule.content.attachment.extension] NVARCHAR(MAX)       '$.content.attachment.extension',
        [rule.content.attachment.contentType] NVARCHAR(100)       '$.content.attachment.contentType',
        [rule.content.attachment.language] NVARCHAR(100)       '$.content.attachment.language',
        [rule.content.attachment.data] NVARCHAR(MAX)       '$.content.attachment.data',
        [rule.content.attachment.url]  VARCHAR(256)        '$.content.attachment.url',
        [rule.content.attachment.size] bigint              '$.content.attachment.size',
        [rule.content.attachment.hash] NVARCHAR(MAX)       '$.content.attachment.hash',
        [rule.content.attachment.title] NVARCHAR(4000)      '$.content.attachment.title',
        [rule.content.attachment.creation] VARCHAR(64)         '$.content.attachment.creation',
        [rule.content.reference.id]    NVARCHAR(100)       '$.content.reference.id',
        [rule.content.reference.extension] NVARCHAR(MAX)       '$.content.reference.extension',
        [rule.content.reference.reference] NVARCHAR(4000)      '$.content.reference.reference',
        [rule.content.reference.type]  VARCHAR(256)        '$.content.reference.type',
        [rule.content.reference.identifier] NVARCHAR(MAX)       '$.content.reference.identifier',
        [rule.content.reference.display] NVARCHAR(4000)      '$.content.reference.display'
    ) j
