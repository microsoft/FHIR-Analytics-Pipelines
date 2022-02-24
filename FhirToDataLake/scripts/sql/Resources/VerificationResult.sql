CREATE EXTERNAL TABLE [fhir].[VerificationResult] (
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
    [target] VARCHAR(MAX),
    [targetLocation] VARCHAR(MAX),
    [need.id] NVARCHAR(100),
    [need.extension] NVARCHAR(MAX),
    [need.coding] VARCHAR(MAX),
    [need.text] NVARCHAR(4000),
    [status] NVARCHAR(100),
    [statusDate] VARCHAR(64),
    [validationType.id] NVARCHAR(100),
    [validationType.extension] NVARCHAR(MAX),
    [validationType.coding] VARCHAR(MAX),
    [validationType.text] NVARCHAR(4000),
    [validationProcess] VARCHAR(MAX),
    [frequency.id] NVARCHAR(100),
    [frequency.extension] NVARCHAR(MAX),
    [frequency.modifierExtension] NVARCHAR(MAX),
    [frequency.event] VARCHAR(MAX),
    [frequency.repeat.id] NVARCHAR(100),
    [frequency.repeat.extension] NVARCHAR(MAX),
    [frequency.repeat.modifierExtension] NVARCHAR(MAX),
    [frequency.repeat.count] bigint,
    [frequency.repeat.countMax] bigint,
    [frequency.repeat.duration] float,
    [frequency.repeat.durationMax] float,
    [frequency.repeat.durationUnit] NVARCHAR(64),
    [frequency.repeat.frequency] bigint,
    [frequency.repeat.frequencyMax] bigint,
    [frequency.repeat.period] float,
    [frequency.repeat.periodMax] float,
    [frequency.repeat.periodUnit] NVARCHAR(64),
    [frequency.repeat.dayOfWeek] NVARCHAR(MAX),
    [frequency.repeat.timeOfDay] NVARCHAR(MAX),
    [frequency.repeat.when] NVARCHAR(MAX),
    [frequency.repeat.offset] bigint,
    [frequency.repeat.bounds.duration] NVARCHAR(MAX),
    [frequency.repeat.bounds.range] NVARCHAR(MAX),
    [frequency.repeat.bounds.period] NVARCHAR(MAX),
    [frequency.code.id] NVARCHAR(100),
    [frequency.code.extension] NVARCHAR(MAX),
    [frequency.code.coding] NVARCHAR(MAX),
    [frequency.code.text] NVARCHAR(4000),
    [lastPerformed] VARCHAR(64),
    [nextScheduled] VARCHAR(64),
    [failureAction.id] NVARCHAR(100),
    [failureAction.extension] NVARCHAR(MAX),
    [failureAction.coding] VARCHAR(MAX),
    [failureAction.text] NVARCHAR(4000),
    [primarySource] VARCHAR(MAX),
    [attestation.id] NVARCHAR(100),
    [attestation.extension] NVARCHAR(MAX),
    [attestation.modifierExtension] NVARCHAR(MAX),
    [attestation.who.id] NVARCHAR(100),
    [attestation.who.extension] NVARCHAR(MAX),
    [attestation.who.reference] NVARCHAR(4000),
    [attestation.who.type] VARCHAR(256),
    [attestation.who.identifier] NVARCHAR(MAX),
    [attestation.who.display] NVARCHAR(4000),
    [attestation.onBehalfOf.id] NVARCHAR(100),
    [attestation.onBehalfOf.extension] NVARCHAR(MAX),
    [attestation.onBehalfOf.reference] NVARCHAR(4000),
    [attestation.onBehalfOf.type] VARCHAR(256),
    [attestation.onBehalfOf.identifier] NVARCHAR(MAX),
    [attestation.onBehalfOf.display] NVARCHAR(4000),
    [attestation.communicationMethod.id] NVARCHAR(100),
    [attestation.communicationMethod.extension] NVARCHAR(MAX),
    [attestation.communicationMethod.coding] NVARCHAR(MAX),
    [attestation.communicationMethod.text] NVARCHAR(4000),
    [attestation.date] VARCHAR(64),
    [attestation.sourceIdentityCertificate] NVARCHAR(4000),
    [attestation.proxyIdentityCertificate] NVARCHAR(4000),
    [attestation.proxySignature.id] NVARCHAR(100),
    [attestation.proxySignature.extension] NVARCHAR(MAX),
    [attestation.proxySignature.type] NVARCHAR(MAX),
    [attestation.proxySignature.when] VARCHAR(64),
    [attestation.proxySignature.who] NVARCHAR(MAX),
    [attestation.proxySignature.onBehalfOf] NVARCHAR(MAX),
    [attestation.proxySignature.targetFormat] NVARCHAR(100),
    [attestation.proxySignature.sigFormat] NVARCHAR(100),
    [attestation.proxySignature.data] NVARCHAR(MAX),
    [attestation.sourceSignature.id] NVARCHAR(100),
    [attestation.sourceSignature.extension] NVARCHAR(MAX),
    [attestation.sourceSignature.type] NVARCHAR(MAX),
    [attestation.sourceSignature.when] VARCHAR(64),
    [attestation.sourceSignature.who] NVARCHAR(MAX),
    [attestation.sourceSignature.onBehalfOf] NVARCHAR(MAX),
    [attestation.sourceSignature.targetFormat] NVARCHAR(100),
    [attestation.sourceSignature.sigFormat] NVARCHAR(100),
    [attestation.sourceSignature.data] NVARCHAR(MAX),
    [validator] VARCHAR(MAX),
) WITH (
    LOCATION='/VerificationResult/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.VerificationResultTarget AS
SELECT
    [id],
    [target.JSON],
    [target.id],
    [target.extension],
    [target.reference],
    [target.type],
    [target.identifier.id],
    [target.identifier.extension],
    [target.identifier.use],
    [target.identifier.type],
    [target.identifier.system],
    [target.identifier.value],
    [target.identifier.period],
    [target.identifier.assigner],
    [target.display]
FROM openrowset (
        BULK 'VerificationResult/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [target.JSON]  VARCHAR(MAX) '$.target'
    ) AS rowset
    CROSS APPLY openjson (rowset.[target.JSON]) with (
        [target.id]                    NVARCHAR(100)       '$.id',
        [target.extension]             NVARCHAR(MAX)       '$.extension',
        [target.reference]             NVARCHAR(4000)      '$.reference',
        [target.type]                  VARCHAR(256)        '$.type',
        [target.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [target.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [target.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [target.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [target.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [target.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [target.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [target.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [target.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.VerificationResultTargetLocation AS
SELECT
    [id],
    [targetLocation.JSON],
    [targetLocation]
FROM openrowset (
        BULK 'VerificationResult/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [targetLocation.JSON]  VARCHAR(MAX) '$.targetLocation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[targetLocation.JSON]) with (
        [targetLocation]               NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.VerificationResultValidationProcess AS
SELECT
    [id],
    [validationProcess.JSON],
    [validationProcess.id],
    [validationProcess.extension],
    [validationProcess.coding],
    [validationProcess.text]
FROM openrowset (
        BULK 'VerificationResult/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [validationProcess.JSON]  VARCHAR(MAX) '$.validationProcess'
    ) AS rowset
    CROSS APPLY openjson (rowset.[validationProcess.JSON]) with (
        [validationProcess.id]         NVARCHAR(100)       '$.id',
        [validationProcess.extension]  NVARCHAR(MAX)       '$.extension',
        [validationProcess.coding]     NVARCHAR(MAX)       '$.coding' AS JSON,
        [validationProcess.text]       NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.VerificationResultPrimarySource AS
SELECT
    [id],
    [primarySource.JSON],
    [primarySource.id],
    [primarySource.extension],
    [primarySource.modifierExtension],
    [primarySource.who.id],
    [primarySource.who.extension],
    [primarySource.who.reference],
    [primarySource.who.type],
    [primarySource.who.identifier],
    [primarySource.who.display],
    [primarySource.type],
    [primarySource.communicationMethod],
    [primarySource.validationStatus.id],
    [primarySource.validationStatus.extension],
    [primarySource.validationStatus.coding],
    [primarySource.validationStatus.text],
    [primarySource.validationDate],
    [primarySource.canPushUpdates.id],
    [primarySource.canPushUpdates.extension],
    [primarySource.canPushUpdates.coding],
    [primarySource.canPushUpdates.text],
    [primarySource.pushTypeAvailable]
FROM openrowset (
        BULK 'VerificationResult/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [primarySource.JSON]  VARCHAR(MAX) '$.primarySource'
    ) AS rowset
    CROSS APPLY openjson (rowset.[primarySource.JSON]) with (
        [primarySource.id]             NVARCHAR(100)       '$.id',
        [primarySource.extension]      NVARCHAR(MAX)       '$.extension',
        [primarySource.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [primarySource.who.id]         NVARCHAR(100)       '$.who.id',
        [primarySource.who.extension]  NVARCHAR(MAX)       '$.who.extension',
        [primarySource.who.reference]  NVARCHAR(4000)      '$.who.reference',
        [primarySource.who.type]       VARCHAR(256)        '$.who.type',
        [primarySource.who.identifier] NVARCHAR(MAX)       '$.who.identifier',
        [primarySource.who.display]    NVARCHAR(4000)      '$.who.display',
        [primarySource.type]           NVARCHAR(MAX)       '$.type' AS JSON,
        [primarySource.communicationMethod] NVARCHAR(MAX)       '$.communicationMethod' AS JSON,
        [primarySource.validationStatus.id] NVARCHAR(100)       '$.validationStatus.id',
        [primarySource.validationStatus.extension] NVARCHAR(MAX)       '$.validationStatus.extension',
        [primarySource.validationStatus.coding] NVARCHAR(MAX)       '$.validationStatus.coding',
        [primarySource.validationStatus.text] NVARCHAR(4000)      '$.validationStatus.text',
        [primarySource.validationDate] VARCHAR(64)         '$.validationDate',
        [primarySource.canPushUpdates.id] NVARCHAR(100)       '$.canPushUpdates.id',
        [primarySource.canPushUpdates.extension] NVARCHAR(MAX)       '$.canPushUpdates.extension',
        [primarySource.canPushUpdates.coding] NVARCHAR(MAX)       '$.canPushUpdates.coding',
        [primarySource.canPushUpdates.text] NVARCHAR(4000)      '$.canPushUpdates.text',
        [primarySource.pushTypeAvailable] NVARCHAR(MAX)       '$.pushTypeAvailable' AS JSON
    ) j

GO

CREATE VIEW fhir.VerificationResultValidator AS
SELECT
    [id],
    [validator.JSON],
    [validator.id],
    [validator.extension],
    [validator.modifierExtension],
    [validator.organization.id],
    [validator.organization.extension],
    [validator.organization.reference],
    [validator.organization.type],
    [validator.organization.identifier],
    [validator.organization.display],
    [validator.identityCertificate],
    [validator.attestationSignature.id],
    [validator.attestationSignature.extension],
    [validator.attestationSignature.type],
    [validator.attestationSignature.when],
    [validator.attestationSignature.who],
    [validator.attestationSignature.onBehalfOf],
    [validator.attestationSignature.targetFormat],
    [validator.attestationSignature.sigFormat],
    [validator.attestationSignature.data]
FROM openrowset (
        BULK 'VerificationResult/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [validator.JSON]  VARCHAR(MAX) '$.validator'
    ) AS rowset
    CROSS APPLY openjson (rowset.[validator.JSON]) with (
        [validator.id]                 NVARCHAR(100)       '$.id',
        [validator.extension]          NVARCHAR(MAX)       '$.extension',
        [validator.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [validator.organization.id]    NVARCHAR(100)       '$.organization.id',
        [validator.organization.extension] NVARCHAR(MAX)       '$.organization.extension',
        [validator.organization.reference] NVARCHAR(4000)      '$.organization.reference',
        [validator.organization.type]  VARCHAR(256)        '$.organization.type',
        [validator.organization.identifier] NVARCHAR(MAX)       '$.organization.identifier',
        [validator.organization.display] NVARCHAR(4000)      '$.organization.display',
        [validator.identityCertificate] NVARCHAR(4000)      '$.identityCertificate',
        [validator.attestationSignature.id] NVARCHAR(100)       '$.attestationSignature.id',
        [validator.attestationSignature.extension] NVARCHAR(MAX)       '$.attestationSignature.extension',
        [validator.attestationSignature.type] NVARCHAR(MAX)       '$.attestationSignature.type',
        [validator.attestationSignature.when] VARCHAR(64)         '$.attestationSignature.when',
        [validator.attestationSignature.who] NVARCHAR(MAX)       '$.attestationSignature.who',
        [validator.attestationSignature.onBehalfOf] NVARCHAR(MAX)       '$.attestationSignature.onBehalfOf',
        [validator.attestationSignature.targetFormat] NVARCHAR(100)       '$.attestationSignature.targetFormat',
        [validator.attestationSignature.sigFormat] NVARCHAR(100)       '$.attestationSignature.sigFormat',
        [validator.attestationSignature.data] NVARCHAR(MAX)       '$.attestationSignature.data'
    ) j
