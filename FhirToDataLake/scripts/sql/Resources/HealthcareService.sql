CREATE EXTERNAL TABLE [fhir].[HealthcareService] (
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
    [active] bit,
    [providedBy.id] NVARCHAR(100),
    [providedBy.extension] NVARCHAR(MAX),
    [providedBy.reference] NVARCHAR(4000),
    [providedBy.type] VARCHAR(256),
    [providedBy.identifier.id] NVARCHAR(100),
    [providedBy.identifier.extension] NVARCHAR(MAX),
    [providedBy.identifier.use] NVARCHAR(64),
    [providedBy.identifier.type] NVARCHAR(MAX),
    [providedBy.identifier.system] VARCHAR(256),
    [providedBy.identifier.value] NVARCHAR(4000),
    [providedBy.identifier.period] NVARCHAR(MAX),
    [providedBy.identifier.assigner] NVARCHAR(MAX),
    [providedBy.display] NVARCHAR(4000),
    [category] VARCHAR(MAX),
    [type] VARCHAR(MAX),
    [specialty] VARCHAR(MAX),
    [location] VARCHAR(MAX),
    [name] NVARCHAR(500),
    [comment] NVARCHAR(4000),
    [extraDetails] NVARCHAR(MAX),
    [photo.id] NVARCHAR(100),
    [photo.extension] NVARCHAR(MAX),
    [photo.contentType] NVARCHAR(100),
    [photo.language] NVARCHAR(100),
    [photo.data] NVARCHAR(MAX),
    [photo.url] VARCHAR(256),
    [photo.size] bigint,
    [photo.hash] NVARCHAR(MAX),
    [photo.title] NVARCHAR(4000),
    [photo.creation] VARCHAR(64),
    [telecom] VARCHAR(MAX),
    [coverageArea] VARCHAR(MAX),
    [serviceProvisionCode] VARCHAR(MAX),
    [eligibility] VARCHAR(MAX),
    [program] VARCHAR(MAX),
    [characteristic] VARCHAR(MAX),
    [communication] VARCHAR(MAX),
    [referralMethod] VARCHAR(MAX),
    [appointmentRequired] bit,
    [availableTime] VARCHAR(MAX),
    [notAvailable] VARCHAR(MAX),
    [availabilityExceptions] NVARCHAR(4000),
    [endpoint] VARCHAR(MAX),
) WITH (
    LOCATION='/HealthcareService/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.HealthcareServiceIdentifier AS
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
        BULK 'HealthcareService/**',
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

CREATE VIEW fhir.HealthcareServiceCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'HealthcareService/**',
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

CREATE VIEW fhir.HealthcareServiceType AS
SELECT
    [id],
    [type.JSON],
    [type.id],
    [type.extension],
    [type.coding],
    [type.text]
FROM openrowset (
        BULK 'HealthcareService/**',
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

CREATE VIEW fhir.HealthcareServiceSpecialty AS
SELECT
    [id],
    [specialty.JSON],
    [specialty.id],
    [specialty.extension],
    [specialty.coding],
    [specialty.text]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specialty.JSON]  VARCHAR(MAX) '$.specialty'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specialty.JSON]) with (
        [specialty.id]                 NVARCHAR(100)       '$.id',
        [specialty.extension]          NVARCHAR(MAX)       '$.extension',
        [specialty.coding]             NVARCHAR(MAX)       '$.coding' AS JSON,
        [specialty.text]               NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.HealthcareServiceLocation AS
SELECT
    [id],
    [location.JSON],
    [location.id],
    [location.extension],
    [location.reference],
    [location.type],
    [location.identifier.id],
    [location.identifier.extension],
    [location.identifier.use],
    [location.identifier.type],
    [location.identifier.system],
    [location.identifier.value],
    [location.identifier.period],
    [location.identifier.assigner],
    [location.display]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [location.JSON]  VARCHAR(MAX) '$.location'
    ) AS rowset
    CROSS APPLY openjson (rowset.[location.JSON]) with (
        [location.id]                  NVARCHAR(100)       '$.id',
        [location.extension]           NVARCHAR(MAX)       '$.extension',
        [location.reference]           NVARCHAR(4000)      '$.reference',
        [location.type]                VARCHAR(256)        '$.type',
        [location.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [location.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [location.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [location.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [location.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [location.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [location.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [location.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [location.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.HealthcareServiceTelecom AS
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
        BULK 'HealthcareService/**',
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

CREATE VIEW fhir.HealthcareServiceCoverageArea AS
SELECT
    [id],
    [coverageArea.JSON],
    [coverageArea.id],
    [coverageArea.extension],
    [coverageArea.reference],
    [coverageArea.type],
    [coverageArea.identifier.id],
    [coverageArea.identifier.extension],
    [coverageArea.identifier.use],
    [coverageArea.identifier.type],
    [coverageArea.identifier.system],
    [coverageArea.identifier.value],
    [coverageArea.identifier.period],
    [coverageArea.identifier.assigner],
    [coverageArea.display]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [coverageArea.JSON]  VARCHAR(MAX) '$.coverageArea'
    ) AS rowset
    CROSS APPLY openjson (rowset.[coverageArea.JSON]) with (
        [coverageArea.id]              NVARCHAR(100)       '$.id',
        [coverageArea.extension]       NVARCHAR(MAX)       '$.extension',
        [coverageArea.reference]       NVARCHAR(4000)      '$.reference',
        [coverageArea.type]            VARCHAR(256)        '$.type',
        [coverageArea.identifier.id]   NVARCHAR(100)       '$.identifier.id',
        [coverageArea.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [coverageArea.identifier.use]  NVARCHAR(64)        '$.identifier.use',
        [coverageArea.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [coverageArea.identifier.system] VARCHAR(256)        '$.identifier.system',
        [coverageArea.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [coverageArea.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [coverageArea.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [coverageArea.display]         NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.HealthcareServiceServiceProvisionCode AS
SELECT
    [id],
    [serviceProvisionCode.JSON],
    [serviceProvisionCode.id],
    [serviceProvisionCode.extension],
    [serviceProvisionCode.coding],
    [serviceProvisionCode.text]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [serviceProvisionCode.JSON]  VARCHAR(MAX) '$.serviceProvisionCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[serviceProvisionCode.JSON]) with (
        [serviceProvisionCode.id]      NVARCHAR(100)       '$.id',
        [serviceProvisionCode.extension] NVARCHAR(MAX)       '$.extension',
        [serviceProvisionCode.coding]  NVARCHAR(MAX)       '$.coding' AS JSON,
        [serviceProvisionCode.text]    NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.HealthcareServiceEligibility AS
SELECT
    [id],
    [eligibility.JSON],
    [eligibility.id],
    [eligibility.extension],
    [eligibility.modifierExtension],
    [eligibility.code.id],
    [eligibility.code.extension],
    [eligibility.code.coding],
    [eligibility.code.text],
    [eligibility.comment]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [eligibility.JSON]  VARCHAR(MAX) '$.eligibility'
    ) AS rowset
    CROSS APPLY openjson (rowset.[eligibility.JSON]) with (
        [eligibility.id]               NVARCHAR(100)       '$.id',
        [eligibility.extension]        NVARCHAR(MAX)       '$.extension',
        [eligibility.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [eligibility.code.id]          NVARCHAR(100)       '$.code.id',
        [eligibility.code.extension]   NVARCHAR(MAX)       '$.code.extension',
        [eligibility.code.coding]      NVARCHAR(MAX)       '$.code.coding',
        [eligibility.code.text]        NVARCHAR(4000)      '$.code.text',
        [eligibility.comment]          NVARCHAR(MAX)       '$.comment'
    ) j

GO

CREATE VIEW fhir.HealthcareServiceProgram AS
SELECT
    [id],
    [program.JSON],
    [program.id],
    [program.extension],
    [program.coding],
    [program.text]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [program.JSON]  VARCHAR(MAX) '$.program'
    ) AS rowset
    CROSS APPLY openjson (rowset.[program.JSON]) with (
        [program.id]                   NVARCHAR(100)       '$.id',
        [program.extension]            NVARCHAR(MAX)       '$.extension',
        [program.coding]               NVARCHAR(MAX)       '$.coding' AS JSON,
        [program.text]                 NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.HealthcareServiceCharacteristic AS
SELECT
    [id],
    [characteristic.JSON],
    [characteristic.id],
    [characteristic.extension],
    [characteristic.coding],
    [characteristic.text]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [characteristic.JSON]  VARCHAR(MAX) '$.characteristic'
    ) AS rowset
    CROSS APPLY openjson (rowset.[characteristic.JSON]) with (
        [characteristic.id]            NVARCHAR(100)       '$.id',
        [characteristic.extension]     NVARCHAR(MAX)       '$.extension',
        [characteristic.coding]        NVARCHAR(MAX)       '$.coding' AS JSON,
        [characteristic.text]          NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.HealthcareServiceCommunication AS
SELECT
    [id],
    [communication.JSON],
    [communication.id],
    [communication.extension],
    [communication.coding],
    [communication.text]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [communication.JSON]  VARCHAR(MAX) '$.communication'
    ) AS rowset
    CROSS APPLY openjson (rowset.[communication.JSON]) with (
        [communication.id]             NVARCHAR(100)       '$.id',
        [communication.extension]      NVARCHAR(MAX)       '$.extension',
        [communication.coding]         NVARCHAR(MAX)       '$.coding' AS JSON,
        [communication.text]           NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.HealthcareServiceReferralMethod AS
SELECT
    [id],
    [referralMethod.JSON],
    [referralMethod.id],
    [referralMethod.extension],
    [referralMethod.coding],
    [referralMethod.text]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [referralMethod.JSON]  VARCHAR(MAX) '$.referralMethod'
    ) AS rowset
    CROSS APPLY openjson (rowset.[referralMethod.JSON]) with (
        [referralMethod.id]            NVARCHAR(100)       '$.id',
        [referralMethod.extension]     NVARCHAR(MAX)       '$.extension',
        [referralMethod.coding]        NVARCHAR(MAX)       '$.coding' AS JSON,
        [referralMethod.text]          NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.HealthcareServiceAvailableTime AS
SELECT
    [id],
    [availableTime.JSON],
    [availableTime.id],
    [availableTime.extension],
    [availableTime.modifierExtension],
    [availableTime.daysOfWeek],
    [availableTime.allDay],
    [availableTime.availableStartTime],
    [availableTime.availableEndTime]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [availableTime.JSON]  VARCHAR(MAX) '$.availableTime'
    ) AS rowset
    CROSS APPLY openjson (rowset.[availableTime.JSON]) with (
        [availableTime.id]             NVARCHAR(100)       '$.id',
        [availableTime.extension]      NVARCHAR(MAX)       '$.extension',
        [availableTime.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [availableTime.daysOfWeek]     NVARCHAR(MAX)       '$.daysOfWeek' AS JSON,
        [availableTime.allDay]         bit                 '$.allDay',
        [availableTime.availableStartTime] NVARCHAR(MAX)       '$.availableStartTime',
        [availableTime.availableEndTime] NVARCHAR(MAX)       '$.availableEndTime'
    ) j

GO

CREATE VIEW fhir.HealthcareServiceNotAvailable AS
SELECT
    [id],
    [notAvailable.JSON],
    [notAvailable.id],
    [notAvailable.extension],
    [notAvailable.modifierExtension],
    [notAvailable.description],
    [notAvailable.during.id],
    [notAvailable.during.extension],
    [notAvailable.during.start],
    [notAvailable.during.end]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [notAvailable.JSON]  VARCHAR(MAX) '$.notAvailable'
    ) AS rowset
    CROSS APPLY openjson (rowset.[notAvailable.JSON]) with (
        [notAvailable.id]              NVARCHAR(100)       '$.id',
        [notAvailable.extension]       NVARCHAR(MAX)       '$.extension',
        [notAvailable.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [notAvailable.description]     NVARCHAR(4000)      '$.description',
        [notAvailable.during.id]       NVARCHAR(100)       '$.during.id',
        [notAvailable.during.extension] NVARCHAR(MAX)       '$.during.extension',
        [notAvailable.during.start]    VARCHAR(64)         '$.during.start',
        [notAvailable.during.end]      VARCHAR(64)         '$.during.end'
    ) j

GO

CREATE VIEW fhir.HealthcareServiceEndpoint AS
SELECT
    [id],
    [endpoint.JSON],
    [endpoint.id],
    [endpoint.extension],
    [endpoint.reference],
    [endpoint.type],
    [endpoint.identifier.id],
    [endpoint.identifier.extension],
    [endpoint.identifier.use],
    [endpoint.identifier.type],
    [endpoint.identifier.system],
    [endpoint.identifier.value],
    [endpoint.identifier.period],
    [endpoint.identifier.assigner],
    [endpoint.display]
FROM openrowset (
        BULK 'HealthcareService/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [endpoint.JSON]  VARCHAR(MAX) '$.endpoint'
    ) AS rowset
    CROSS APPLY openjson (rowset.[endpoint.JSON]) with (
        [endpoint.id]                  NVARCHAR(100)       '$.id',
        [endpoint.extension]           NVARCHAR(MAX)       '$.extension',
        [endpoint.reference]           NVARCHAR(4000)      '$.reference',
        [endpoint.type]                VARCHAR(256)        '$.type',
        [endpoint.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [endpoint.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [endpoint.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [endpoint.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [endpoint.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [endpoint.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [endpoint.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [endpoint.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [endpoint.display]             NVARCHAR(4000)      '$.display'
    ) j
