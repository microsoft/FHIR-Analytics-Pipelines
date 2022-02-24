CREATE EXTERNAL TABLE [fhir].[CoverageEligibilityRequest] (
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
    [priority.id] NVARCHAR(100),
    [priority.extension] NVARCHAR(MAX),
    [priority.coding] VARCHAR(MAX),
    [priority.text] NVARCHAR(4000),
    [purpose] VARCHAR(MAX),
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
    [created] VARCHAR(64),
    [enterer.id] NVARCHAR(100),
    [enterer.extension] NVARCHAR(MAX),
    [enterer.reference] NVARCHAR(4000),
    [enterer.type] VARCHAR(256),
    [enterer.identifier.id] NVARCHAR(100),
    [enterer.identifier.extension] NVARCHAR(MAX),
    [enterer.identifier.use] NVARCHAR(64),
    [enterer.identifier.type] NVARCHAR(MAX),
    [enterer.identifier.system] VARCHAR(256),
    [enterer.identifier.value] NVARCHAR(4000),
    [enterer.identifier.period] NVARCHAR(MAX),
    [enterer.identifier.assigner] NVARCHAR(MAX),
    [enterer.display] NVARCHAR(4000),
    [provider.id] NVARCHAR(100),
    [provider.extension] NVARCHAR(MAX),
    [provider.reference] NVARCHAR(4000),
    [provider.type] VARCHAR(256),
    [provider.identifier.id] NVARCHAR(100),
    [provider.identifier.extension] NVARCHAR(MAX),
    [provider.identifier.use] NVARCHAR(64),
    [provider.identifier.type] NVARCHAR(MAX),
    [provider.identifier.system] VARCHAR(256),
    [provider.identifier.value] NVARCHAR(4000),
    [provider.identifier.period] NVARCHAR(MAX),
    [provider.identifier.assigner] NVARCHAR(MAX),
    [provider.display] NVARCHAR(4000),
    [insurer.id] NVARCHAR(100),
    [insurer.extension] NVARCHAR(MAX),
    [insurer.reference] NVARCHAR(4000),
    [insurer.type] VARCHAR(256),
    [insurer.identifier.id] NVARCHAR(100),
    [insurer.identifier.extension] NVARCHAR(MAX),
    [insurer.identifier.use] NVARCHAR(64),
    [insurer.identifier.type] NVARCHAR(MAX),
    [insurer.identifier.system] VARCHAR(256),
    [insurer.identifier.value] NVARCHAR(4000),
    [insurer.identifier.period] NVARCHAR(MAX),
    [insurer.identifier.assigner] NVARCHAR(MAX),
    [insurer.display] NVARCHAR(4000),
    [facility.id] NVARCHAR(100),
    [facility.extension] NVARCHAR(MAX),
    [facility.reference] NVARCHAR(4000),
    [facility.type] VARCHAR(256),
    [facility.identifier.id] NVARCHAR(100),
    [facility.identifier.extension] NVARCHAR(MAX),
    [facility.identifier.use] NVARCHAR(64),
    [facility.identifier.type] NVARCHAR(MAX),
    [facility.identifier.system] VARCHAR(256),
    [facility.identifier.value] NVARCHAR(4000),
    [facility.identifier.period] NVARCHAR(MAX),
    [facility.identifier.assigner] NVARCHAR(MAX),
    [facility.display] NVARCHAR(4000),
    [supportingInfo] VARCHAR(MAX),
    [insurance] VARCHAR(MAX),
    [item] VARCHAR(MAX),
    [serviced.date] VARCHAR(64),
    [serviced.period.id] NVARCHAR(100),
    [serviced.period.extension] NVARCHAR(MAX),
    [serviced.period.start] VARCHAR(64),
    [serviced.period.end] VARCHAR(64),
) WITH (
    LOCATION='/CoverageEligibilityRequest/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.CoverageEligibilityRequestIdentifier AS
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
        BULK 'CoverageEligibilityRequest/**',
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

CREATE VIEW fhir.CoverageEligibilityRequestPurpose AS
SELECT
    [id],
    [purpose.JSON],
    [purpose]
FROM openrowset (
        BULK 'CoverageEligibilityRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [purpose.JSON]  VARCHAR(MAX) '$.purpose'
    ) AS rowset
    CROSS APPLY openjson (rowset.[purpose.JSON]) with (
        [purpose]                      NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.CoverageEligibilityRequestSupportingInfo AS
SELECT
    [id],
    [supportingInfo.JSON],
    [supportingInfo.id],
    [supportingInfo.extension],
    [supportingInfo.modifierExtension],
    [supportingInfo.sequence],
    [supportingInfo.information.id],
    [supportingInfo.information.extension],
    [supportingInfo.information.reference],
    [supportingInfo.information.type],
    [supportingInfo.information.identifier],
    [supportingInfo.information.display],
    [supportingInfo.appliesToAll]
FROM openrowset (
        BULK 'CoverageEligibilityRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [supportingInfo.JSON]  VARCHAR(MAX) '$.supportingInfo'
    ) AS rowset
    CROSS APPLY openjson (rowset.[supportingInfo.JSON]) with (
        [supportingInfo.id]            NVARCHAR(100)       '$.id',
        [supportingInfo.extension]     NVARCHAR(MAX)       '$.extension',
        [supportingInfo.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [supportingInfo.sequence]      bigint              '$.sequence',
        [supportingInfo.information.id] NVARCHAR(100)       '$.information.id',
        [supportingInfo.information.extension] NVARCHAR(MAX)       '$.information.extension',
        [supportingInfo.information.reference] NVARCHAR(4000)      '$.information.reference',
        [supportingInfo.information.type] VARCHAR(256)        '$.information.type',
        [supportingInfo.information.identifier] NVARCHAR(MAX)       '$.information.identifier',
        [supportingInfo.information.display] NVARCHAR(4000)      '$.information.display',
        [supportingInfo.appliesToAll]  bit                 '$.appliesToAll'
    ) j

GO

CREATE VIEW fhir.CoverageEligibilityRequestInsurance AS
SELECT
    [id],
    [insurance.JSON],
    [insurance.id],
    [insurance.extension],
    [insurance.modifierExtension],
    [insurance.focal],
    [insurance.coverage.id],
    [insurance.coverage.extension],
    [insurance.coverage.reference],
    [insurance.coverage.type],
    [insurance.coverage.identifier],
    [insurance.coverage.display],
    [insurance.businessArrangement]
FROM openrowset (
        BULK 'CoverageEligibilityRequest/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [insurance.JSON]  VARCHAR(MAX) '$.insurance'
    ) AS rowset
    CROSS APPLY openjson (rowset.[insurance.JSON]) with (
        [insurance.id]                 NVARCHAR(100)       '$.id',
        [insurance.extension]          NVARCHAR(MAX)       '$.extension',
        [insurance.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [insurance.focal]              bit                 '$.focal',
        [insurance.coverage.id]        NVARCHAR(100)       '$.coverage.id',
        [insurance.coverage.extension] NVARCHAR(MAX)       '$.coverage.extension',
        [insurance.coverage.reference] NVARCHAR(4000)      '$.coverage.reference',
        [insurance.coverage.type]      VARCHAR(256)        '$.coverage.type',
        [insurance.coverage.identifier] NVARCHAR(MAX)       '$.coverage.identifier',
        [insurance.coverage.display]   NVARCHAR(4000)      '$.coverage.display',
        [insurance.businessArrangement] NVARCHAR(4000)      '$.businessArrangement'
    ) j

GO

CREATE VIEW fhir.CoverageEligibilityRequestItem AS
SELECT
    [id],
    [item.JSON],
    [item.id],
    [item.extension],
    [item.modifierExtension],
    [item.supportingInfoSequence],
    [item.category.id],
    [item.category.extension],
    [item.category.coding],
    [item.category.text],
    [item.productOrService.id],
    [item.productOrService.extension],
    [item.productOrService.coding],
    [item.productOrService.text],
    [item.modifier],
    [item.provider.id],
    [item.provider.extension],
    [item.provider.reference],
    [item.provider.type],
    [item.provider.identifier],
    [item.provider.display],
    [item.quantity.id],
    [item.quantity.extension],
    [item.quantity.value],
    [item.quantity.comparator],
    [item.quantity.unit],
    [item.quantity.system],
    [item.quantity.code],
    [item.unitPrice.id],
    [item.unitPrice.extension],
    [item.unitPrice.value],
    [item.unitPrice.currency],
    [item.facility.id],
    [item.facility.extension],
    [item.facility.reference],
    [item.facility.type],
    [item.facility.identifier],
    [item.facility.display],
    [item.diagnosis],
    [item.detail]
FROM openrowset (
        BULK 'CoverageEligibilityRequest/**',
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
        [item.supportingInfoSequence]  NVARCHAR(MAX)       '$.supportingInfoSequence' AS JSON,
        [item.category.id]             NVARCHAR(100)       '$.category.id',
        [item.category.extension]      NVARCHAR(MAX)       '$.category.extension',
        [item.category.coding]         NVARCHAR(MAX)       '$.category.coding',
        [item.category.text]           NVARCHAR(4000)      '$.category.text',
        [item.productOrService.id]     NVARCHAR(100)       '$.productOrService.id',
        [item.productOrService.extension] NVARCHAR(MAX)       '$.productOrService.extension',
        [item.productOrService.coding] NVARCHAR(MAX)       '$.productOrService.coding',
        [item.productOrService.text]   NVARCHAR(4000)      '$.productOrService.text',
        [item.modifier]                NVARCHAR(MAX)       '$.modifier' AS JSON,
        [item.provider.id]             NVARCHAR(100)       '$.provider.id',
        [item.provider.extension]      NVARCHAR(MAX)       '$.provider.extension',
        [item.provider.reference]      NVARCHAR(4000)      '$.provider.reference',
        [item.provider.type]           VARCHAR(256)        '$.provider.type',
        [item.provider.identifier]     NVARCHAR(MAX)       '$.provider.identifier',
        [item.provider.display]        NVARCHAR(4000)      '$.provider.display',
        [item.quantity.id]             NVARCHAR(100)       '$.quantity.id',
        [item.quantity.extension]      NVARCHAR(MAX)       '$.quantity.extension',
        [item.quantity.value]          float               '$.quantity.value',
        [item.quantity.comparator]     NVARCHAR(64)        '$.quantity.comparator',
        [item.quantity.unit]           NVARCHAR(100)       '$.quantity.unit',
        [item.quantity.system]         VARCHAR(256)        '$.quantity.system',
        [item.quantity.code]           NVARCHAR(4000)      '$.quantity.code',
        [item.unitPrice.id]            NVARCHAR(100)       '$.unitPrice.id',
        [item.unitPrice.extension]     NVARCHAR(MAX)       '$.unitPrice.extension',
        [item.unitPrice.value]         float               '$.unitPrice.value',
        [item.unitPrice.currency]      NVARCHAR(100)       '$.unitPrice.currency',
        [item.facility.id]             NVARCHAR(100)       '$.facility.id',
        [item.facility.extension]      NVARCHAR(MAX)       '$.facility.extension',
        [item.facility.reference]      NVARCHAR(4000)      '$.facility.reference',
        [item.facility.type]           VARCHAR(256)        '$.facility.type',
        [item.facility.identifier]     NVARCHAR(MAX)       '$.facility.identifier',
        [item.facility.display]        NVARCHAR(4000)      '$.facility.display',
        [item.diagnosis]               NVARCHAR(MAX)       '$.diagnosis' AS JSON,
        [item.detail]                  NVARCHAR(MAX)       '$.detail' AS JSON
    ) j
