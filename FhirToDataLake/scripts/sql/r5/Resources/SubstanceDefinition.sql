CREATE EXTERNAL TABLE [fhir].[SubstanceDefinition] (
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
    [version] NVARCHAR(100),
    [status.id] NVARCHAR(100),
    [status.extension] NVARCHAR(MAX),
    [status.coding] VARCHAR(MAX),
    [status.text] NVARCHAR(4000),
    [classification] VARCHAR(MAX),
    [domain.id] NVARCHAR(100),
    [domain.extension] NVARCHAR(MAX),
    [domain.coding] VARCHAR(MAX),
    [domain.text] NVARCHAR(4000),
    [grade] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [informationSource] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [manufacturer] VARCHAR(MAX),
    [supplier] VARCHAR(MAX),
    [moiety] VARCHAR(MAX),
    [property] VARCHAR(MAX),
    [referenceInformation.id] NVARCHAR(100),
    [referenceInformation.extension] NVARCHAR(MAX),
    [referenceInformation.reference] NVARCHAR(4000),
    [referenceInformation.type] VARCHAR(256),
    [referenceInformation.identifier.id] NVARCHAR(100),
    [referenceInformation.identifier.extension] NVARCHAR(MAX),
    [referenceInformation.identifier.use] NVARCHAR(64),
    [referenceInformation.identifier.type] NVARCHAR(MAX),
    [referenceInformation.identifier.system] VARCHAR(256),
    [referenceInformation.identifier.value] NVARCHAR(4000),
    [referenceInformation.identifier.period] NVARCHAR(MAX),
    [referenceInformation.identifier.assigner] NVARCHAR(MAX),
    [referenceInformation.display] NVARCHAR(4000),
    [molecularWeight] VARCHAR(MAX),
    [structure.id] NVARCHAR(100),
    [structure.extension] NVARCHAR(MAX),
    [structure.modifierExtension] NVARCHAR(MAX),
    [structure.stereochemistry.id] NVARCHAR(100),
    [structure.stereochemistry.extension] NVARCHAR(MAX),
    [structure.stereochemistry.coding] NVARCHAR(MAX),
    [structure.stereochemistry.text] NVARCHAR(4000),
    [structure.opticalActivity.id] NVARCHAR(100),
    [structure.opticalActivity.extension] NVARCHAR(MAX),
    [structure.opticalActivity.coding] NVARCHAR(MAX),
    [structure.opticalActivity.text] NVARCHAR(4000),
    [structure.molecularFormula] NVARCHAR(500),
    [structure.molecularFormulaByMoiety] NVARCHAR(500),
    [structure.molecularWeight.id] NVARCHAR(100),
    [structure.molecularWeight.extension] NVARCHAR(MAX),
    [structure.molecularWeight.modifierExtension] NVARCHAR(MAX),
    [structure.molecularWeight.method] NVARCHAR(MAX),
    [structure.molecularWeight.type] NVARCHAR(MAX),
    [structure.molecularWeight.amount] NVARCHAR(MAX),
    [structure.technique] VARCHAR(MAX),
    [structure.sourceDocument] VARCHAR(MAX),
    [structure.representation] VARCHAR(MAX),
    [code] VARCHAR(MAX),
    [name] VARCHAR(MAX),
    [relationship] VARCHAR(MAX),
    [nucleicAcid.id] NVARCHAR(100),
    [nucleicAcid.extension] NVARCHAR(MAX),
    [nucleicAcid.reference] NVARCHAR(4000),
    [nucleicAcid.type] VARCHAR(256),
    [nucleicAcid.identifier.id] NVARCHAR(100),
    [nucleicAcid.identifier.extension] NVARCHAR(MAX),
    [nucleicAcid.identifier.use] NVARCHAR(64),
    [nucleicAcid.identifier.type] NVARCHAR(MAX),
    [nucleicAcid.identifier.system] VARCHAR(256),
    [nucleicAcid.identifier.value] NVARCHAR(4000),
    [nucleicAcid.identifier.period] NVARCHAR(MAX),
    [nucleicAcid.identifier.assigner] NVARCHAR(MAX),
    [nucleicAcid.display] NVARCHAR(4000),
    [polymer.id] NVARCHAR(100),
    [polymer.extension] NVARCHAR(MAX),
    [polymer.reference] NVARCHAR(4000),
    [polymer.type] VARCHAR(256),
    [polymer.identifier.id] NVARCHAR(100),
    [polymer.identifier.extension] NVARCHAR(MAX),
    [polymer.identifier.use] NVARCHAR(64),
    [polymer.identifier.type] NVARCHAR(MAX),
    [polymer.identifier.system] VARCHAR(256),
    [polymer.identifier.value] NVARCHAR(4000),
    [polymer.identifier.period] NVARCHAR(MAX),
    [polymer.identifier.assigner] NVARCHAR(MAX),
    [polymer.display] NVARCHAR(4000),
    [protein.id] NVARCHAR(100),
    [protein.extension] NVARCHAR(MAX),
    [protein.reference] NVARCHAR(4000),
    [protein.type] VARCHAR(256),
    [protein.identifier.id] NVARCHAR(100),
    [protein.identifier.extension] NVARCHAR(MAX),
    [protein.identifier.use] NVARCHAR(64),
    [protein.identifier.type] NVARCHAR(MAX),
    [protein.identifier.system] VARCHAR(256),
    [protein.identifier.value] NVARCHAR(4000),
    [protein.identifier.period] NVARCHAR(MAX),
    [protein.identifier.assigner] NVARCHAR(MAX),
    [protein.display] NVARCHAR(4000),
    [sourceMaterial.id] NVARCHAR(100),
    [sourceMaterial.extension] NVARCHAR(MAX),
    [sourceMaterial.modifierExtension] NVARCHAR(MAX),
    [sourceMaterial.type.id] NVARCHAR(100),
    [sourceMaterial.type.extension] NVARCHAR(MAX),
    [sourceMaterial.type.coding] NVARCHAR(MAX),
    [sourceMaterial.type.text] NVARCHAR(4000),
    [sourceMaterial.genus.id] NVARCHAR(100),
    [sourceMaterial.genus.extension] NVARCHAR(MAX),
    [sourceMaterial.genus.coding] NVARCHAR(MAX),
    [sourceMaterial.genus.text] NVARCHAR(4000),
    [sourceMaterial.species.id] NVARCHAR(100),
    [sourceMaterial.species.extension] NVARCHAR(MAX),
    [sourceMaterial.species.coding] NVARCHAR(MAX),
    [sourceMaterial.species.text] NVARCHAR(4000),
    [sourceMaterial.part.id] NVARCHAR(100),
    [sourceMaterial.part.extension] NVARCHAR(MAX),
    [sourceMaterial.part.coding] NVARCHAR(MAX),
    [sourceMaterial.part.text] NVARCHAR(4000),
    [sourceMaterial.countryOfOrigin] VARCHAR(MAX),
) WITH (
    LOCATION='/SubstanceDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.SubstanceDefinitionIdentifier AS
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
        BULK 'SubstanceDefinition/**',
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

CREATE VIEW fhir.SubstanceDefinitionClassification AS
SELECT
    [id],
    [classification.JSON],
    [classification.id],
    [classification.extension],
    [classification.coding],
    [classification.text]
FROM openrowset (
        BULK 'SubstanceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [classification.JSON]  VARCHAR(MAX) '$.classification'
    ) AS rowset
    CROSS APPLY openjson (rowset.[classification.JSON]) with (
        [classification.id]            NVARCHAR(100)       '$.id',
        [classification.extension]     NVARCHAR(MAX)       '$.extension',
        [classification.coding]        NVARCHAR(MAX)       '$.coding' AS JSON,
        [classification.text]          NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.SubstanceDefinitionGrade AS
SELECT
    [id],
    [grade.JSON],
    [grade.id],
    [grade.extension],
    [grade.coding],
    [grade.text]
FROM openrowset (
        BULK 'SubstanceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [grade.JSON]  VARCHAR(MAX) '$.grade'
    ) AS rowset
    CROSS APPLY openjson (rowset.[grade.JSON]) with (
        [grade.id]                     NVARCHAR(100)       '$.id',
        [grade.extension]              NVARCHAR(MAX)       '$.extension',
        [grade.coding]                 NVARCHAR(MAX)       '$.coding' AS JSON,
        [grade.text]                   NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.SubstanceDefinitionInformationSource AS
SELECT
    [id],
    [informationSource.JSON],
    [informationSource.id],
    [informationSource.extension],
    [informationSource.reference],
    [informationSource.type],
    [informationSource.identifier.id],
    [informationSource.identifier.extension],
    [informationSource.identifier.use],
    [informationSource.identifier.type],
    [informationSource.identifier.system],
    [informationSource.identifier.value],
    [informationSource.identifier.period],
    [informationSource.identifier.assigner],
    [informationSource.display]
FROM openrowset (
        BULK 'SubstanceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [informationSource.JSON]  VARCHAR(MAX) '$.informationSource'
    ) AS rowset
    CROSS APPLY openjson (rowset.[informationSource.JSON]) with (
        [informationSource.id]         NVARCHAR(100)       '$.id',
        [informationSource.extension]  NVARCHAR(MAX)       '$.extension',
        [informationSource.reference]  NVARCHAR(4000)      '$.reference',
        [informationSource.type]       VARCHAR(256)        '$.type',
        [informationSource.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [informationSource.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [informationSource.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [informationSource.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [informationSource.identifier.system] VARCHAR(256)        '$.identifier.system',
        [informationSource.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [informationSource.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [informationSource.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [informationSource.display]    NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.SubstanceDefinitionNote AS
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
        BULK 'SubstanceDefinition/**',
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

GO

CREATE VIEW fhir.SubstanceDefinitionManufacturer AS
SELECT
    [id],
    [manufacturer.JSON],
    [manufacturer.id],
    [manufacturer.extension],
    [manufacturer.reference],
    [manufacturer.type],
    [manufacturer.identifier.id],
    [manufacturer.identifier.extension],
    [manufacturer.identifier.use],
    [manufacturer.identifier.type],
    [manufacturer.identifier.system],
    [manufacturer.identifier.value],
    [manufacturer.identifier.period],
    [manufacturer.identifier.assigner],
    [manufacturer.display]
FROM openrowset (
        BULK 'SubstanceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [manufacturer.JSON]  VARCHAR(MAX) '$.manufacturer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[manufacturer.JSON]) with (
        [manufacturer.id]              NVARCHAR(100)       '$.id',
        [manufacturer.extension]       NVARCHAR(MAX)       '$.extension',
        [manufacturer.reference]       NVARCHAR(4000)      '$.reference',
        [manufacturer.type]            VARCHAR(256)        '$.type',
        [manufacturer.identifier.id]   NVARCHAR(100)       '$.identifier.id',
        [manufacturer.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [manufacturer.identifier.use]  NVARCHAR(64)        '$.identifier.use',
        [manufacturer.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [manufacturer.identifier.system] VARCHAR(256)        '$.identifier.system',
        [manufacturer.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [manufacturer.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [manufacturer.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [manufacturer.display]         NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.SubstanceDefinitionSupplier AS
SELECT
    [id],
    [supplier.JSON],
    [supplier.id],
    [supplier.extension],
    [supplier.reference],
    [supplier.type],
    [supplier.identifier.id],
    [supplier.identifier.extension],
    [supplier.identifier.use],
    [supplier.identifier.type],
    [supplier.identifier.system],
    [supplier.identifier.value],
    [supplier.identifier.period],
    [supplier.identifier.assigner],
    [supplier.display]
FROM openrowset (
        BULK 'SubstanceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [supplier.JSON]  VARCHAR(MAX) '$.supplier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[supplier.JSON]) with (
        [supplier.id]                  NVARCHAR(100)       '$.id',
        [supplier.extension]           NVARCHAR(MAX)       '$.extension',
        [supplier.reference]           NVARCHAR(4000)      '$.reference',
        [supplier.type]                VARCHAR(256)        '$.type',
        [supplier.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [supplier.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [supplier.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [supplier.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [supplier.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [supplier.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [supplier.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [supplier.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [supplier.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.SubstanceDefinitionMoiety AS
SELECT
    [id],
    [moiety.JSON],
    [moiety.id],
    [moiety.extension],
    [moiety.modifierExtension],
    [moiety.role.id],
    [moiety.role.extension],
    [moiety.role.coding],
    [moiety.role.text],
    [moiety.identifier.id],
    [moiety.identifier.extension],
    [moiety.identifier.use],
    [moiety.identifier.type],
    [moiety.identifier.system],
    [moiety.identifier.value],
    [moiety.identifier.period],
    [moiety.identifier.assigner],
    [moiety.name],
    [moiety.stereochemistry.id],
    [moiety.stereochemistry.extension],
    [moiety.stereochemistry.coding],
    [moiety.stereochemistry.text],
    [moiety.opticalActivity.id],
    [moiety.opticalActivity.extension],
    [moiety.opticalActivity.coding],
    [moiety.opticalActivity.text],
    [moiety.molecularFormula],
    [moiety.amountType.id],
    [moiety.amountType.extension],
    [moiety.amountType.coding],
    [moiety.amountType.text],
    [moiety.amount.quantity.id],
    [moiety.amount.quantity.extension],
    [moiety.amount.quantity.value],
    [moiety.amount.quantity.comparator],
    [moiety.amount.quantity.unit],
    [moiety.amount.quantity.system],
    [moiety.amount.quantity.code],
    [moiety.amount.string]
FROM openrowset (
        BULK 'SubstanceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [moiety.JSON]  VARCHAR(MAX) '$.moiety'
    ) AS rowset
    CROSS APPLY openjson (rowset.[moiety.JSON]) with (
        [moiety.id]                    NVARCHAR(100)       '$.id',
        [moiety.extension]             NVARCHAR(MAX)       '$.extension',
        [moiety.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [moiety.role.id]               NVARCHAR(100)       '$.role.id',
        [moiety.role.extension]        NVARCHAR(MAX)       '$.role.extension',
        [moiety.role.coding]           NVARCHAR(MAX)       '$.role.coding',
        [moiety.role.text]             NVARCHAR(4000)      '$.role.text',
        [moiety.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [moiety.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [moiety.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [moiety.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [moiety.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [moiety.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [moiety.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [moiety.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [moiety.name]                  NVARCHAR(500)       '$.name',
        [moiety.stereochemistry.id]    NVARCHAR(100)       '$.stereochemistry.id',
        [moiety.stereochemistry.extension] NVARCHAR(MAX)       '$.stereochemistry.extension',
        [moiety.stereochemistry.coding] NVARCHAR(MAX)       '$.stereochemistry.coding',
        [moiety.stereochemistry.text]  NVARCHAR(4000)      '$.stereochemistry.text',
        [moiety.opticalActivity.id]    NVARCHAR(100)       '$.opticalActivity.id',
        [moiety.opticalActivity.extension] NVARCHAR(MAX)       '$.opticalActivity.extension',
        [moiety.opticalActivity.coding] NVARCHAR(MAX)       '$.opticalActivity.coding',
        [moiety.opticalActivity.text]  NVARCHAR(4000)      '$.opticalActivity.text',
        [moiety.molecularFormula]      NVARCHAR(500)       '$.molecularFormula',
        [moiety.amountType.id]         NVARCHAR(100)       '$.amountType.id',
        [moiety.amountType.extension]  NVARCHAR(MAX)       '$.amountType.extension',
        [moiety.amountType.coding]     NVARCHAR(MAX)       '$.amountType.coding',
        [moiety.amountType.text]       NVARCHAR(4000)      '$.amountType.text',
        [moiety.amount.quantity.id]    NVARCHAR(100)       '$.amount.quantity.id',
        [moiety.amount.quantity.extension] NVARCHAR(MAX)       '$.amount.quantity.extension',
        [moiety.amount.quantity.value] float               '$.amount.quantity.value',
        [moiety.amount.quantity.comparator] NVARCHAR(64)        '$.amount.quantity.comparator',
        [moiety.amount.quantity.unit]  NVARCHAR(100)       '$.amount.quantity.unit',
        [moiety.amount.quantity.system] VARCHAR(256)        '$.amount.quantity.system',
        [moiety.amount.quantity.code]  NVARCHAR(4000)      '$.amount.quantity.code',
        [moiety.amount.string]         NVARCHAR(4000)      '$.amount.string'
    ) j

GO

CREATE VIEW fhir.SubstanceDefinitionProperty AS
SELECT
    [id],
    [property.JSON],
    [property.id],
    [property.extension],
    [property.modifierExtension],
    [property.type.id],
    [property.type.extension],
    [property.type.coding],
    [property.type.text],
    [property.value.codeableConcept.id],
    [property.value.codeableConcept.extension],
    [property.value.codeableConcept.coding],
    [property.value.codeableConcept.text],
    [property.value.quantity.id],
    [property.value.quantity.extension],
    [property.value.quantity.value],
    [property.value.quantity.comparator],
    [property.value.quantity.unit],
    [property.value.quantity.system],
    [property.value.quantity.code],
    [property.value.date],
    [property.value.boolean],
    [property.value.attachment.id],
    [property.value.attachment.extension],
    [property.value.attachment.contentType],
    [property.value.attachment.language],
    [property.value.attachment.data],
    [property.value.attachment.url],
    [property.value.attachment.size],
    [property.value.attachment.hash],
    [property.value.attachment.title],
    [property.value.attachment.creation],
    [property.value.attachment.height],
    [property.value.attachment.width],
    [property.value.attachment.frames],
    [property.value.attachment.duration],
    [property.value.attachment.pages]
FROM openrowset (
        BULK 'SubstanceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [property.JSON]  VARCHAR(MAX) '$.property'
    ) AS rowset
    CROSS APPLY openjson (rowset.[property.JSON]) with (
        [property.id]                  NVARCHAR(100)       '$.id',
        [property.extension]           NVARCHAR(MAX)       '$.extension',
        [property.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [property.type.id]             NVARCHAR(100)       '$.type.id',
        [property.type.extension]      NVARCHAR(MAX)       '$.type.extension',
        [property.type.coding]         NVARCHAR(MAX)       '$.type.coding',
        [property.type.text]           NVARCHAR(4000)      '$.type.text',
        [property.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [property.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [property.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [property.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [property.value.quantity.id]   NVARCHAR(100)       '$.value.quantity.id',
        [property.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [property.value.quantity.value] float               '$.value.quantity.value',
        [property.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [property.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [property.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [property.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [property.value.date]          VARCHAR(64)         '$.value.date',
        [property.value.boolean]       bit                 '$.value.boolean',
        [property.value.attachment.id] NVARCHAR(100)       '$.value.attachment.id',
        [property.value.attachment.extension] NVARCHAR(MAX)       '$.value.attachment.extension',
        [property.value.attachment.contentType] NVARCHAR(100)       '$.value.attachment.contentType',
        [property.value.attachment.language] NVARCHAR(100)       '$.value.attachment.language',
        [property.value.attachment.data] NVARCHAR(MAX)       '$.value.attachment.data',
        [property.value.attachment.url] VARCHAR(256)        '$.value.attachment.url',
        [property.value.attachment.size] NVARCHAR(MAX)       '$.value.attachment.size',
        [property.value.attachment.hash] NVARCHAR(MAX)       '$.value.attachment.hash',
        [property.value.attachment.title] NVARCHAR(4000)      '$.value.attachment.title',
        [property.value.attachment.creation] VARCHAR(64)         '$.value.attachment.creation',
        [property.value.attachment.height] bigint              '$.value.attachment.height',
        [property.value.attachment.width] bigint              '$.value.attachment.width',
        [property.value.attachment.frames] bigint              '$.value.attachment.frames',
        [property.value.attachment.duration] float               '$.value.attachment.duration',
        [property.value.attachment.pages] bigint              '$.value.attachment.pages'
    ) j

GO

CREATE VIEW fhir.SubstanceDefinitionMolecularWeight AS
SELECT
    [id],
    [molecularWeight.JSON],
    [molecularWeight.id],
    [molecularWeight.extension],
    [molecularWeight.modifierExtension],
    [molecularWeight.method.id],
    [molecularWeight.method.extension],
    [molecularWeight.method.coding],
    [molecularWeight.method.text],
    [molecularWeight.type.id],
    [molecularWeight.type.extension],
    [molecularWeight.type.coding],
    [molecularWeight.type.text],
    [molecularWeight.amount.id],
    [molecularWeight.amount.extension],
    [molecularWeight.amount.value],
    [molecularWeight.amount.comparator],
    [molecularWeight.amount.unit],
    [molecularWeight.amount.system],
    [molecularWeight.amount.code]
FROM openrowset (
        BULK 'SubstanceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [molecularWeight.JSON]  VARCHAR(MAX) '$.molecularWeight'
    ) AS rowset
    CROSS APPLY openjson (rowset.[molecularWeight.JSON]) with (
        [molecularWeight.id]           NVARCHAR(100)       '$.id',
        [molecularWeight.extension]    NVARCHAR(MAX)       '$.extension',
        [molecularWeight.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [molecularWeight.method.id]    NVARCHAR(100)       '$.method.id',
        [molecularWeight.method.extension] NVARCHAR(MAX)       '$.method.extension',
        [molecularWeight.method.coding] NVARCHAR(MAX)       '$.method.coding',
        [molecularWeight.method.text]  NVARCHAR(4000)      '$.method.text',
        [molecularWeight.type.id]      NVARCHAR(100)       '$.type.id',
        [molecularWeight.type.extension] NVARCHAR(MAX)       '$.type.extension',
        [molecularWeight.type.coding]  NVARCHAR(MAX)       '$.type.coding',
        [molecularWeight.type.text]    NVARCHAR(4000)      '$.type.text',
        [molecularWeight.amount.id]    NVARCHAR(100)       '$.amount.id',
        [molecularWeight.amount.extension] NVARCHAR(MAX)       '$.amount.extension',
        [molecularWeight.amount.value] float               '$.amount.value',
        [molecularWeight.amount.comparator] NVARCHAR(64)        '$.amount.comparator',
        [molecularWeight.amount.unit]  NVARCHAR(100)       '$.amount.unit',
        [molecularWeight.amount.system] VARCHAR(256)        '$.amount.system',
        [molecularWeight.amount.code]  NVARCHAR(4000)      '$.amount.code'
    ) j

GO

CREATE VIEW fhir.SubstanceDefinitionCode AS
SELECT
    [id],
    [code.JSON],
    [code.id],
    [code.extension],
    [code.modifierExtension],
    [code.code.id],
    [code.code.extension],
    [code.code.coding],
    [code.code.text],
    [code.status.id],
    [code.status.extension],
    [code.status.coding],
    [code.status.text],
    [code.statusDate],
    [code.note],
    [code.source]
FROM openrowset (
        BULK 'SubstanceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [code.JSON]  VARCHAR(MAX) '$.code'
    ) AS rowset
    CROSS APPLY openjson (rowset.[code.JSON]) with (
        [code.id]                      NVARCHAR(100)       '$.id',
        [code.extension]               NVARCHAR(MAX)       '$.extension',
        [code.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [code.code.id]                 NVARCHAR(100)       '$.code.id',
        [code.code.extension]          NVARCHAR(MAX)       '$.code.extension',
        [code.code.coding]             NVARCHAR(MAX)       '$.code.coding',
        [code.code.text]               NVARCHAR(4000)      '$.code.text',
        [code.status.id]               NVARCHAR(100)       '$.status.id',
        [code.status.extension]        NVARCHAR(MAX)       '$.status.extension',
        [code.status.coding]           NVARCHAR(MAX)       '$.status.coding',
        [code.status.text]             NVARCHAR(4000)      '$.status.text',
        [code.statusDate]              VARCHAR(64)         '$.statusDate',
        [code.note]                    NVARCHAR(MAX)       '$.note' AS JSON,
        [code.source]                  NVARCHAR(MAX)       '$.source' AS JSON
    ) j

GO

CREATE VIEW fhir.SubstanceDefinitionName AS
SELECT
    [id],
    [name.JSON],
    [name.id],
    [name.extension],
    [name.modifierExtension],
    [name.name],
    [name.type.id],
    [name.type.extension],
    [name.type.coding],
    [name.type.text],
    [name.status.id],
    [name.status.extension],
    [name.status.coding],
    [name.status.text],
    [name.preferred],
    [name.language],
    [name.domain],
    [name.jurisdiction],
    [name.synonym],
    [name.translation],
    [name.official],
    [name.source]
FROM openrowset (
        BULK 'SubstanceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [name.JSON]  VARCHAR(MAX) '$.name'
    ) AS rowset
    CROSS APPLY openjson (rowset.[name.JSON]) with (
        [name.id]                      NVARCHAR(100)       '$.id',
        [name.extension]               NVARCHAR(MAX)       '$.extension',
        [name.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [name.name]                    NVARCHAR(500)       '$.name',
        [name.type.id]                 NVARCHAR(100)       '$.type.id',
        [name.type.extension]          NVARCHAR(MAX)       '$.type.extension',
        [name.type.coding]             NVARCHAR(MAX)       '$.type.coding',
        [name.type.text]               NVARCHAR(4000)      '$.type.text',
        [name.status.id]               NVARCHAR(100)       '$.status.id',
        [name.status.extension]        NVARCHAR(MAX)       '$.status.extension',
        [name.status.coding]           NVARCHAR(MAX)       '$.status.coding',
        [name.status.text]             NVARCHAR(4000)      '$.status.text',
        [name.preferred]               bit                 '$.preferred',
        [name.language]                NVARCHAR(MAX)       '$.language' AS JSON,
        [name.domain]                  NVARCHAR(MAX)       '$.domain' AS JSON,
        [name.jurisdiction]            NVARCHAR(MAX)       '$.jurisdiction' AS JSON,
        [name.synonym]                 NVARCHAR(MAX)       '$.synonym' AS JSON,
        [name.translation]             NVARCHAR(MAX)       '$.translation' AS JSON,
        [name.official]                NVARCHAR(MAX)       '$.official' AS JSON,
        [name.source]                  NVARCHAR(MAX)       '$.source' AS JSON
    ) j

GO

CREATE VIEW fhir.SubstanceDefinitionRelationship AS
SELECT
    [id],
    [relationship.JSON],
    [relationship.id],
    [relationship.extension],
    [relationship.modifierExtension],
    [relationship.type.id],
    [relationship.type.extension],
    [relationship.type.coding],
    [relationship.type.text],
    [relationship.isDefining],
    [relationship.amountRatioHighLimit.id],
    [relationship.amountRatioHighLimit.extension],
    [relationship.amountRatioHighLimit.numerator],
    [relationship.amountRatioHighLimit.denominator],
    [relationship.amountType.id],
    [relationship.amountType.extension],
    [relationship.amountType.coding],
    [relationship.amountType.text],
    [relationship.source],
    [relationship.substanceDefinition.reference.id],
    [relationship.substanceDefinition.reference.extension],
    [relationship.substanceDefinition.reference.reference],
    [relationship.substanceDefinition.reference.type],
    [relationship.substanceDefinition.reference.identifier],
    [relationship.substanceDefinition.reference.display],
    [relationship.substanceDefinition.codeableConcept.id],
    [relationship.substanceDefinition.codeableConcept.extension],
    [relationship.substanceDefinition.codeableConcept.coding],
    [relationship.substanceDefinition.codeableConcept.text],
    [relationship.amount.quantity.id],
    [relationship.amount.quantity.extension],
    [relationship.amount.quantity.value],
    [relationship.amount.quantity.comparator],
    [relationship.amount.quantity.unit],
    [relationship.amount.quantity.system],
    [relationship.amount.quantity.code],
    [relationship.amount.ratio.id],
    [relationship.amount.ratio.extension],
    [relationship.amount.ratio.numerator],
    [relationship.amount.ratio.denominator],
    [relationship.amount.string]
FROM openrowset (
        BULK 'SubstanceDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [relationship.JSON]  VARCHAR(MAX) '$.relationship'
    ) AS rowset
    CROSS APPLY openjson (rowset.[relationship.JSON]) with (
        [relationship.id]              NVARCHAR(100)       '$.id',
        [relationship.extension]       NVARCHAR(MAX)       '$.extension',
        [relationship.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [relationship.type.id]         NVARCHAR(100)       '$.type.id',
        [relationship.type.extension]  NVARCHAR(MAX)       '$.type.extension',
        [relationship.type.coding]     NVARCHAR(MAX)       '$.type.coding',
        [relationship.type.text]       NVARCHAR(4000)      '$.type.text',
        [relationship.isDefining]      bit                 '$.isDefining',
        [relationship.amountRatioHighLimit.id] NVARCHAR(100)       '$.amountRatioHighLimit.id',
        [relationship.amountRatioHighLimit.extension] NVARCHAR(MAX)       '$.amountRatioHighLimit.extension',
        [relationship.amountRatioHighLimit.numerator] NVARCHAR(MAX)       '$.amountRatioHighLimit.numerator',
        [relationship.amountRatioHighLimit.denominator] NVARCHAR(MAX)       '$.amountRatioHighLimit.denominator',
        [relationship.amountType.id]   NVARCHAR(100)       '$.amountType.id',
        [relationship.amountType.extension] NVARCHAR(MAX)       '$.amountType.extension',
        [relationship.amountType.coding] NVARCHAR(MAX)       '$.amountType.coding',
        [relationship.amountType.text] NVARCHAR(4000)      '$.amountType.text',
        [relationship.source]          NVARCHAR(MAX)       '$.source' AS JSON,
        [relationship.substanceDefinition.reference.id] NVARCHAR(100)       '$.substanceDefinition.reference.id',
        [relationship.substanceDefinition.reference.extension] NVARCHAR(MAX)       '$.substanceDefinition.reference.extension',
        [relationship.substanceDefinition.reference.reference] NVARCHAR(4000)      '$.substanceDefinition.reference.reference',
        [relationship.substanceDefinition.reference.type] VARCHAR(256)        '$.substanceDefinition.reference.type',
        [relationship.substanceDefinition.reference.identifier] NVARCHAR(MAX)       '$.substanceDefinition.reference.identifier',
        [relationship.substanceDefinition.reference.display] NVARCHAR(4000)      '$.substanceDefinition.reference.display',
        [relationship.substanceDefinition.codeableConcept.id] NVARCHAR(100)       '$.substanceDefinition.codeableConcept.id',
        [relationship.substanceDefinition.codeableConcept.extension] NVARCHAR(MAX)       '$.substanceDefinition.codeableConcept.extension',
        [relationship.substanceDefinition.codeableConcept.coding] NVARCHAR(MAX)       '$.substanceDefinition.codeableConcept.coding',
        [relationship.substanceDefinition.codeableConcept.text] NVARCHAR(4000)      '$.substanceDefinition.codeableConcept.text',
        [relationship.amount.quantity.id] NVARCHAR(100)       '$.amount.quantity.id',
        [relationship.amount.quantity.extension] NVARCHAR(MAX)       '$.amount.quantity.extension',
        [relationship.amount.quantity.value] float               '$.amount.quantity.value',
        [relationship.amount.quantity.comparator] NVARCHAR(64)        '$.amount.quantity.comparator',
        [relationship.amount.quantity.unit] NVARCHAR(100)       '$.amount.quantity.unit',
        [relationship.amount.quantity.system] VARCHAR(256)        '$.amount.quantity.system',
        [relationship.amount.quantity.code] NVARCHAR(4000)      '$.amount.quantity.code',
        [relationship.amount.ratio.id] NVARCHAR(100)       '$.amount.ratio.id',
        [relationship.amount.ratio.extension] NVARCHAR(MAX)       '$.amount.ratio.extension',
        [relationship.amount.ratio.numerator] NVARCHAR(MAX)       '$.amount.ratio.numerator',
        [relationship.amount.ratio.denominator] NVARCHAR(MAX)       '$.amount.ratio.denominator',
        [relationship.amount.string]   NVARCHAR(4000)      '$.amount.string'
    ) j
