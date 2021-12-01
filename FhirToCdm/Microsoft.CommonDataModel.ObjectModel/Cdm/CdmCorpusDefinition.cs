﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Runtime.CompilerServices;

#if INTERNAL_VSTS
[assembly: InternalsVisibleTo("Microsoft.CommonDataModel.ObjectModel.Persistence.Odi" + Microsoft.CommonDataModel.AssemblyRef.ProductPublicKey)]
[assembly: InternalsVisibleTo("Microsoft.CommonDataModel.ObjectModel.Persistence.Odi.Tests" + Microsoft.CommonDataModel.AssemblyRef.TestPublicKey)]
#else
[assembly: InternalsVisibleTo("Microsoft.CommonDataModel.ObjectModel.Persistence.Odi")]
[assembly: InternalsVisibleTo("Microsoft.CommonDataModel.ObjectModel.Persistence.Odi.Tests")]
#endif
namespace Microsoft.CommonDataModel.ObjectModel.Cdm
{
    using Microsoft.CommonDataModel.ObjectModel.Enums;
    using Microsoft.CommonDataModel.ObjectModel.Persistence;
    using Microsoft.CommonDataModel.ObjectModel.ResolvedModel;
    using Microsoft.CommonDataModel.ObjectModel.Storage;
    using Microsoft.CommonDataModel.ObjectModel.Utilities;
    using Microsoft.CommonDataModel.ObjectModel.Utilities.Logging;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public class CdmCorpusDefinition
    {
        internal static int _nextId = 0;

        /// <summary>
        /// Gets or sets the root path.
        /// </summary>
        public string RootPath { get; set; }

        /// <summary>
        /// The storage.
        /// </summary>
        public StorageManager Storage { get; }

        /// <summary>
        /// The persistence layer.
        /// </summary>
        public PersistenceLayer Persistence { get; }

        /// <summary>
        /// Gets the object context.
        /// </summary>
        public CdmCorpusContext Ctx { get; }

        /// <summary>
        /// Gets or sets the app ID, optional property.
        /// </summary>
        public string AppId { get; set; }

        /// <summary>
        /// Whether we are currently performing a resolution or not.
        /// Used to stop making documents dirty during CdmCollections operations.
        /// </summary>
        internal bool isCurrentlyResolving = false;

        /// <summary>
        /// Used by Visit functions of CdmObjects to skip calculating the declaredPath.
        /// </summary>
        internal bool blockDeclaredPathChanges = false;

        /// <summary>
        /// The set of resolution directives that will be used by default by the object model when it is resolving
        /// entities and when no per-call set of directives is provided.
        /// </summary>
        public AttributeResolutionDirectiveSet DefaultResolutionDirectives { get; set; }

        private IDictionary<string, List<CdmDocumentDefinition>> SymbolDefinitions { get; set; }

        internal IDictionary<string, SymbolSet> DefinitionReferenceSymbols { get; set; }

        private IDictionary<string, ResolvedTraitSet> EmptyRts { get; set; }

        private IDictionary<string, CdmFolderDefinition> NamespaceFolders { get; set; }

        internal CdmManifestDefinition rootManifest { get; set; }

        internal DocumentLibrary documentLibrary;

        private IDictionary<CdmObjectDefinition, List<CdmE2ERelationship>> OutgoingRelationships;

        private IDictionary<CdmObjectDefinition, List<CdmE2ERelationship>> IncomingRelationships;

        internal IDictionary<string, string> resEntMap { get; set; }

        internal SpinLock spinLock;

        /// <summary>
        /// Constructs a CdmCorpusDefinition.
        /// </summary>
        public CdmCorpusDefinition()
        {
            this.SymbolDefinitions = new Dictionary<string, List<CdmDocumentDefinition>>();
            this.DefinitionReferenceSymbols = new Dictionary<string, SymbolSet>();
            this.EmptyRts = new Dictionary<string, ResolvedTraitSet>();
            this.NamespaceFolders = new Dictionary<string, CdmFolderDefinition>();
            this.OutgoingRelationships = new Dictionary<CdmObjectDefinition, List<CdmE2ERelationship>>();
            this.IncomingRelationships = new Dictionary<CdmObjectDefinition, List<CdmE2ERelationship>>();
            this.resEntMap = new Dictionary<string, string>();

            this.documentLibrary = new DocumentLibrary();

            this.Ctx = new ResolveContext(this, null);
            this.Storage = new StorageManager(this);

            this.spinLock = new SpinLock(false);

            this.Persistence = new PersistenceLayer(this);

            // the default for the default is to make entity attributes into foreign key references when they point at one other instance and 
            // to ignore the other entities when there are an array of them
            this.DefaultResolutionDirectives = new AttributeResolutionDirectiveSet(new HashSet<string>() { "normalized", "referenceOnly" });
        }

        internal static int NextId()
        {
            _nextId++;
            return _nextId;
        }

        [Obsolete("Use FetchObjectAsync instead.")]
        public async Task<CdmManifestDefinition> CreateRootManifestAsync(string corpusPath)
        {
            if (this.IsPathManifestDocument(corpusPath))
            {
                this.rootManifest = await this.FetchObjectAsync<CdmManifestDefinition>(corpusPath);
                return this.rootManifest;
            }
            return null;
        }

        internal ResolvedTraitSet CreateEmptyResolvedTraitSet(ResolveOptions resOpt)
        {
            string key = string.Empty;
            if (resOpt != null)
            {
                if (resOpt.WrtDoc != null)
                    key = resOpt.WrtDoc.Id.ToString();
                key += "-";
                if (resOpt.Directives != null)
                    key += resOpt.Directives.GetTag();
            }
            this.EmptyRts.TryGetValue(key, out ResolvedTraitSet rts);
            if (rts == null)
            {
                rts = new ResolvedTraitSet(resOpt);
                this.EmptyRts[key] = rts;
            }
            return rts;
        }

        private void RegisterSymbol(string symbol, CdmDocumentDefinition inDoc)
        {
            this.SymbolDefinitions.TryGetValue(symbol, out List<CdmDocumentDefinition> docs);
            if (docs == null)
            {
                docs = new List<CdmDocumentDefinition>();
                this.SymbolDefinitions[symbol] = docs;
            }
            docs.Add(inDoc);
        }

        private void UnRegisterSymbol(string symbol, CdmDocumentDefinition inDoc)
        {
            this.SymbolDefinitions.TryGetValue(symbol, out List<CdmDocumentDefinition> docs);
            if (docs != null)
            {
                // if the symbol is listed for the given doc, remove it
                int index = docs.IndexOf(inDoc);
                if (index != -1)
                {
                    docs.RemoveAt(index);
                }
            }
        }

        internal DocsResult DocsForSymbol(ResolveOptions resOpt, CdmDocumentDefinition wrtDoc, CdmDocumentDefinition fromDoc, string symbol)
        {
            ResolveContext ctx = this.Ctx as ResolveContext;
            DocsResult result = new DocsResult
            {
                NewSymbol = symbol
            };

            // first decision, is the symbol defined anywhere?
            this.SymbolDefinitions.TryGetValue(symbol, out List<CdmDocumentDefinition> docList);
            result.DocList = docList;
            if (result.DocList == null || result.DocList.Count == 0)
            {
                // this can happen when the symbol is disambiguated with a moniker for one of the imports used 
                // in this situation, the 'wrt' needs to be ignored, the document where the reference is being made has a map of the 'one best' monikered import to search for each moniker
                int preEnd = symbol.IndexOf("/");
                if (preEnd == 0)
                {
                    // absolute reference
                    Logger.Error(nameof(CdmCorpusDefinition), ctx, "no support for absolute references yet. fix '" + symbol + "'", ctx.RelativePath);
                    return null;
                }
                if (preEnd > 0)
                {
                    string prefix = StringUtils.Slice(symbol, 0, preEnd);
                    result.NewSymbol = StringUtils.Slice(symbol, preEnd + 1);
                    this.SymbolDefinitions.TryGetValue(result.NewSymbol, out List<CdmDocumentDefinition> tempDocList);
                    result.DocList = tempDocList;

                    CdmDocumentDefinition tempMonikerDoc = null;
                    bool usingWrtDoc = false;
                    if (fromDoc?.ImportPriorities?.MonikerPriorityMap?.ContainsKey(prefix) == true)
                    {
                        fromDoc.ImportPriorities.MonikerPriorityMap.TryGetValue(prefix, out tempMonikerDoc);
                    }
                    else if (wrtDoc.ImportPriorities?.MonikerPriorityMap?.ContainsKey(prefix) == true)
                    {
                        // if that didn't work, then see if the wrtDoc can find the moniker
                        wrtDoc.ImportPriorities.MonikerPriorityMap.TryGetValue(prefix, out tempMonikerDoc);
                        usingWrtDoc = true;
                    }

                    if (tempMonikerDoc != null)
                    {
                        // if more monikers, keep looking
                        if (result.NewSymbol.IndexOf("/") >= 0 && (usingWrtDoc || !this.SymbolDefinitions.ContainsKey(result.NewSymbol)))
                        {
                            DocsResult currDocsResult = this.DocsForSymbol(resOpt, wrtDoc, tempMonikerDoc, result.NewSymbol);
                            if (currDocsResult.DocList == null && fromDoc == wrtDoc)
                            {
                                // we are back at the top and we have not found the docs, move the wrtDoc down one level
                                return this.DocsForSymbol(resOpt, tempMonikerDoc, tempMonikerDoc, result.NewSymbol);
                            }
                            else
                            {
                                return currDocsResult;
                            }
                        }
                        resOpt.FromMoniker = prefix;
                        result.DocBest = tempMonikerDoc;
                    }
                    else
                    {
                        // moniker not recognized in either doc, fail with grace
                        result.NewSymbol = symbol;
                        result.DocList = null;
                    }
                }
            }
            return result;
        }

        internal CdmObjectBase ResolveSymbolReference(ResolveOptions resOpt, CdmDocumentDefinition fromDoc, string symbolDef, CdmObjectType expectedType, bool retry)
        {
            ResolveContext ctx = this.Ctx as ResolveContext;

            // given a symbolic name, find the 'highest prirority' definition of the object from the point of view of a given document (with respect to, wrtDoc)
            // (meaning given a document and the things it defines and the files it imports and the files they import, where is the 'last' definition found)
            if (resOpt?.WrtDoc == null)
                return null; // no way to figure this out
            CdmDocumentDefinition wrtDoc = resOpt.WrtDoc as CdmDocumentDefinition;

            var indexTask = wrtDoc.IndexIfNeeded(resOpt, true);
            indexTask.Wait();
            // if the wrtDoc needs to be indexed (like it was just modified) then do that first
            if (!indexTask.Result)
            {
                Logger.Error(nameof(CdmEntityDefinition), this.Ctx as ResolveContext, "Couldn't index source document.", nameof(ResolveSymbolReference));
                return null;
            }

            // get the array of documents where the symbol is defined
            DocsResult symbolDocsResult = this.DocsForSymbol(resOpt, wrtDoc, fromDoc, symbolDef);
            CdmDocumentDefinition docBest = symbolDocsResult.DocBest;
            symbolDef = symbolDocsResult.NewSymbol;
            List<CdmDocumentDefinition> docs = symbolDocsResult.DocList;
            if (docs != null)
            {
                // add this symbol to the set being collected in resOpt, we will need this when caching
                if (resOpt.SymbolRefSet == null)
                {
                    resOpt.SymbolRefSet = new SymbolSet();
                }

                resOpt.SymbolRefSet.Add(symbolDef);
                // for the given doc, there is a sorted list of imported docs (including the doc itself as item 0).
                // find the lowest number imported document that has a definition for this symbol
                if (wrtDoc.ImportPriorities == null)
                {
                    return null;
                }

                IDictionary<CdmDocumentDefinition, int> importPriority = wrtDoc.ImportPriorities.ImportPriority;
                if (importPriority.Count == 0)
                {
                    return null;
                }


                if (docBest == null)
                {
                    docBest = FetchPriorityDocument(docs, importPriority);
                }
            }

            // perhaps we have never heard of this symbol in the imports for this document?
            if (docBest == null)
                return null;

            // return the definition found in the best document
            docBest.InternalDeclarations.TryGetValue(symbolDef, out CdmObjectBase found);
            if (found == null && retry == true)
            {
                // maybe just locatable from here not defined here.
                // this happens when the symbol is monikered, but the moniker path doesn't lead to the document where the symbol is defined.
                // it leads to the document from where the symbol can be found. 
                // Ex.: resolvedFrom/Owner, while resolvedFrom is the Account that imports Owner.
                found = this.ResolveSymbolReference(resOpt, docBest, symbolDef, expectedType, retry: false);
            }

            if (found != null && expectedType != CdmObjectType.Error)
            {
                switch (expectedType)
                {
                    case CdmObjectType.TraitRef:
                        if (found.ObjectType != CdmObjectType.TraitDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type trait", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.DataTypeRef:
                        if (found.ObjectType != CdmObjectType.DataTypeDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type dataType", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.EntityRef:
                        if (found.ObjectType != CdmObjectType.EntityDef && found.ObjectType != CdmObjectType.ProjectionDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type entity or type projection", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.ParameterDef:
                        if (found.ObjectType != CdmObjectType.ParameterDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type parameter", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.PurposeRef:
                        if (found.ObjectType != CdmObjectType.PurposeDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type purpose", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.AttributeGroupRef:
                        if (found.ObjectType != CdmObjectType.AttributeGroupDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type attributeGroup", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.ProjectionDef:
                        if (found.ObjectType != CdmObjectType.ProjectionDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type projection", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.OperationAddCountAttributeDef:
                        if (found.ObjectType != CdmObjectType.OperationAddCountAttributeDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type add count attribute operation", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.OperationAddSupportingAttributeDef:
                        if (found.ObjectType != CdmObjectType.OperationAddSupportingAttributeDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type add supporting attribute operation", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.OperationAddTypeAttributeDef:
                        if (found.ObjectType != CdmObjectType.OperationAddTypeAttributeDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type add type attribute operation", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.OperationExcludeAttributesDef:
                        if (found.ObjectType != CdmObjectType.OperationExcludeAttributesDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type exclude attributes operation", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.OperationArrayExpansionDef:
                        if (found.ObjectType != CdmObjectType.OperationArrayExpansionDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type array expansion operation", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.OperationCombineAttributesDef:
                        if (found.ObjectType != CdmObjectType.OperationCombineAttributesDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type combine attributes operation", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.OperationRenameAttributesDef:
                        if (found.ObjectType != CdmObjectType.OperationRenameAttributesDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type rename attributes operation", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.OperationReplaceAsForeignKeyDef:
                        if (found.ObjectType != CdmObjectType.OperationReplaceAsForeignKeyDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type replace as foreign key operation", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.OperationIncludeAttributesDef:
                        if (found.ObjectType != CdmObjectType.OperationIncludeAttributesDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type include attributes operation", symbolDef);
                            found = null;
                        }
                        break;
                    case CdmObjectType.OperationAddAttributeGroupDef:
                        if (found.ObjectType != CdmObjectType.OperationAddAttributeGroupDef)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, "expected type add attribute group operation", symbolDef);
                            found = null;
                        }
                        break;
                }
            }
            return found;
        }

        internal void RegisterDefinitionReferenceSymbols(CdmObject definition, string kind, SymbolSet symbolRefSet)
        {
            string key = CdmCorpusDefinition.CreateCacheKeyFromObject(definition, kind);
            this.DefinitionReferenceSymbols.TryGetValue(key, out SymbolSet existingSymbols);
            if (existingSymbols == null)
            {
                // nothing set, just use it
                this.DefinitionReferenceSymbols[key] = symbolRefSet;
            }
            else
            {
                // something there, need to merge
                existingSymbols.Merge(symbolRefSet);
            }
        }

        internal void UnRegisterDefinitionReferenceSymbols(CdmObject definition, string kind)
        {
            string key = CdmCorpusDefinition.CreateCacheKeyFromObject(definition, kind);
            this.DefinitionReferenceSymbols.Remove(key);
        }

        internal string CreateDefinitionCacheTag(ResolveOptions resOpt, CdmObjectBase definition, string kind, string extraTags = "", bool notKnownToHaveParameters = false, string pathToDef = null)
        {
            // construct a tag that is unique for a given object in a given context
            // context is: 
            //   (1) the wrtDoc has a set of imports and definitions that may change what the object is point at
            //   (2) there are different kinds of things stored per object (resolved traits, atts, etc.)
            //   (3) the directives from the resolve Options might matter
            //   (4) sometimes the caller needs different caches (extraTags) even give 1-3 are the same
            // the hardest part is (1). To do this, see if the object has a set of reference documents registered.
            // if there is nothing registered, then there is only one possible way to resolve the object so don't include doc info in the tag.
            // if there IS something registered, then the object could be ambiguous. find the 'index' of each of the ref documents (potential definition of something referenced under this scope)
            // in the wrt document's list of imports. sort the ref docs by their index, the relative ordering of found documents makes a unique context.
            // the hope is that many, many different lists of imported files will result in identical reference sortings, so lots of re-use
            // since this is an expensive operation, actually cache the sorted list associated with this object and wrtDoc

            // easy stuff first
            string thisId;
            string thisPath = (definition.ObjectType == CdmObjectType.ProjectionDef) ? definition.DeclaredPath.Replace("/", "") : definition.AtCorpusPath;
            if (!string.IsNullOrEmpty(pathToDef) && notKnownToHaveParameters)
            {
                thisId = pathToDef;
            }
            else
            {
                thisId = definition.Id.ToString();
            }

            StringBuilder tagSuffix = new StringBuilder();
            tagSuffix.AppendFormat("-{0}-{1}", kind, thisId);
            tagSuffix.AppendFormat("-({0})", resOpt.Directives != null ? resOpt.Directives.GetTag() : string.Empty);
            if (!string.IsNullOrEmpty(extraTags))
            {
                tagSuffix.AppendFormat("-{0}", extraTags);
            }

            // is there a registered set? (for the objectdef, not for a reference) of the many symbols involved in defining this thing (might be none)
            var objDef = definition.FetchObjectDefinition<CdmObjectDefinition>(resOpt);
            SymbolSet symbolsRef = null;
            if (objDef != null)
            {
                string key = CreateCacheKeyFromObject(objDef, kind);
                this.DefinitionReferenceSymbols.TryGetValue(key, out symbolsRef);
            }

            if (symbolsRef == null && thisPath != null)
            {
                // every symbol should depend on at least itself
                SymbolSet symSetThis = new SymbolSet
                {
                    thisPath
                };
                this.RegisterDefinitionReferenceSymbols(definition, kind, symSetThis);
                symbolsRef = symSetThis;
            }

            if (symbolsRef?.Size > 0)
            {
                // each symbol may have definitions in many documents. use importPriority to figure out which one we want
                CdmDocumentDefinition wrtDoc = resOpt.WrtDoc;
                HashSet<int> foundDocIds = new HashSet<int>();

                if (wrtDoc.ImportPriorities != null)
                {
                    foreach (string symRef in symbolsRef)
                    {
                        // get the set of docs where defined
                        DocsResult docsRes = this.DocsForSymbol(resOpt, wrtDoc, definition.InDocument, symRef);
                        // we only add the best doc if there are multiple options
                        if (docsRes?.DocList?.Count > 1)
                        {
                            CdmDocumentDefinition docBest = FetchPriorityDocument(docsRes.DocList, wrtDoc.ImportPriorities.ImportPriority);
                            if (docBest != null)
                            {
                                foundDocIds.Add(docBest.Id);
                            }
                        }
                    }
                }

                List<int> sortedList = foundDocIds.ToList();
                sortedList.Sort();
                string tagPre = string.Join("-", sortedList);

                return tagPre + tagSuffix;
            }
            return null;
        }

        /// <summary>
        /// Instantiates an OM class reference based on the object type passed as the first parameter.
        /// </summary>
        public T MakeRef<T>(CdmObjectType ofType, dynamic refObj, bool simpleNameRef) where T : CdmObjectReference
        {
            CdmObjectReference oRef = null;
            if (refObj != null)
            {
                if (refObj is CdmObject)
                {
                    if (refObj.ObjectType == ofType)
                    {
                        // forgive this mistake, return the ref passed in
                        oRef = (refObj as dynamic) as CdmObjectReference;
                    }
                    else
                    {
                        oRef = MakeObject<CdmObjectReference>(ofType, null, false);
                        oRef.ExplicitReference = refObj;
                    }
                }
                else
                {
                    // refObj is a string or JValue
                    oRef = MakeObject<CdmObjectReference>(ofType, (string)refObj, simpleNameRef);
                }
            }
            return (T)oRef;
        }

        /// <summary>
        /// Instantiates an OM class based on the object type passed as the first parameter.
        /// </summary>
        public T MakeObject<T>(CdmObjectType ofType, string nameOrRef = null, bool simpleNameRef = false) where T : CdmObject
        {
            CdmObject newObj = null;

            switch (ofType)
            {
                case CdmObjectType.ArgumentDef:
                    newObj = new CdmArgumentDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.AttributeContextDef:
                    newObj = new CdmAttributeContext(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.AttributeContextRef:
                    newObj = new CdmAttributeContextReference(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.AttributeGroupDef:
                    newObj = new CdmAttributeGroupDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.AttributeGroupRef:
                    newObj = new CdmAttributeGroupReference(this.Ctx, nameOrRef, simpleNameRef);
                    break;
                case CdmObjectType.AttributeRef:
                    newObj = new CdmAttributeReference(this.Ctx, nameOrRef, simpleNameRef);
                    break;
                case CdmObjectType.AttributeResolutionGuidanceDef:
                    newObj = new CdmAttributeResolutionGuidance(this.Ctx);
                    break;
                case CdmObjectType.ConstantEntityDef:
                    newObj = new CdmConstantEntityDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.DataPartitionDef:
                    newObj = new CdmDataPartitionDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.DataPartitionPatternDef:
                    newObj = new CdmDataPartitionPatternDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.DataTypeDef:
                    newObj = new CdmDataTypeDefinition(this.Ctx, nameOrRef, null);
                    break;
                case CdmObjectType.DataTypeRef:
                    newObj = new CdmDataTypeReference(this.Ctx, nameOrRef, simpleNameRef);
                    break;
                case CdmObjectType.DocumentDef:
                    newObj = new CdmDocumentDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.EntityAttributeDef:
                    newObj = new CdmEntityAttributeDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.EntityDef:
                    newObj = new CdmEntityDefinition(this.Ctx, nameOrRef, null);
                    break;
                case CdmObjectType.EntityRef:
                    newObj = new CdmEntityReference(this.Ctx, nameOrRef, simpleNameRef);
                    break;
                case CdmObjectType.FolderDef:
                    newObj = new CdmFolderDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.ManifestDef:
                    newObj = new CdmManifestDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.ManifestDeclarationDef:
                    newObj = new CdmManifestDeclarationDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.Import:
                    newObj = new CdmImport(this.Ctx, nameOrRef, null);
                    break;
                case CdmObjectType.LocalEntityDeclarationDef:
                    newObj = new CdmLocalEntityDeclarationDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.ParameterDef:
                    newObj = new CdmParameterDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.PurposeDef:
                    newObj = new CdmPurposeDefinition(this.Ctx, nameOrRef, null);
                    break;
                case CdmObjectType.PurposeRef:
                    newObj = new CdmPurposeReference(this.Ctx, nameOrRef, simpleNameRef);
                    break;
                case CdmObjectType.ReferencedEntityDeclarationDef:
                    newObj = new CdmReferencedEntityDeclarationDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.TraitDef:
                    newObj = new CdmTraitDefinition(this.Ctx, nameOrRef, null);
                    break;
                case CdmObjectType.TraitRef:
                    newObj = new CdmTraitReference(this.Ctx, nameOrRef, simpleNameRef, false);
                    break;
                case CdmObjectType.TypeAttributeDef:
                    newObj = new CdmTypeAttributeDefinition(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.E2ERelationshipDef:
                    newObj = new CdmE2ERelationship(this.Ctx, nameOrRef);
                    break;
                case CdmObjectType.ProjectionDef:
                    newObj = new CdmProjection(this.Ctx);
                    break;
                case CdmObjectType.OperationAddCountAttributeDef:
                    newObj = new CdmOperationAddCountAttribute(this.Ctx);
                    break;
                case CdmObjectType.OperationAddSupportingAttributeDef:
                    newObj = new CdmOperationAddSupportingAttribute(this.Ctx);
                    break;
                case CdmObjectType.OperationAddTypeAttributeDef:
                    newObj = new CdmOperationAddTypeAttribute(this.Ctx);
                    break;
                case CdmObjectType.OperationExcludeAttributesDef:
                    newObj = new CdmOperationExcludeAttributes(this.Ctx);
                    break;
                case CdmObjectType.OperationArrayExpansionDef:
                    newObj = new CdmOperationArrayExpansion(this.Ctx);
                    break;
                case CdmObjectType.OperationCombineAttributesDef:
                    newObj = new CdmOperationCombineAttributes(this.Ctx);
                    break;
                case CdmObjectType.OperationRenameAttributesDef:
                    newObj = new CdmOperationRenameAttributes(this.Ctx);
                    break;
                case CdmObjectType.OperationReplaceAsForeignKeyDef:
                    newObj = new CdmOperationReplaceAsForeignKey(this.Ctx);
                    break;
                case CdmObjectType.OperationIncludeAttributesDef:
                    newObj = new CdmOperationIncludeAttributes(this.Ctx);
                    break;
                case CdmObjectType.OperationAddAttributeGroupDef:
                    newObj = new CdmOperationAddAttributeGroup(this.Ctx);
                    break;
            }
            return (T)newObj;
        }

        internal static CdmObjectType MapReferenceType(CdmObjectType ofType)
        {
            switch (ofType)
            {
                case CdmObjectType.ArgumentDef:
                case CdmObjectType.DocumentDef:
                case CdmObjectType.ManifestDef:
                case CdmObjectType.Import:
                case CdmObjectType.ParameterDef:
                default:
                    return CdmObjectType.Error;

                case CdmObjectType.AttributeGroupRef:
                case CdmObjectType.AttributeGroupDef:
                    return CdmObjectType.AttributeGroupRef;

                case CdmObjectType.ConstantEntityDef:
                case CdmObjectType.EntityDef:
                case CdmObjectType.EntityRef:
                    return CdmObjectType.EntityRef;

                case CdmObjectType.DataTypeDef:
                case CdmObjectType.DataTypeRef:
                    return CdmObjectType.DataTypeRef;

                case CdmObjectType.PurposeDef:
                case CdmObjectType.PurposeRef:
                    return CdmObjectType.PurposeRef;

                case CdmObjectType.TraitDef:
                case CdmObjectType.TraitRef:
                    return CdmObjectType.TraitRef;

                case CdmObjectType.EntityAttributeDef:
                case CdmObjectType.TypeAttributeDef:
                case CdmObjectType.AttributeRef:
                    return CdmObjectType.AttributeRef;

                case CdmObjectType.AttributeContextDef:
                case CdmObjectType.AttributeContextRef:
                    return CdmObjectType.AttributeContextRef;
            }
        }

        internal static string CreateCacheKeyFromObject(CdmObject definition, string kind)
        {
            return definition.Id.ToString() + "-" + kind;
        }

        internal static CdmDocumentDefinition FetchPriorityDocument(List<CdmDocumentDefinition> docs, IDictionary<CdmDocumentDefinition, int> importPriority)
        {
            CdmDocumentDefinition docBest = null;
            int indexBest = int.MaxValue;
            foreach (CdmDocumentDefinition docDefined in docs)
            {
                // is this one of the imported docs?
                int indexFound = int.MaxValue;
                bool worked = importPriority.TryGetValue(docDefined, out indexFound);
                if (worked && indexFound < indexBest)
                {
                    indexBest = indexFound;
                    docBest = docDefined;
                    // hard to be better than the best
                    if (indexBest == 0)
                    {
                        break;
                    }
                }
            }
            return docBest;
        }

        internal CdmDocumentDefinition AddDocumentObjects(CdmFolderDefinition folder, CdmDocumentDefinition docDef)
        {
            var path = this.Storage.CreateAbsoluteCorpusPath(docDef.FolderPath + docDef.Name, docDef).ToLower();
            this.documentLibrary.AddDocumentPath(path, folder, docDef);

            return docDef;
        }

        internal void RemoveDocumentObjects(CdmFolderDefinition folder, CdmDocumentDefinition docDef)
        {
            CdmDocumentDefinition doc = docDef as CdmDocumentDefinition;

            // every symbol defined in this document is pointing at the document, so remove from cache.
            // also remove the list of docs that it depends on
            this.RemoveObjectDefinitions(doc);

            // remove from path lookup, folder lookup and global list of documents
            string path = this.Storage.CreateAbsoluteCorpusPath(doc.FolderPath + doc.Name, doc).ToLower();
            this.documentLibrary.RemoveDocumentPath(path, folder, doc);
        }

        internal bool IndexDocuments(ResolveOptions resOpt, bool strictValidation)
        {
            List<CdmDocumentDefinition> docsNotIndexed = this.documentLibrary.ListDocsNotIndexed();

            if (docsNotIndexed.Count == 0)
            {
                return true;
            }

            // clear caches.
            foreach (CdmDocumentDefinition doc in docsNotIndexed)
            {
                if (!doc.DeclarationsIndexed)
                {
                    Logger.Debug(nameof(CdmCorpusDefinition), this.Ctx, $"index start: {doc.AtCorpusPath}", nameof(this.IndexDocuments));
                    doc.ClearCaches();
                }
            }

            // check basic integrity.
            foreach (CdmDocumentDefinition doc in docsNotIndexed)
            {
                if (!doc.DeclarationsIndexed)
                {
                    doc.IsValid = true; // assume valid unless this fails
                    if (!this.CheckObjectIntegrity(doc))
                    {
                        doc.IsValid = false;
                    }
                }
            }

            // declare definitions in objects in this doc.
            foreach (CdmDocumentDefinition doc in docsNotIndexed)
            {
                if (!doc.DeclarationsIndexed && doc.IsValid)
                {
                    this.DeclareObjectDefinitions(doc, "");
                }
            }

            if (strictValidation)
            {
                // index any imports.
                foreach (CdmDocumentDefinition doc in docsNotIndexed)
                {
                    doc.GetImportPriorities();
                }

                // make sure we can find everything that is named by reference.
                foreach (CdmDocumentDefinition doc in docsNotIndexed)
                {
                    if (doc.IsValid)
                    {
                        ResolveOptions resOptLocal = CdmObjectBase.CopyResolveOptions(resOpt);
                        resOptLocal.WrtDoc = doc;
                        this.ResolveObjectDefinitions(resOptLocal, doc);
                    }
                }

                // now resolve any trait arguments that are type object.
                foreach (CdmDocumentDefinition doc in docsNotIndexed)
                {
                    if (doc.IsValid)
                    {
                        ResolveOptions resOptLocal = CdmObjectBase.CopyResolveOptions(resOpt);
                        resOptLocal.WrtDoc = doc;
                        this.ResolveTraitArguments(resOptLocal, doc);
                    }
                }
            }

            // finish up.
            foreach (CdmDocumentDefinition doc in docsNotIndexed)
            {                    
                Logger.Debug(nameof(CdmCorpusDefinition), this.Ctx, $"index finish: { doc.AtCorpusPath}", nameof(this.IndexDocuments));
                this.FinishDocumentResolve(doc, strictValidation);
            }

            return true;
        }

        internal async Task<CdmContainerDefinition> LoadFolderOrDocument(string objectPath, bool forceReload = false, ResolveOptions resOpt = null)
        {
            if (!string.IsNullOrWhiteSpace(objectPath))
            {
                // first check for namespace
                Tuple<string, string> pathTuple = StorageUtils.SplitNamespacePath(objectPath);
                if (pathTuple == null)
                {
                    Logger.Error(nameof(CdmCorpusDefinition), this.Ctx, "The object path cannot be null or empty.", nameof(LoadFolderOrDocument));
                    return null;
                }
                string nameSpace = !string.IsNullOrWhiteSpace(pathTuple.Item1) ? pathTuple.Item1 : this.Storage.DefaultNamespace;
                objectPath = pathTuple.Item2;

                if (objectPath.StartsWith("/"))
                {
                    var namespaceFolder = this.Storage.FetchRootFolder(nameSpace);
                    StorageAdapter namespaceAdapter = this.Storage.FetchAdapter(nameSpace);
                    if (namespaceFolder == null || namespaceAdapter == null)
                    {
                        Logger.Error(nameof(CdmCorpusDefinition), this.Ctx, "The namespace '" + nameSpace + "' has not been registered", $"LoadFolderOrDocument({objectPath})");
                        return null;
                    }
                    CdmFolderDefinition lastFolder = await namespaceFolder.FetchChildFolderFromPathAsync(objectPath, false);

                    // don't create new folders, just go as far as possible
                    if (lastFolder != null)
                    {
                        // maybe the search is for a folder?
                        string lastPath = lastFolder.FolderPath;
                        if (lastPath == objectPath)
                            return lastFolder;

                        // remove path to folder and then look in the folder
                        objectPath = StringUtils.Slice(objectPath, lastPath.Length);

                        return await lastFolder.FetchDocumentFromFolderPathAsync(objectPath, namespaceAdapter, forceReload, resOpt);
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Fetches an object by the path from the corpus.
        /// </summary>
        /// /// <typeparam name="T"> Type of the object to be fetched</typeparam>
        /// <param name="objectPath">Object path, absolute or relative.</param>
        /// <param name="obj">Optional parameter. When provided, it is used to obtain the FolderPath and the Namespace needed to create the absolute path from a relative path.</param>
        /// <param name="resOpt">Optional parameter. When provided, will use be used to determine how the symbols are resolved.</param>
        /// <param name="forceReload">Optional parameter. When true, the document containing the requested object is reloaded from storage to access any external changes made to the document since it may have been cached by the corpus.</param>
        /// <returns>The object obtained from the provided path.</returns>
        public async Task<T> FetchObjectAsync<T>(string objectPath, CdmObject obj = null, ResolveOptions resOpt = null, bool forceReload = false)
            where T: CdmObject
        {
            if (resOpt == null)
            {
                resOpt = new ResolveOptions();
            }

            // convert the object path to the absolute corpus path.
            objectPath = this.Storage.CreateAbsoluteCorpusPath(objectPath, obj);

            var documentPath = objectPath;
            var documentNameIndex = objectPath.LastIndexOf(PersistenceLayer.CdmExtension);

            if (documentNameIndex != -1)
            {
                // if there is something after the document path, split it into document path and object path.
                documentNameIndex += PersistenceLayer.CdmExtension.Count();
                documentPath = objectPath.Slice(0, documentNameIndex);
            }

            Logger.Debug(nameof(CdmCorpusDefinition), this.Ctx, $"request object: {objectPath}", nameof(this.FetchObjectAsync));
            CdmContainerDefinition newObj = await LoadFolderOrDocument(documentPath, forceReload);

            if (newObj != null)
            {
                // get imports and index each document that is loaded
                if (newObj is CdmDocumentDefinition doc)
                {
                    if (!await doc.IndexIfNeeded(resOpt))
                    {
                        return default;
                    }

                    if (!doc.IsValid)
                    {
                        Logger.Error(nameof(CdmCorpusDefinition), this.Ctx, $"The requested path: {objectPath} involves a document that failed validation", nameof(this.FetchObjectAsync));
                        return default;
                    }
                }

                if (documentPath.Equals(objectPath))
                {
                    return (T)newObj;
                }

                if (documentNameIndex == -1)
                {
                    // there is no remaining path to be loaded, so return.
                    return default;
                }

                // trim off the document path to get the object path in the doc
                var remainingObjectPath = objectPath.Slice(documentNameIndex + 1);

                var result = ((CdmDocumentDefinition)newObj).FetchObjectFromDocumentPath(remainingObjectPath, resOpt);
                if (result == null)
                {
                    Logger.Error(nameof(CdmCorpusDefinition), (ResolveContext)this.Ctx, $"Could not find symbol '{objectPath}' in document[{newObj.AtCorpusPath}]", nameof(FetchObjectAsync));
                }

                return (T)result;
            }

            return default;
        }

        /// <summary>
        /// Fetches an object by the path from the corpus.
        /// </summary>
        /// <typeparam name="T"> Type of the object to be fetched</typeparam>
        /// <param name="objectPath">Object path, absolute or relative.</param>
        /// <param name="obj">Optional parameter. When provided, it is used to obtain the FolderPath and the Namespace needed to create the absolute path from a relative path.</param>
        /// <param name="shallowValidation">Optional parameter. When provided, shallow validation in ResolveOptions is enabled, which logs errors regarding resolving/loading references as warnings.</param>
        /// <param name="forceReload">Optional parameter. When true, the document containing the requested object is reloaded from storage to access any external changes made to the document since it may have been cached by the corpus.</param>
        /// <returns>The object obtained from the provided path.</returns>
        public async Task<T> FetchObjectAsync<T>(string objectPath, CdmObject obj, bool shallowValidation, bool forceReload = false)
            where T : CdmObject
        {
            var resOpt = new ResolveOptions
            {
                ShallowValidation = shallowValidation
            };
            return await FetchObjectAsync<T>(objectPath, obj, resOpt, forceReload);
        }

        /// <summary>
        /// A callback that gets called on an event.
        /// </summary>
        public void SetEventCallback(EventCallback status, CdmStatusLevel reportAtLevel = CdmStatusLevel.Info)
        {
            ResolveContext ctx = this.Ctx as ResolveContext;
            ctx.StatusEvent = status;
            ctx.ReportAtLevel = reportAtLevel;
        }

        /// <summary>
        /// Find import objects for the document that have not been loaded yet
        /// </summary>
        internal void FindMissingImportsFromDocument(CdmDocumentDefinition doc)
        {
            if (doc.Imports != null)
            {
                foreach (var imp in doc.Imports)
                {
                    if (imp.Document == null)
                    {
                        // no document set for this import, see if it is already loaded into the corpus
                        string path = this.Storage.CreateAbsoluteCorpusPath(imp.CorpusPath, doc);
                        this.documentLibrary.AddToDocsNotLoaded(path);
                    }
                }
            }
        }

        /// <summary>
        /// Attach document objects to corresponding import object
        /// </summary>
        internal void SetImportDocuments(CdmDocumentDefinition doc)
        {
            if (doc.Imports != null)
            {
                foreach (var imp in doc.Imports)
                {
                    if (imp.Document == null)
                    {
                        // no document set for this import, see if it is already loaded into the corpus
                        var path = this.Storage.CreateAbsoluteCorpusPath(imp.CorpusPath, doc);
                        var impDoc = this.documentLibrary.FetchDocumentAndMarkForIndexing(path);
                        if (impDoc != null)
                        {
                            imp.Document = impDoc;

                            // repeat the process for the import documents
                            this.SetImportDocuments(imp.Document);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        internal async Task LoadImportsAsync(CdmDocumentDefinition doc, ResolveOptions resOpt)
        {
            var docsNowLoaded = new ConcurrentDictionary<CdmDocumentDefinition, byte>();
            List<string> docsNotLoaded = this.documentLibrary.ListDocsNotLoaded();

            if (docsNotLoaded.Count > 0)
            {
                async Task loadDocs(string missing)
                {
                    if (this.documentLibrary.NeedToLoadDocument(missing))
                    {
                        // load it
                        var newDoc = await this.LoadFolderOrDocument(missing, false, resOpt) as CdmDocumentDefinition;

                        if (this.documentLibrary.MarkDocumentAsLoadedOrFailed(newDoc, missing, docsNowLoaded))
                        {
                            Logger.Info(nameof(CdmCorpusDefinition), this.Ctx, $"resolved import for '{newDoc.Name}'", doc.AtCorpusPath);
                        }
                        else
                        {
                            Logger.Warning(nameof(CdmCorpusDefinition), this.Ctx, $"unable to resolve import for '{missing}'", doc.AtCorpusPath);
                        }
                    }
                }

                var taskList = new List<Task>();
                foreach (var missing in docsNotLoaded)
                {
                    taskList.Add(loadDocs(missing));
                }

                // wait for all of the missing docs to finish loading
                await Task.WhenAll(taskList);

                // now that we've loaded new docs, find imports from them that need loading
                foreach (var loadedDoc in docsNowLoaded.Keys)
                {
                    this.FindMissingImportsFromDocument(loadedDoc);
                }

                // repeat this process for the imports of the imports
                List<Task> importTaskList = new List<Task>();
                foreach (var loadedDoc in docsNowLoaded.Keys)
                {
                    importTaskList.Add(this.LoadImportsAsync(loadedDoc, resOpt));
                }

                await Task.WhenAll(importTaskList);
            }
        }

        /// <summary>
        /// Takes a callback that asks for a promise to do URI resolution.
        /// </summary>
        internal async Task ResolveImportsAsync(CdmDocumentDefinition doc, ResolveOptions resOpt)
        {
            // find imports for this doc
            this.FindMissingImportsFromDocument(doc);
            // load imports (and imports of imports)
            await this.LoadImportsAsync(doc, resOpt);
            // now that everything is loaded, attach import docs to this doc's import list
            this.SetImportDocuments(doc);
        }

        internal bool CheckObjectIntegrity(CdmDocumentDefinition CurrentDoc)
        {
            ResolveContext ctx = this.Ctx as ResolveContext;
            var errorCount = 0;
            VisitCallback preChildren = new VisitCallback
            {
                Invoke = (iObject, path) =>
                {
                    if (iObject.Validate() == false)
                    {
                        errorCount++;
                    }
                    else
                    {
                        (iObject as CdmObjectBase).Ctx = ctx;
                    }

                    Logger.Info(nameof(CdmCorpusDefinition), ctx, $"checked '{path}'", CurrentDoc.FolderPath + path);
                    return false;
                }
            };

            CurrentDoc.Visit(string.Empty, preChildren, null);

            return errorCount == 0;
        }

        internal void DeclareObjectDefinitions(CdmDocumentDefinition CurrentDoc, string relativePath)
        {
            ResolveContext ctx = this.Ctx as ResolveContext;
            string CorpusPathRoot = CurrentDoc.FolderPath + CurrentDoc.Name;
            CurrentDoc.Visit(relativePath, new VisitCallback
            {
                Invoke = (iObject, path) =>
                {
                    if (path.IndexOf("(unspecified)") > 0)
                        return true;
                    bool skipDuplicates = false;
                    switch (iObject.ObjectType)
                    {
                        case CdmObjectType.AttributeGroupRef:
                        case CdmObjectType.AttributeContextRef:
                        case CdmObjectType.DataTypeRef:
                        case CdmObjectType.EntityRef:
                        case CdmObjectType.PurposeRef:
                        case CdmObjectType.TraitRef:
                        case CdmObjectType.AttributeGroupDef:
                        case CdmObjectType.EntityDef:
                        case CdmObjectType.ParameterDef:
                        case CdmObjectType.TraitDef:
                        case CdmObjectType.PurposeDef:
                        case CdmObjectType.AttributeContextDef:
                        case CdmObjectType.DataTypeDef:
                        case CdmObjectType.TypeAttributeDef:
                        case CdmObjectType.EntityAttributeDef:
                        case CdmObjectType.ConstantEntityDef:
                        case CdmObjectType.LocalEntityDeclarationDef:
                        case CdmObjectType.ReferencedEntityDeclarationDef:
                        case CdmObjectType.ProjectionDef:
                        case CdmObjectType.OperationAddCountAttributeDef:
                        case CdmObjectType.OperationAddSupportingAttributeDef:
                        case CdmObjectType.OperationAddTypeAttributeDef:
                        case CdmObjectType.OperationExcludeAttributesDef:
                        case CdmObjectType.OperationArrayExpansionDef:
                        case CdmObjectType.OperationCombineAttributesDef:
                        case CdmObjectType.OperationRenameAttributesDef:
                        case CdmObjectType.OperationReplaceAsForeignKeyDef:
                        case CdmObjectType.OperationIncludeAttributesDef:
                        case CdmObjectType.OperationAddAttributeGroupDef:
                            if (iObject.ObjectType == CdmObjectType.AttributeGroupRef || iObject.ObjectType == CdmObjectType.AttributeContextRef
                            || iObject.ObjectType == CdmObjectType.DataTypeRef || iObject.ObjectType == CdmObjectType.EntityRef
                            || iObject.ObjectType == CdmObjectType.PurposeRef || iObject.ObjectType == CdmObjectType.TraitRef
                            || iObject.ObjectType == CdmObjectType.ConstantEntityDef)
                            {
                                // these are all references
                                // we will now allow looking up a reference object based on path, so they get indexed too
                                // if there is a duplicate, don't complain, the path just finds the first one
                                skipDuplicates = true;
                            }
                            ctx.RelativePath = relativePath;
                            string corpusPath = CorpusPathRoot + '/' + path;
                            if (CurrentDoc.InternalDeclarations.ContainsKey(path) && !skipDuplicates)
                            {
                                Logger.Error(nameof(CdmCorpusDefinition), ctx, $"duplicate declaration for item '{path}'", corpusPath);
                                return false;
                            }
                            else
                            {
                                CurrentDoc.InternalDeclarations.TryAdd(path, iObject as CdmObjectBase);
                                this.RegisterSymbol(path, CurrentDoc);

                                Logger.Info(nameof(CdmCorpusDefinition), ctx, $"declared '{path}'", corpusPath);
                            }
                            break;
                    }

                    return false;
                }
            }, null);
        }

        private void RemoveObjectDefinitions(CdmDocumentDefinition doc)
        {
            ResolveContext ctx = this.Ctx as ResolveContext;
            doc.Visit(string.Empty, new VisitCallback
            {
                Invoke = (CdmObject iObject, string path) =>
                {
                    if (path.IndexOf("(unspecified") > 0)
                        return true;
                    switch (iObject.ObjectType)
                    {
                        case CdmObjectType.EntityDef:
                        case CdmObjectType.ParameterDef:
                        case CdmObjectType.TraitDef:
                        case CdmObjectType.PurposeDef:
                        case CdmObjectType.DataTypeDef:
                        case CdmObjectType.TypeAttributeDef:
                        case CdmObjectType.EntityAttributeDef:
                        case CdmObjectType.AttributeGroupDef:
                        case CdmObjectType.ConstantEntityDef:
                        case CdmObjectType.AttributeContextDef:
                        case CdmObjectType.LocalEntityDeclarationDef:
                        case CdmObjectType.ReferencedEntityDeclarationDef:
                        case CdmObjectType.ProjectionDef:
                        case CdmObjectType.OperationAddCountAttributeDef:
                        case CdmObjectType.OperationAddSupportingAttributeDef:
                        case CdmObjectType.OperationAddTypeAttributeDef:
                        case CdmObjectType.OperationExcludeAttributesDef:
                        case CdmObjectType.OperationArrayExpansionDef:
                        case CdmObjectType.OperationCombineAttributesDef:
                        case CdmObjectType.OperationRenameAttributesDef:
                        case CdmObjectType.OperationReplaceAsForeignKeyDef:
                        case CdmObjectType.OperationIncludeAttributesDef:
                        case CdmObjectType.OperationAddAttributeGroupDef:
                            this.UnRegisterSymbol(path, doc);
                            this.UnRegisterDefinitionReferenceSymbols(iObject as CdmObjectBase, "rasb");
                            break;
                    }
                    return false;
                }
            }, null);
        }

        internal dynamic ConstTypeCheck(ResolveOptions resOpt, CdmDocumentDefinition CurrentDoc, CdmParameterDefinition paramDef, dynamic aValue)
        {
            ResolveContext ctx = this.Ctx as ResolveContext;
            dynamic replacement = aValue;
            // if parameter type is entity, then the value should be an entity or ref to one
            // same is true of 'dataType' dataType
            if (paramDef.DataTypeRef != null)
            {
                CdmDataTypeDefinition dt = paramDef.DataTypeRef.FetchObjectDefinition<CdmDataTypeDefinition>(resOpt);
                if (dt == null)
                {
                    dt = paramDef.DataTypeRef.FetchObjectDefinition<CdmDataTypeDefinition>(resOpt);
                    Logger.Error(nameof(CdmCorpusDefinition), ctx, $"parameter '${paramDef.Name}' has an unrecognized dataType.", ctx.RelativePath);
                    return null;
                }
                // compare with passed in value or default for parameter
                dynamic pValue = aValue;
                if (pValue == null)
                {
                    pValue = paramDef.DefaultValue;
                    replacement = pValue;
                }
                if (pValue != null)
                {
                    if (dt.IsDerivedFrom("cdmObject", resOpt))
                    {
                        List<CdmObjectType> expectedTypes = new List<CdmObjectType>();
                        string expected = null;
                        if (dt.IsDerivedFrom("entity", resOpt))
                        {
                            expectedTypes.Add(CdmObjectType.ConstantEntityDef);
                            expectedTypes.Add(CdmObjectType.EntityRef);
                            expectedTypes.Add(CdmObjectType.EntityDef);
                            expectedTypes.Add(CdmObjectType.ProjectionDef);
                            expected = "entity";
                        }
                        else if (dt.IsDerivedFrom("attribute", resOpt))
                        {
                            expectedTypes.Add(CdmObjectType.AttributeRef);
                            expectedTypes.Add(CdmObjectType.TypeAttributeDef);
                            expectedTypes.Add(CdmObjectType.EntityAttributeDef);
                            expected = "attribute";
                        }
                        else if (dt.IsDerivedFrom("dataType", resOpt))
                        {
                            expectedTypes.Add(CdmObjectType.DataTypeRef);
                            expectedTypes.Add(CdmObjectType.DataTypeDef);
                            expected = "dataType";
                        }
                        else if (dt.IsDerivedFrom("purpose", resOpt))
                        {
                            expectedTypes.Add(CdmObjectType.PurposeRef);
                            expectedTypes.Add(CdmObjectType.PurposeDef);
                            expected = "purpose";
                        }
                        else if (dt.IsDerivedFrom("trait", resOpt))
                        {
                            expectedTypes.Add(CdmObjectType.TraitRef);
                            expectedTypes.Add(CdmObjectType.TraitDef);
                            expected = "trait";
                        }
                        else if (dt.IsDerivedFrom("attributeGroup", resOpt))
                        {
                            expectedTypes.Add(CdmObjectType.AttributeGroupRef);
                            expectedTypes.Add(CdmObjectType.AttributeGroupDef);
                            expected = "attributeGroup";
                        }

                        if (expectedTypes.Count == 0)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, $"parameter '${paramDef.Name}' has an unexpected dataType.", ctx.RelativePath);
                        }

                        // if a string constant, resolve to an object ref.
                        CdmObjectType foundType = CdmObjectType.Error;
                        Type pValueType = pValue.GetType();

                        if (typeof(CdmObject).IsAssignableFrom(pValueType))
                            foundType = (pValue as CdmObject).ObjectType;
                        string foundDesc = ctx.RelativePath;
                        if (!(pValue is CdmObject))
                        {
                            // pValue is a string or JValue 
                            pValue = (string)pValue;
                            if (pValue == "this.attribute" && expected == "attribute")
                            {
                                // will get sorted out later when resolving traits
                                foundType = CdmObjectType.AttributeRef;
                            }
                            else
                            {
                                foundDesc = pValue;
                                int seekResAtt = CdmObjectReferenceBase.offsetAttributePromise(pValue);
                                if (seekResAtt >= 0)
                                {
                                    // get an object there that will get resolved later after resolved attributes
                                    replacement = new CdmAttributeReference(ctx, pValue, true);
                                    (replacement as CdmAttributeReference).Ctx = ctx;
                                    (replacement as CdmAttributeReference).InDocument = CurrentDoc;
                                    foundType = CdmObjectType.AttributeRef;
                                }
                                else
                                {
                                    CdmObjectBase lu = ctx.Corpus.ResolveSymbolReference(resOpt, CurrentDoc, pValue, CdmObjectType.Error, retry: true);
                                    if (lu != null)
                                    {
                                        if (expected == "attribute")
                                        {
                                            replacement = new CdmAttributeReference(ctx, pValue, true);
                                            (replacement as CdmAttributeReference).Ctx = ctx;
                                            (replacement as CdmAttributeReference).InDocument = CurrentDoc;
                                            foundType = CdmObjectType.AttributeRef;
                                        }
                                        else
                                        {
                                            replacement = lu;
                                            foundType = (replacement as CdmObject).ObjectType;
                                        }
                                    }
                                }
                            }
                        }
                        if (expectedTypes.IndexOf(foundType) == -1)
                        {
                            Logger.Error(nameof(CdmCorpusDefinition), ctx, $"parameter '{paramDef.Name}' has the dataType of '{expected}' but the value '{foundDesc}' doesn't resolve to a known {expected} referenece", CurrentDoc.FolderPath + ctx.RelativePath);
                        }
                        else
                        {
                            Logger.Info(nameof(CdmCorpusDefinition), ctx, $"    resolved '{foundDesc}'", ctx.RelativePath);
                        }
                    }
                }
            }
            return replacement;
        }

        internal void ResolveObjectDefinitions(ResolveOptions resOpt, CdmDocumentDefinition CurrentDoc)
        {
            ResolveContext ctx = this.Ctx as ResolveContext;
            resOpt.IndexingDoc = CurrentDoc;

            CurrentDoc.Visit(string.Empty, new VisitCallback
            {
                Invoke = (iObject, path) =>
                {
                    CdmObjectType ot = iObject.ObjectType;
                    switch (ot)
                    {
                        case CdmObjectType.AttributeRef:
                        case CdmObjectType.AttributeGroupRef:
                        case CdmObjectType.AttributeContextRef:
                        case CdmObjectType.DataTypeRef:
                        case CdmObjectType.EntityRef:
                        case CdmObjectType.PurposeRef:
                        case CdmObjectType.TraitRef:
                            ctx.RelativePath = path;
                            CdmObjectReferenceBase reff = iObject as CdmObjectReferenceBase;

                            if (CdmObjectReferenceBase.offsetAttributePromise(reff.NamedReference) < 0)
                            {
                                CdmObjectDefinition resNew = reff.FetchObjectDefinition<CdmObjectDefinition>(resOpt);

                                if (resNew == null)
                                {
                                    string message = $"Unable to resolve the reference '{reff.NamedReference}' to a known object";
                                    string messagePath = $"{CurrentDoc.FolderPath}{path}";

                                    // It's okay if references can't be resolved when shallow validation is enabled.
                                    if (resOpt.ShallowValidation)
                                    {
                                        Logger.Warning(nameof(CdmCorpusDefinition), ctx, message, messagePath);
                                    }
                                    else
                                    {
                                        Logger.Error(nameof(CdmCorpusDefinition), ctx, message, messagePath);
                                    }
                                    // don't check in this file without both of these comments. handy for debug of failed lookups
                                    // CdmObjectDefinitionBase resTest = ref.FetchObjectDefinition(resOpt);
                                }
                                else
                                {
                                    Logger.Info(nameof(CdmCorpusDefinition), ctx, $"    resolved '{reff.NamedReference}'", $"{CurrentDoc.FolderPath}{path}");
                                }
                            }
                            break;
                    }
                    return false;
                }
            }, new VisitCallback
            {
                Invoke = (iObject, path) =>
                {
                    CdmObjectType ot = iObject.ObjectType;
                    switch (ot)
                    {
                        case CdmObjectType.ParameterDef:
                            // when a parameter has a datatype that is a cdm object, validate that any default value is the
                            // right kind object
                            CdmParameterDefinition p = iObject as CdmParameterDefinition;
                            this.ConstTypeCheck(resOpt, CurrentDoc, p, null);
                            break;
                    }
                    return false;
                }
            });
            resOpt.IndexingDoc = null;
        }

        internal void ResolveTraitArguments(ResolveOptions resOpt, CdmDocumentDefinition CurrentDoc)
        {
            ResolveContext ctx = this.Ctx as ResolveContext;
            CurrentDoc.Visit(string.Empty, new VisitCallback
            {
                Invoke = (iObject, path) =>
                {
                    CdmObjectType ot = iObject.ObjectType;
                    switch (ot)
                    {
                        case CdmObjectType.TraitRef:
                            ctx.PushScope(iObject.FetchObjectDefinition<CdmTraitDefinition>(resOpt));
                            break;
                        case CdmObjectType.ArgumentDef:
                            try
                            {
                                if (ctx.CurrentScope.CurrentTrait != null)
                                {
                                    ctx.RelativePath = path;
                                    ParameterCollection paramCollection = ctx.CurrentScope.CurrentTrait.FetchAllParameters(resOpt);
                                    CdmParameterDefinition paramFound = null;
                                    dynamic aValue;
                                    paramFound = paramCollection.ResolveParameter(ctx.CurrentScope.CurrentParameter, (iObject as CdmArgumentDefinition).Name);
                                    (iObject as CdmArgumentDefinition).ResolvedParameter = paramFound;
                                    aValue = (iObject as CdmArgumentDefinition).Value;

                                    // if parameter type is entity, then the value should be an entity or ref to one
                                    // same is true of 'dataType' dataType
                                    aValue = this.ConstTypeCheck(resOpt, CurrentDoc, paramFound, aValue);
                                    if (aValue != null)
                                    {
                                        (iObject as CdmArgumentDefinition).Value = aValue;
                                    }
                                            
                                }
                            }
                            catch (Exception e)
                            {
                                Logger.Error(nameof(CdmCorpusDefinition), ctx, e.ToString(), path);
                                Logger.Error(nameof(CdmCorpusDefinition), ctx, $"failed to resolve parameter on trait '{ctx.CurrentScope.CurrentTrait?.GetName()}'", CurrentDoc.FolderPath + path);
                            }
                            ctx.CurrentScope.CurrentParameter++;
                            break;
                    }
                    return false;
                }
            }, new VisitCallback
            {
                Invoke = (iObject, path) =>
                {
                    CdmObjectType ot = iObject.ObjectType;
                    switch (ot)
                    {
                        case CdmObjectType.TraitRef:
                            (iObject as CdmTraitReference).ResolvedArguments = true;
                            ctx.PopScope();
                            break;
                    }
                    return false;
                }
            });
            return;
        }

        internal void FinishDocumentResolve(CdmDocumentDefinition doc, bool strictValidation)
        {
            bool wasIndexedPreviously = doc.DeclarationsIndexed;

            doc.CurrentlyIndexing = false;
            doc.ImportsIndexed = doc.ImportsIndexed || !strictValidation;
            doc.DeclarationsIndexed = true;
            doc.NeedsIndexing = !strictValidation;
            this.documentLibrary.MarkDocumentAsIndexed(doc);

            // if the document declarations were indexed previously, do not log again.
            if (!wasIndexedPreviously && doc.IsValid)
            {
                doc.Definitions.AllItems.ForEach(def =>
                {
                    if (def.ObjectType == CdmObjectType.EntityDef)
                    {
                        Logger.Debug(nameof(CdmCorpusDefinition), this.Ctx, $"indexed entity: {def.AtCorpusPath}");
                    }
                });
            }
        }

        internal void FinishResolve()
        {
            ResolveContext ctx = this.Ctx as ResolveContext;
            ////////////////////////////////////////////////////////////////////////////////////////////////////
            //  cleanup references
            ////////////////////////////////////////////////////////////////////////////////////////////////////
            Logger.Debug(nameof(CdmCorpusDefinition), ctx, "finishing...");
            // turn elevated traits back on, they are off by default and should work fully now that everything is resolved
            List<CdmDocumentDefinition> AllDocuments = this.documentLibrary.ListAllDocuments();
            int l = AllDocuments.Count;
            for (int i = 0; i < l; i++)
            {
                this.FinishDocumentResolve(AllDocuments[i], false);
            }
        }

        private bool IsPathManifestDocument(string path)
        {
            return (path.EndsWith(PersistenceLayer.ManifestExtension)) || path.EndsWith(PersistenceLayer.ModelJsonExtension)
                || path.EndsWith(PersistenceLayer.FolioExtension) || path.EndsWith(PersistenceLayer.OdiExtension);
        }

        /// <summary>
        /// Returns a list of relationships where the input entity is the incoming entity.
        /// <param name="entity"> The entity that we want to get relationships for.</param>
        public List<CdmE2ERelationship> FetchIncomingRelationships(CdmEntityDefinition entity)
        {
            if (this.IncomingRelationships != null && this.IncomingRelationships.ContainsKey(entity))
                return this.IncomingRelationships[entity];
            return new List<CdmE2ERelationship>();
        }

        /// <summary>
        /// Returns a list of relationships where the input entity is the outgoing entity.
        /// <param name="entity"> The entity that we want to get relationships for.</param>
        public List<CdmE2ERelationship> FetchOutgoingRelationships(CdmEntityDefinition entity)
        {
            if (this.OutgoingRelationships != null && this.OutgoingRelationships.ContainsKey(entity))
                return this.OutgoingRelationships[entity];
            return new List<CdmE2ERelationship>();
        }

        /// <summary>
        /// Calculates the entity to entity relationships for all the entities present in the manifest and its sub-manifests.
        /// </summary>
        /// <param name="currManifest">The manifest (and any sub-manifests it contains) that we want to calculate relationships for.</param>
        /// <returns>A <see cref="Task"/> for the completion of entity graph calculation.</returns>
        public async Task CalculateEntityGraphAsync(CdmManifestDefinition currManifest)
        {
            if (currManifest.Entities != null)
            {
                foreach (var entityDec in currManifest.Entities)
                {
                    var entityPath = await currManifest.GetEntityPathFromDeclaration(entityDec, currManifest);
                    // the path returned by GetEntityPathFromDeclaration is an absolute path.
                    // no need to pass the manifest to FetchObjectAsync.
                    var entity = await this.FetchObjectAsync<CdmEntityDefinition>(entityPath);

                    if (entity == null)
                        continue;

                    CdmEntityDefinition resEntity;
                    // make options wrt this entity document and "relational" always
                    var resOpt = new ResolveOptions(entity.InDocument, new AttributeResolutionDirectiveSet(new HashSet<string>() { "normalized", "referenceOnly" }));

                    bool isResolvedEntity = entity.AttributeContext != null;

                    // only create a resolved entity if the entity passed in was not a resolved entity
                    if (!isResolvedEntity)
                    {
                        // first get the resolved entity so that all of the references are present
                        resEntity = await entity.CreateResolvedEntityAsync($"wrtSelf_{entity.EntityName}", resOpt);
                    }
                    else
                    {
                        resEntity = entity;
                    }

                    // find outgoing entity relationships using attribute context
                    List<CdmE2ERelationship> outgoingRelationships = this.FindOutgoingRelationships(
                        resOpt,
                        resEntity,
                        resEntity.AttributeContext,
                        isResolvedEntity);

                    this.OutgoingRelationships[entity] = outgoingRelationships;

                    // flip outgoing entity relationships list to get incoming relationships map
                    if (outgoingRelationships != null)
                    {
                        foreach (CdmE2ERelationship rel in outgoingRelationships)
                        {
                            var targetEnt = await this.FetchObjectAsync<CdmEntityDefinition>(rel.ToEntity, currManifest);
                            if (targetEnt != null)
                            {
                                if (!this.IncomingRelationships.ContainsKey(targetEnt))
                                    this.IncomingRelationships[targetEnt] = new List<CdmE2ERelationship>();

                                this.IncomingRelationships[targetEnt].Add(rel);
                            }
                        }
                    }

                    // delete the resolved entity if we created one here
                    if (!isResolvedEntity)
                        resEntity.InDocument.Folder.Documents.Remove(resEntity.InDocument.Name);

                }

                if (currManifest.SubManifests != null)
                {
                    foreach (CdmManifestDeclarationDefinition subManifestDef in currManifest.SubManifests)
                    {
                        var corpusPath = this.Storage.CreateAbsoluteCorpusPath(subManifestDef.Definition, currManifest);
                        var subManifest = await this.FetchObjectAsync<CdmManifestDefinition>(corpusPath);
                        if (subManifest != null)
                        {
                            await this.CalculateEntityGraphAsync(subManifest);
                        }
                    }
                }
            }
        }

        internal List<CdmE2ERelationship> FindOutgoingRelationships(
            ResolveOptions resOpt,
            CdmEntityDefinition resEntity,
            CdmAttributeContext attCtx,
            bool isResolvedEntity,
            CdmAttributeContext generatedAttSetContext = null,
            bool wasProjectionPolymorphic = false,
            bool wasEntityRef = false,
            List<CdmAttributeReference> fromAtts = null)
        {
            List<CdmE2ERelationship> outRels = new List<CdmE2ERelationship>();

            if (attCtx?.Contents != null)
            {
                // as we traverse the context tree, look for these nodes which hold the foreign key
                // once we find a context node that refers to an entity reference, we will use the
                // nearest _generatedAttributeSet (which is above or at the same level as the entRef context)
                // and use its foreign key
                CdmAttributeContext newGenSet = (CdmAttributeContext)attCtx.Contents.Item("_generatedAttributeSet");
                if (newGenSet == null)
                    newGenSet = generatedAttSetContext;

                bool isEntityRef = false;
                bool isPolymorphicSource = false;
                foreach (dynamic subAttCtx in attCtx.Contents)
                {
                    if (subAttCtx.ObjectType == CdmObjectType.AttributeContextDef)
                    {
                        // find entity references that identifies the 'this' entity
                        var child = subAttCtx as CdmAttributeContext;
                        if (child?.Definition?.ObjectType == CdmObjectType.EntityRef)
                        {
                            CdmObjectDefinition toEntity = child.Definition.FetchObjectDefinition<CdmObjectDefinition>(resOpt);

                            if (toEntity?.ObjectType == CdmObjectType.ProjectionDef)
                            {
                                // Projections

                                isEntityRef = false;
                                isPolymorphicSource = (toEntity?.Owner?.ObjectType == CdmObjectType.EntityAttributeDef &&
                                    ((CdmEntityAttributeDefinition)toEntity.Owner).IsPolymorphicSource == true);

                                // From the top of the projection (or the top most which contains a generatedSet / operations)
                                // get the attribute names for the foreign key
                                if (newGenSet != null && fromAtts == null)
                                {
                                    fromAtts = GetFromAttributes(newGenSet, fromAtts);
                                }

                                outRels = FindOutgoingRelationshipsForProjection(
                                    outRels,
                                    child,
                                    resOpt,
                                    resEntity,
                                    fromAtts);

                                wasProjectionPolymorphic = isPolymorphicSource;
                            }
                            else
                            {
                                // Non-Projections based approach and current as-is code path

                                isEntityRef = true;

                                List<string> toAtt = child.ExhibitsTraits
                                    .Where(x => x.FetchObjectDefinitionName() == "is.identifiedBy" && x.Arguments?.Count > 0)
                                    .Select(y =>
                                    {
                                        string namedRef = (y.Arguments[0].Value as CdmAttributeReference).NamedReference;
                                        return namedRef.Slice(namedRef.LastIndexOf("/") + 1);
                                    }
                                    )
                                    .ToList();

                                outRels = FindOutgoingRelationshipsForEntityRef(
                                    toEntity,
                                    toAtt,
                                    outRels,
                                    newGenSet,
                                    child,
                                    resOpt,
                                    resEntity,
                                    isResolvedEntity,
                                    wasProjectionPolymorphic: wasProjectionPolymorphic,
                                    wasEntityRef: isEntityRef);
                            }
                        }

                        // repeat the process on the child node
                        bool skipAdd = wasProjectionPolymorphic && isEntityRef;

                        List<CdmE2ERelationship> subOutRels = this.FindOutgoingRelationships(
                            resOpt,
                            resEntity,
                            child,
                            isResolvedEntity,
                            newGenSet,
                            wasProjectionPolymorphic: wasProjectionPolymorphic,
                            wasEntityRef: isEntityRef,
                            fromAtts: fromAtts);
                        outRels.AddRange(subOutRels);

                        // if it was a projection-based polymorphic source up through this branch of the tree and currently it has reached the end of the projection tree to come to a non-projection source,
                        // then skip adding just this one source and continue with the rest of the tree
                        if (skipAdd)
                        {
                            // skip adding only this entry in the tree and continue with the rest of the tree
                            wasProjectionPolymorphic = false;
                        }
                    }
                }
            }

            return outRels;
        }

        /// <summary>
        /// Find the outgoing relationships for Projections.
        /// Given a list of 'From' attributes, find the E2E relationships based on the 'To' information stored in the trait of the attribute in the resolved entity
        /// </summary>
        /// <param name="outRels"></param>
        /// <param name="child"></param>
        /// <param name="resOpt"></param>
        /// <param name="resEntity"></param>
        /// <param name="fromAtts"></param>
        /// <param name="toAttDict"></param>
        internal List<CdmE2ERelationship> FindOutgoingRelationshipsForProjection(
            List<CdmE2ERelationship> outRels,
            CdmAttributeContext child,
            ResolveOptions resOpt,
            CdmEntityDefinition resEntity,
            List<CdmAttributeReference> fromAtts = null)
        {
            if (fromAtts != null)
            {
                ResolveOptions resOptCopy = CdmObjectBase.CopyResolveOptions(resOpt);
                resOptCopy.WrtDoc = resEntity.InDocument;

                // Extract the from entity from resEntity
                CdmObjectReference refToLogicalEntity = resEntity.AttributeContext.Definition;
                CdmEntityDefinition unResolvedEntity = refToLogicalEntity?.FetchObjectDefinition<CdmEntityDefinition>(resOptCopy);
                string fromEntity = unResolvedEntity?.Ctx.Corpus.Storage.CreateRelativeCorpusPath(unResolvedEntity.AtCorpusPath, unResolvedEntity.InDocument);

                for (int i = 0; i < fromAtts.Count; i++)
                {
                    // List of to attributes from the constant entity argument parameter
                    CdmTypeAttributeDefinition fromAttrDef = fromAtts[i].FetchObjectDefinition<CdmTypeAttributeDefinition>(resOptCopy);
                    List<Tuple<string, string, string>> tupleList = GetToAttributes(fromAttrDef, resOptCopy);

                    // For each of the to attributes, create a relationship
                    foreach (var tuple in tupleList)
                    {
                        CdmE2ERelationship newE2ERel = new CdmE2ERelationship(this.Ctx, tuple.Item3)
                        {
                            FromEntity = fromEntity,
                            FromEntityAttribute = fromAtts[i].FetchObjectDefinitionName(),
                            ToEntity = tuple.Item1,
                            ToEntityAttribute = tuple.Item2
                        };

                        outRels.Add(newE2ERel);
                    }
                }
            }

            return outRels;
        }

        /// <summary>
        /// Find the outgoing relationships for Non-Projections EntityRef
        /// </summary>
        /// <param name="toEntity"></param>
        /// <param name="toAtt"></param>
        /// <param name="outRels"></param>
        /// <param name="newGenSet"></param>
        /// <param name="child"></param>
        /// <param name="resOpt"></param>
        /// <param name="resEntity"></param>
        /// <param name="isResolvedEntity"></param>
        internal List<CdmE2ERelationship> FindOutgoingRelationshipsForEntityRef(
            CdmObjectDefinition toEntity,
            List<string> toAtt,
            List<CdmE2ERelationship> outRels,
            CdmAttributeContext newGenSet,
            CdmAttributeContext child,
            ResolveOptions resOpt,
            CdmEntityDefinition resEntity,
            bool isResolvedEntity,
            bool wasProjectionPolymorphic = false,
            bool wasEntityRef = false)
        {
            // entity references should have the "is.identifiedBy" trait, and the entity ref should be valid
            if (toAtt.Count == 1 && toEntity != null)
            {
                // get the attribute name from the foreign key
                Func<CdmAttributeContext, string> findAddedAttributeIdentity = null;
                findAddedAttributeIdentity = (CdmAttributeContext context) =>
                {
                    if (context?.Contents != null)
                    {
                        foreach (var sub in context.Contents)
                        {
                            if (sub.ObjectType == CdmObjectType.AttributeContextDef)
                            {
                                CdmAttributeContext subCtx = sub as CdmAttributeContext;
                                if (subCtx.Type == CdmAttributeContextType.Entity)
                                {
                                    continue;
                                }

                                string fk = findAddedAttributeIdentity(subCtx);
                                if (fk != null)
                                {
                                    return fk;
                                }
                                else if (subCtx?.Type == CdmAttributeContextType.AddedAttributeIdentity && subCtx?.Contents?.Count > 0)
                                {
                                    // the foreign key is found in the first of the array of the "AddedAttributeIdentity" context type
                                    return (subCtx.Contents[0] as CdmObjectReference).NamedReference;
                                }
                            }
                        }
                    }
                    return null;
                };

                string foreignKey = findAddedAttributeIdentity(newGenSet);

                if (foreignKey != null)
                {
                    // this list will contain the final tuples used for the toEntity where
                    // index 0 is the absolute path to the entity and index 1 is the toEntityAttribute
                    List<Tuple<string, string>> toAttList = new List<Tuple<string, string>>();

                    // get the list of toAttributes from the traits on the resolved attribute
                    var resolvedResOpt = new ResolveOptions(resEntity.InDocument);
                    CdmTypeAttributeDefinition attFromFk = this.ResolveSymbolReference(resolvedResOpt, resEntity.InDocument, foreignKey, CdmObjectType.TypeAttributeDef, false) as CdmTypeAttributeDefinition;
                    if (attFromFk != null)
                    {
                        List<Tuple<string, string, string>> fkArgValues = GetToAttributes(attFromFk, resolvedResOpt);

                        foreach (var constEnt in fkArgValues)
                        {
                            var absolutePath = this.Storage.CreateAbsoluteCorpusPath(constEnt.Item1, toEntity);
                            toAttList.Add(new Tuple<string, string>(absolutePath, constEnt.Item2));
                        }
                    }

                    foreach (var attributeTuple in toAttList)
                    {
                        string fromAtt = foreignKey.Slice(foreignKey.LastIndexOf("/") + 1)
                            .Replace($"{child.Name}_", "");
                        CdmE2ERelationship newE2ERel = new CdmE2ERelationship(this.Ctx, "")
                        {
                            FromEntityAttribute = fromAtt,
                            ToEntityAttribute = attributeTuple.Item2
                        };

                        if (isResolvedEntity)
                        {
                            newE2ERel.FromEntity = resEntity.AtCorpusPath;
                            if (this.resEntMap.ContainsKey(attributeTuple.Item1))
                                newE2ERel.ToEntity = this.resEntMap[attributeTuple.Item1];
                            else
                                newE2ERel.ToEntity = attributeTuple.Item1;
                        }
                        else
                        {
                            // find the path of the unresolved entity using the attribute context of the resolved entity
                            CdmObjectReference refToLogicalEntity = resEntity.AttributeContext.Definition;

                            CdmEntityDefinition unResolvedEntity = null;
                            if (refToLogicalEntity != null)
                            {
                                unResolvedEntity = refToLogicalEntity.FetchObjectDefinition<CdmEntityDefinition>(resOpt);
                            }
                            CdmEntityDefinition selectedEntity = unResolvedEntity != null ? unResolvedEntity : resEntity;
                            string selectedEntCorpusPath = unResolvedEntity != null ? unResolvedEntity.AtCorpusPath : resEntity.AtCorpusPath.Replace("wrtSelf_", "");

                            newE2ERel.FromEntity = this.Storage.CreateAbsoluteCorpusPath(selectedEntCorpusPath, selectedEntity);
                            newE2ERel.ToEntity = attributeTuple.Item1;
                        }

                        // if it was a projection-based polymorphic source up through this branch of the tree and currently it has reached the end of the projection tree to come to a non-projection source,
                        // then skip adding just this one source and continue with the rest of the tree
                        if (!(wasProjectionPolymorphic && wasEntityRef))
                        {
                            outRels.Add(newE2ERel);
                        }
                    }
                }
            }

            return outRels;
        }

        /// <summary>
        /// Gets the last modified time of the object found at the input corpus path.
        /// <param name="corpusPath">The path to the object that you want to get the last modified time for</param>
        /// </summary>
        internal async Task<DateTimeOffset?> ComputeLastModifiedTimeAsync(string corpusPath, CdmObject obj = null)
        {
            CdmObject currObject = await this.FetchObjectAsync<CdmObject>(corpusPath, obj, true);
            if (currObject != null)
            {
                return await this.GetLastModifiedTimeAsyncFromObject(currObject);
            }
            return null;
        }

        /// <summary>
        /// Gets the last modified time of the object where it was read from.
        /// </summary>
        internal async Task<DateTimeOffset?> GetLastModifiedTimeAsyncFromObject(CdmObject currObject)
        {
            if (currObject is CdmContainerDefinition)
            {
                StorageAdapter adapter = this.Storage.FetchAdapter((currObject as CdmContainerDefinition).Namespace);

                if (adapter == null)
                {
                    Logger.Error(nameof(CdmCorpusDefinition), this.Ctx,
                        $"Adapter not found for the Cdm object by ID {currObject.Id}.", "GetLastModifiedTimeAsyncFromObject");
                    return null;
                }

                // Remove namespace from path
                Tuple<string, string> pathTuple = StorageUtils.SplitNamespacePath(currObject.AtCorpusPath);
                if (pathTuple == null)
                {
                    Logger.Error(nameof(CdmCorpusDefinition), this.Ctx, "The object's AtCorpusPath should not be null or empty.", nameof(GetLastModifiedTimeAsyncFromObject));
                    return null;
                }
                return await adapter.ComputeLastModifiedTimeAsync(pathTuple.Item2);
            }
            else
            {
                return await this.GetLastModifiedTimeAsyncFromObject(currObject.InDocument);
            }
        }

        /// <summary>
        /// Gets the last modified time of the partition path without trying to read the file itself.
        /// </summary>
        internal async Task<DateTimeOffset?> GetLastModifiedTimeAsyncFromPartitionPath(string corpusPath)
        {
            // we do not want to load partitions from file, just check the modified times
            Tuple<string, string> pathTuple = StorageUtils.SplitNamespacePath(corpusPath);
            if (pathTuple == null)
            {
                Logger.Error(nameof(CdmCorpusDefinition), this.Ctx, "The object path cannot be null or empty.", nameof(GetLastModifiedTimeAsyncFromPartitionPath));
                return null;
            }
            string nameSpace = pathTuple.Item1;
            if (!string.IsNullOrWhiteSpace(nameSpace))
            {
                StorageAdapter adapter = this.Storage.FetchAdapter(nameSpace);

                if (adapter == null)
                {
                    Logger.Error(nameof(CdmCorpusDefinition), this.Ctx,
                        $"Adapter not found for the corpus path '{corpusPath}'", "GetLastModifiedTimeAsyncFromPartitionPath");
                    return null;
                }

                return await adapter.ComputeLastModifiedTimeAsync(pathTuple.Item2);
            }
            return null;
        }

        /// <summary>
        /// Resolves references according to the provided stages and validates.
        /// </summary>
        /// <returns>The validation step that follows the completed step.</returns>
        [Obsolete("This function is likely to be removed soon.")]
        public async Task<CdmValidationStep> ResolveReferencesAndValidateAsync(CdmValidationStep stage, CdmValidationStep stageThrough, ResolveOptions resOpt)
        {
            // use the provided directives or use the current default
            AttributeResolutionDirectiveSet directives = null;
            if (resOpt != null)
            {
                directives = resOpt.Directives;
            }
            else
            {
                directives = this.DefaultResolutionDirectives;
            }

            resOpt = new ResolveOptions { WrtDoc = null, Directives = directives, RelationshipDepth = 0 };

            foreach (CdmDocumentDefinition doc in this.documentLibrary.ListAllDocuments())
            {
                await doc.IndexIfNeeded(resOpt);
            }

            bool finishResolve = stageThrough == stage;
            switch (stage)
            {
                case CdmValidationStep.Start:
                case CdmValidationStep.TraitAppliers:
                    return this.ResolveReferencesStep(
                        "defining traits...",
                        (ref CdmDocumentDefinition currentDoc, ref ResolveOptions resOptions, ref int entityNesting) =>
                        {
                        },
                            resOpt, true, finishResolve || stageThrough == CdmValidationStep.MinimumForResolving,
                            CdmValidationStep.Traits);

                case CdmValidationStep.Traits:
                    this.ResolveReferencesStep(
                        "resolving traits...",
                        (ref CdmDocumentDefinition currentDoc, ref ResolveOptions resOptions, ref int entityNesting) =>
                        {
                            this.ResolveTraits(ref currentDoc, resOptions, ref entityNesting);
                        },
                            resOpt, false, finishResolve, CdmValidationStep.Traits);

                    return this.ResolveReferencesStep(
                        "checking required arguments...",
                        (ref CdmDocumentDefinition currentDoc, ref ResolveOptions resOptions, ref int entityNesting) =>
                        {
                            this.ResolveReferencesTraitsArguments(ref currentDoc, resOptions, ref entityNesting);
                        },
                            resOpt, true, finishResolve, CdmValidationStep.Attributes);

                case CdmValidationStep.Attributes:
                    return this.ResolveReferencesStep(
                        "resolving attributes...",
                        (ref CdmDocumentDefinition currentDoc, ref ResolveOptions resOptions, ref int entityNesting) =>
                        {
                            this.ResolveAttributes(ref currentDoc, resOptions, ref entityNesting);
                        },
                            resOpt, true, finishResolve, CdmValidationStep.EntityReferences);

                case CdmValidationStep.EntityReferences:
                    return this.ResolveReferencesStep(
                        "resolving foreign key references...",
                        (ref CdmDocumentDefinition currentDoc, ref ResolveOptions resOptions, ref int entityNesting) =>
                        {
                            this.ResolveForeignKeyReferences(ref currentDoc, resOptions, ref entityNesting);
                        },
                            resOpt, true, true, CdmValidationStep.Finished);
                default:
                    break;
            }

            // bad step sent in
            return CdmValidationStep.Error;
        }

        private delegate void ResolveAction(ref CdmDocumentDefinition currentDoc, ref ResolveOptions resOptions, ref int entityNesting);

        private CdmValidationStep ResolveReferencesStep(
            string statusMessage,
            ResolveAction resolveAction,
            ResolveOptions resolveOpt,
            bool stageFinished,
            bool finishResolve,
            CdmValidationStep nextStage)
        {
            var ctx = this.Ctx as ResolveContext;
            Logger.Debug(nameof(CdmCorpusDefinition), ctx, statusMessage);
            int entityNesting = 0;
            foreach (CdmDocumentDefinition doc in this.documentLibrary.ListAllDocuments())
            {
                // cache import documents
                CdmDocumentDefinition CurrentDoc = doc;
                resolveOpt.WrtDoc = CurrentDoc;
                resolveAction(ref CurrentDoc, ref resolveOpt, ref entityNesting);
            }
            if (stageFinished)
            {
                if (finishResolve)
                {
                    this.FinishResolve();
                    return CdmValidationStep.Finished;
                }
                return nextStage;
            }
            return nextStage;
        }

        private void ResolveForeignKeyReferences(
            ref CdmDocumentDefinition currentDoc,
            ResolveOptions resOpt,
            ref int entityNesting)
        {
            var nesting = entityNesting;
            currentDoc.Visit("", new VisitCallback
            {
                Invoke = (iObject, path) =>
                    {
                        CdmObjectType ot = iObject.ObjectType;
                        if (ot == CdmObjectType.AttributeGroupDef)
                        {
                            nesting++;
                        }
                        if (ot == CdmObjectType.EntityDef)
                        {
                            nesting++;
                            if (nesting == 1)
                            {
                                (this.Ctx as ResolveContext).RelativePath = path;
                                (iObject as CdmEntityDefinition).FetchResolvedEntityReferences(resOpt);
                            }
                        }
                        return false;
                    }
            }, new VisitCallback
            {
                Invoke = (iObject, path) =>
                    {
                        if (iObject.ObjectType == CdmObjectType.EntityDef || iObject.ObjectType == CdmObjectType.AttributeGroupDef)
                            nesting--;
                        return false;
                    }
            });
            entityNesting = nesting;
        }

        private void ResolveAttributes(ref CdmDocumentDefinition currentDoc, ResolveOptions resOpt, ref int entityNesting)
        {
            var ctx = this.Ctx as ResolveContext;
            var nesting = entityNesting;
            currentDoc.Visit("",
                new VisitCallback
                {
                    Invoke = (iObject, path) =>
                        {
                            CdmObjectType ot = iObject.ObjectType;
                            if (ot == CdmObjectType.EntityDef)
                            {
                                nesting++;
                                if (nesting == 1)
                                {
                                    ctx.RelativePath = path;
                                    (iObject as CdmEntityDefinition).FetchResolvedAttributes(resOpt);
                                }
                            }
                            if (ot == CdmObjectType.AttributeGroupDef)
                            {
                                nesting++;
                                if (nesting == 1)
                                {
                                    ctx.RelativePath = path;
                                    (iObject as CdmAttributeGroupDefinition).FetchResolvedAttributes(resOpt);
                                }
                            }
                            return false;
                        }
                }, new VisitCallback
                {
                    Invoke = (iObject, path) =>
                        {
                            if (iObject.ObjectType == CdmObjectType.EntityDef || iObject.ObjectType == CdmObjectType.AttributeGroupDef)
                                nesting--;
                            return false;
                        }
                });
            entityNesting = nesting;
        }

        private void ResolveReferencesTraitsArguments(
            ref CdmDocumentDefinition currentDoc,
            ResolveOptions resOpt,
            ref int entityNesting)
        {
            CdmDocumentDefinition CurrentDoc = currentDoc; // not clear why the currentDoc is ref. anyone?
            var ctx = this.Ctx as ResolveContext;
            Action<CdmObject> checkRequiredParamsOnResolvedTraits = obj =>
                {
                    ResolvedTraitSet rts = (obj as CdmObjectBase).FetchResolvedTraits(resOpt);
                    if (rts != null)
                    {
                        for (int i = 0; i < rts.Size; i++)
                        {
                            ResolvedTrait rt = rts.Set[i];
                            int found = 0;
                            int resolved = 0;
                            if (rt.ParameterValues != null)
                            {
                                for (int iParam = 0; iParam < rt.ParameterValues.Length; iParam++)
                                {
                                    if (rt.ParameterValues.FetchParameterAtIndex(iParam).Required)
                                    {
                                        found++;
                                        if (rt.ParameterValues.FetchValue(iParam) == null)
                                        {
                                            Logger.Error(nameof(CdmCorpusDefinition), ctx, $"no argument supplied for required parameter '{rt.ParameterValues.FetchParameterAtIndex(iParam).Name}' of trait '{rt.TraitName}' on '{obj.FetchObjectDefinition<CdmObjectDefinition>(resOpt).GetName()}'", CurrentDoc.FolderPath + ctx.RelativePath);
                                        }
                                        else
                                            resolved++;
                                    }
                                }
                            }
                            if (found > 0 && found == resolved)
                            {
                                Logger.Info(nameof(CdmCorpusDefinition), ctx, $"found and resolved '{found}' required parameters of trait '{rt.TraitName}' on '{obj.FetchObjectDefinition<CdmObjectDefinition>(resOpt).GetName()}'", CurrentDoc.FolderPath + ctx.RelativePath);
                            }
                        }
                    }
                };

            currentDoc.Visit("", null, new VisitCallback
            {
                Invoke = (iObject, path) =>
                {
                    CdmObjectType ot = iObject.ObjectType;
                    if (ot == CdmObjectType.EntityDef)
                    {
                        ctx.RelativePath = path;
                        // get the resolution of all parameters and values through inheritence and defaults and arguments, etc.
                        checkRequiredParamsOnResolvedTraits(iObject);
                        var hasAttributeDefs = (iObject as CdmEntityDefinition).Attributes;
                        // do the same for all attributes
                        if (hasAttributeDefs != null)
                        {
                            foreach (var attDef in hasAttributeDefs)
                            {
                                checkRequiredParamsOnResolvedTraits(attDef as CdmObject);
                            }
                        }
                    }
                    if (ot == CdmObjectType.AttributeGroupDef)
                    {
                        ctx.RelativePath = path;
                        // get the resolution of all parameters and values through inheritence and defaults and arguments, etc.
                        checkRequiredParamsOnResolvedTraits(iObject);
                        var memberAttributeDefs = (CdmCollection<CdmAttributeItem>)(iObject as CdmAttributeGroupDefinition).Members;
                        // do the same for all attributes
                        if (memberAttributeDefs != null)
                        {
                            foreach (var attDef in memberAttributeDefs)
                            {
                                checkRequiredParamsOnResolvedTraits(attDef as CdmObject);
                            }
                        }
                    }
                    return false;
                }
            });
        }

        private void ResolveTraits(ref CdmDocumentDefinition currentDoc, ResolveOptions resOpt, ref int entityNesting)
        {
            int nesting = entityNesting;
            currentDoc.Visit("", new VisitCallback
            {
                Invoke = (iObject, path) =>
                {
                    switch (iObject.ObjectType)
                    {
                        case CdmObjectType.TraitDef:
                        case CdmObjectType.PurposeDef:
                        case CdmObjectType.DataTypeDef:
                        case CdmObjectType.EntityDef:
                        case CdmObjectType.AttributeGroupDef:
                            if (iObject.ObjectType == CdmObjectType.EntityDef || iObject.ObjectType == CdmObjectType.AttributeGroupDef)
                            {
                                nesting++;
                                // don't do this for entities and groups defined within entities since getting traits already does that
                                if (nesting > 1)
                                    break;
                            }

                            (this.Ctx as ResolveContext).RelativePath = path;
                            (iObject as CdmObjectDefinitionBase).FetchResolvedTraits(resOpt);
                            break;
                        case CdmObjectType.EntityAttributeDef:
                        case CdmObjectType.TypeAttributeDef:
                            (this.Ctx as ResolveContext).RelativePath = path;
                            (iObject as CdmAttribute).FetchResolvedTraits(resOpt);
                            break;
                    }
                    return false;
                }
            }, new VisitCallback
            {
                Invoke = (iObject, path) =>
                {
                    if (iObject.ObjectType == CdmObjectType.EntityDef || iObject.ObjectType == CdmObjectType.AttributeGroupDef)
                        nesting--;
                    return false;
                }
            });
            entityNesting = nesting;
        }

        /// <summary>
        /// Generates the warnings for a single document.
        /// </summary>
        /// <param name="fd">The folder/document tuple.</param>
        /// <param name="resOpt">The resolve options parameter.</param>
        private async Task GenerateWarningsForSingleDoc(Tuple<CdmFolderDefinition, CdmDocumentDefinition> fd, ResolveOptions resOpt)
        {
            var doc = fd.Item2;

            if (doc.Definitions == null)
            {
                return;
            }

            resOpt.WrtDoc = doc;

            await Task.WhenAll(doc.Definitions.AllItems.Select(async element =>
            {
                if (element is CdmEntityDefinition entity && entity.Attributes.Count > 0)
                {
                    var resolvedEntity = await entity.CreateResolvedEntityAsync(entity.GetName() + "_", resOpt);

                    // TODO: Add additional checks here.
                    this.CheckPrimaryKeyAttributes(resolvedEntity, resOpt);
                }
            }));
        }

        /// <summary>
        /// Checks whether a resolved entity has an "is.identifiedBy" trait.
        /// </summary>
        /// <param name="resolvedEntity">The resolved entity.</param>
        /// <param name="resOpt">The resolve options parameter.</param>
        private void CheckPrimaryKeyAttributes(CdmEntityDefinition resolvedEntity, ResolveOptions resOpt)
        {
            if (resolvedEntity.FetchResolvedTraits(resOpt).Find(resOpt, "is.identifiedBy") == null)
            {
                Logger.Warning(nameof(CdmCorpusDefinition), this.Ctx as ResolveContext, $"There is a primary key missing for the entry {resolvedEntity.GetName()}.");
            }
        }

        /// <summary>
        /// For Projections get the list of 'From' Attributes
        /// </summary>
        /// <param name="newGenSet"></param>
        /// <param name="attrs"></param>
        private List<CdmAttributeReference> GetFromAttributes(CdmAttributeContext newGenSet, List<CdmAttributeReference> fromAttrs)
        {
            if (newGenSet?.Contents != null)
            {
                if (fromAttrs == null)
                {
                    fromAttrs = new List<CdmAttributeReference>();
                }

                foreach (var sub in newGenSet.Contents)
                {
                    if (sub.ObjectType == CdmObjectType.AttributeContextDef)
                    {
                        CdmAttributeContext subCtx = sub as CdmAttributeContext;
                        fromAttrs = GetFromAttributes(subCtx, fromAttrs);
                    }
                    else if (sub.ObjectType == CdmObjectType.AttributeRef)
                    {
                        fromAttrs.Add(sub as CdmAttributeReference);
                    }
                }
            }

            return fromAttrs;
        }

        /// <summary>
        /// For Projections get the list of 'To' Attributes
        /// </summary>
        /// <param name="resEntity"></param>
        /// <param name="resOpt"></param>
        /// <returns></returns>
        private List<Tuple<string, string, string>> GetToAttributes(CdmTypeAttributeDefinition fromAttrDef, ResolveOptions resOpt)
        {
            var tupleList = fromAttrDef?.AppliedTraits?
                .Where(x => x.NamedReference == "is.linkedEntity.identifier" && x.Arguments?.Count > 0)?
                .Select(y => ((y.Arguments[0].Value as CdmEntityReference).FetchObjectDefinition<CdmConstantEntityDefinition>(resOpt) as CdmConstantEntityDefinition))?
                .Where(e => e.ConstantValues.Count > 0)?
                .SelectMany(f => f.ConstantValues)?
                .Select(z => new Tuple<string, string, string>(z[0], z[1], z.Count > 2 ? z[2] : ""))?
                .ToList() as List<Tuple<string, string, string>>;

            return tupleList;
        }
    }
}
