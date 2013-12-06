from __future__ import absolute_import
from .patch_zodb import patch_zodb
from UserDict import DictMixin
from ZODB.DB import DB
from ZODB.FileStorage.FileStorage import FileStorage
from persistent.mapping import PersistentMapping
import collections
import inspect
import json
import logging
import os
import sys


patch_zodb()


log = logging.getLogger("zodbdump")


PRIMITIVES = frozenset([int, bool, str, unicode, type(None)])


class Node(DictMixin):
    def __init__(self, context):
        self.context = context
        self.type_ = type(self.context)

        if self.type_ not in PRIMITIVES:
            for base in inspect.getmro(self.type_):
                if base.__module__.startswith("BTrees"):
                    self.type_ = base
                    break
                elif base.__module__.startswith("persistent"):
                    self.type_ = base
                    break

        self.dict_ = None
        if (self.type_.__name__.endswith('BTree') or
            self.type_.__name__.endswith('TreeSet') or
            self.type_.__name__.endswith('Bucket')):
            # ZODB BTrees
            self.dict_ = self.context
        elif (self.type_.__name__.startswith('BTrees.') and
              self.type_.__name__.endswith('Set')):
            self.dict_ = dict(enumerate(self.context.keys()))
        elif (isinstance(self.context, collections.Mapping) or
              isinstance(self.context, PersistentMapping)):
            self.dict_ = self.context
        elif isinstance(self.context, collections.Iterable):
            self.dict_ = dict(enumerate(self.context))
        elif hasattr(self.context, '__Broken_state__'):
            # ZODB
            if isinstance(self.context.__Broken_state__, collections.Mapping):
                self.dict_ = self.context.__Broken_state__
            else:
                import pdb; pdb.set_trace()

        if self.dict_ is None:
            self.dict_ = {}

        # Wake up sleepy objects - a hack for ZODB objects in "ghost" state.
        wakeupcall = dir(self.dict_)
        del wakeupcall

        self.keys_ = tuple(sorted(self.dict_))

    def __getitem__(self, key):
        v = self.dict_[key]
        if type(v) in PRIMITIVES:
            return v
        return Node(v)

    def keys(self):
        return self.keys_


def debug_processor(obj, path, name):
    import pdb; pdb.set_trace()


def dict_processor(obj, path, name):
    return extract_metadata(obj, path, name)


def list_processor(obj, path, name):
    data = extract_metadata(obj, path, name)
    result = []
    for k in sorted(data):
        result.append(data[k])
    return result


def long_processor(obj, path, name):
    return obj.context


def tuple_processor(obj, path, name):
    return tuple(list_processor(obj, path, name))


def datetime_processor(obj, path, name):
    return obj.context.__Broken_state__['_t']


def extract_metadata(obj, path, name):
    d = {}
    for k, v in obj.iteritems():
        if not isinstance(k, str):
            k = repr(k)
        if isinstance(v, str):
            try:
                v = v.decode('utf-8')
            except UnicodeDecodeError:
                v = "<binary data of %s bytes>" % len(v)
        elif isinstance(v, Node):
            t = type(v.context)
            n = "%s.%s" % (t.__module__, t.__name__)
            if n not in metadata_processors:
                raise ValueError("Unknown metadata type '%s' for '%s' in '%s'" % (n, name, path))
            p = metadata_processors[n]
            if p is None:
                d[k] = u"<%s>" % n
                continue
            v = p(v, path, k)
        d[k] = v
    return d


def write_metadata(obj, path, name):
    d = extract_metadata(obj, path, name)
    newpath = os.path.join(path, "%s.json" % name)
    if len(d) == 0:
        if os.path.exists(newpath):
            os.unlink(newpath)
        return
    with open(newpath, 'w') as f:
        log.info("Writing metadata %s", newpath)
        json.dump(d, f, indent=4)


def write(obj, path, name, data):
    t = type(obj.context)
    n = "%s.%s" % (t.__module__, t.__name__)
    newpath = os.path.join(path, name)
    log.info("Writing %s %s", newpath, n)
    with open(newpath, 'wb') as f:
        if isinstance(data, Node):
            while data:
                f.write(data['data'])
                data = data.get('next')
        else:
            if isinstance(data, unicode):
                data = data.encode('utf-8')
            f.write(data)
    write_metadata(obj, path, name)


def document(obj, path, name):
    annotations = obj.get('__annotations__', {})
    if 'cooked_text' in obj:
        data = obj['text']
    elif 'data' in obj:
        data = obj['data']
    elif 'raw' in obj:
        data = obj['raw']
    elif '__annotations__' in obj and 'Archetypes.storage.AnnotationStorage-text' in annotations:
        data = annotations['Archetypes.storage.AnnotationStorage-text']['raw']
    elif '__annotations__' in obj and 'Archetypes.storage.AnnotationStorage-image' in annotations:
        data = annotations['Archetypes.storage.AnnotationStorage-image']['data']
    elif '__annotations__' in obj and 'Archetypes.storage.AnnotationStorage-file' in annotations:
        data = annotations['Archetypes.storage.AnnotationStorage-file']['data']
    else:
        import pdb; pdb.set_trace()
        return
    write(obj, path, name, data)


def blog_entry(obj, path, name):
    data = obj['body']['raw']
    write(obj, path, name, data)


def quills_entry(obj, path, name):
    data = obj['text']['raw']
    write(obj, path, name, data)


def folder(obj, path, name):
    newpath = os.path.join(path, name)
    if not os.path.exists(newpath):
        os.mkdir(newpath)
    write_metadata(obj, path, name)
    return dump(obj, newpath, name)


def largefolder(obj, path, name):
    return folder(obj['_tree'], path, name)


def dump(obj, path, name):
    for k in obj:
        v = obj[k]
        if isinstance(v, Node):
            t = type(v.context)
            n = "%s.%s" % (t.__module__, t.__name__)
            if n not in processors:
                raise ValueError("Unknown type '%s' for '%s' in '%s'" % (n, name, path))
            p = processors[n]
            if p is None:
                continue
            p(v, path, k)


common_skip = [
    'Products.ATContentTypes.tool.atct.ATCTTool',
    'Products.Archetypes.ArchetypeTool.ArchetypeTool',
    'Products.Archetypes.ReferenceEngine.ReferenceCatalog',
    'Products.Archetypes.ReferenceEngine.UIDCatalog',
    'Products.Archetypes.Schema.Schema',
    'Products.CMFCore.CachingPolicyManager.CachingPolicyManager',
    'Products.CMFCore.ContentTypeRegistry.ContentTypeRegistry',
    'Products.CMFCore.CookieCrumbler.CookieCrumbler',
    'Products.CMFDefault.SyndicationInfo.SyndicationInformation',
    'Products.CMFFormController.FormController.FormController',
    'Products.CMFPlone.ActionIconsTool.ActionIconsTool',
    'Products.CMFPlone.ActionsTool.ActionsTool',
    'Products.CMFPlone.CalendarTool.CalendarTool',
    'Products.CMFPlone.CatalogTool.CatalogTool',
    'Products.CMFPlone.DiscussionTool.DiscussionTool',
    'Products.CMFPlone.FactoryTool.FactoryTool',
    'Products.CMFPlone.FormTool.FormTool',
    'Products.CMFPlone.GroupDataTool.GroupDataTool',
    'Products.CMFPlone.GroupsTool.GroupsTool',
    'Products.CMFPlone.InterfaceTool.InterfaceTool',
    'Products.CMFPlone.MemberDataTool.MemberDataTool',
    'Products.CMFPlone.MembershipTool.MembershipTool',
    'Products.CMFPlone.MetadataTool.MetadataTool',
    'Products.CMFPlone.MigrationTool.MigrationTool',
    'Products.CMFPlone.NavigationTool.NavigationTool',
    'Products.CMFPlone.PloneControlPanel.PloneControlPanel',
    'Products.CMFPlone.PloneTool.PloneTool',
    'Products.CMFPlone.PropertiesTool.PropertiesTool',
    'Products.CMFPlone.QuickInstallerTool.QuickInstallerTool',
    'Products.CMFPlone.RegistrationTool.RegistrationTool',
    'Products.CMFPlone.SkinsTool.SkinsTool',
    'Products.CMFPlone.SyndicationTool.SyndicationTool',
    'Products.CMFPlone.TranslationServiceTool.TranslationServiceTool',
    'Products.CMFPlone.TypesTool.TypesTool',
    'Products.CMFPlone.URLTool.URLTool',
    'Products.CMFPlone.UndoTool.UndoTool',
    'Products.CMFPlone.WorkflowTool.WorkflowTool',
    'Products.CMFUid.UniqueIdAnnotationTool.UniqueIdAnnotationTool',
    'Products.CMFUid.UniqueIdGeneratorTool.UniqueIdGeneratorTool',
    'Products.CMFUid.UniqueIdHandlerTool.UniqueIdHandlerTool',
    'Products.GroupUserFolder.GroupUserFolder.GroupUserFolder',
    'Products.MailHost.MailHost.MailHost',
    'Products.MimetypesRegistry.MimeTypesRegistry.MimeTypesRegistry',
    'Products.PortalTransforms.TransformEngine.TransformTool',
    'Products.Quills.BloggerAPI.BloggerAPI',
    'Products.Quills.MetaWeblogAPI.MetaWeblogAPI',
    'Products.Quills.QuillsTool.QuillsTool',
    'Products.RPCAuth.RPCAuth.RPCAuth',
    'Products.ResourceRegistries.tools.CSSRegistry.CSSRegistryTool',
    'Products.ResourceRegistries.tools.JSRegistry.JSRegistryTool',
    'Products.SecureMailHost.SecureMailHost.SecureMailHost',
    'Products.SimpleBlog.SimpleBlogTool.SimpleBlogManager',
    'Products.SiteErrorLog.SiteErrorLog.SiteErrorLog',
    'Products.StandardCacheManagers.AcceleratedHTTPCacheManager.AcceleratedHTTPCacheManager',
    'Products.StandardCacheManagers.RAMCacheManager.RAMCacheManager',
    'Products.ZCatalog.ZCatalog.ZCatalog',
    'Products.kupu.plone.plonelibrarytool.PloneKupuLibraryTool',
    'ZPublisher.BeforeTraverse.MultiHook',
    ]


processors = dict((k, None) for k in common_skip)
processors.update({
    'DateTime.DateTime.DateTime': None,
    'OFS.DTMLDocument.DTMLDocument': document,
    'OFS.Folder.Folder': folder,
    'OFS.Image.File': document,
    'OFS.Image.Image': document,
    'Persistence.PersistentMapping': None,
    'Products.ATContentTypes.content.document.ATDocument': document,
    'Products.ATContentTypes.content.file.ATFile': document,
    'Products.ATContentTypes.content.folder.ATBTreeFolder': largefolder,
    'Products.ATContentTypes.content.folder.ATFolder': folder,
    'Products.ATContentTypes.content.image.ATImage': document,
    'Products.ATContentTypes.content.topic.ATTopic': write_metadata,
    'Products.Archetypes.BaseUnit.BaseUnit': None,
    'Products.CMFDefault.Document.Document': document,
    'Products.CMFDefault.File.File': document,
    'Products.CMFDefault.Image.Image': document,
    'Products.CMFPhoto.Photo.Photo': document,
    'Products.CMFPhotoAlbum.PhotoAlbum.PhotoAlbum': largefolder,
    'Products.CMFPlone.LargePloneFolder.LargePloneFolder': largefolder,
    'Products.CMFPlone.PloneFolder.PloneFolder': folder,
    'Products.CMFTopic.Topic.Topic': write_metadata,
    'Products.ExternalFile.ExternalFile.ExternalFile': write_metadata,
    'Products.ExternalMethod.ExternalMethod.ExternalMethod': write_metadata,
    'Products.PythonScripts.PythonScript.PythonScript': write_metadata,
    'Products.Quills.Weblog.Weblog': folder,
    'Products.Quills.WeblogArchive.WeblogArchive': write_metadata,
    'Products.Quills.WeblogDrafts.WeblogDrafts': folder,
    'Products.Quills.WeblogEntry.WeblogEntry': quills_entry,
    'Products.Quills.WeblogTopic.WeblogTopic': write_metadata,
    'Products.SimpleBlog.Blog.Blog': folder,
    'Products.SimpleBlog.BlogEntry.BlogEntry': blog_entry,
    'Products.SimpleBlog.BlogFolder.BlogFolder': folder,
    '__builtin__.dict': None,
    '__builtin__.list': None,
    '__builtin__.tuple': None,
    })


metadata_processors = dict((k, None) for k in common_skip)
metadata_processors.update({
    'BTrees.OOBTree.OOBTree': dict_processor,
    'DateTime.DateTime.DateTime': datetime_processor,
    'OFS.DTMLDocument.DTMLDocument': None,
    'OFS.Folder.Folder': None,
    'OFS.Image.File': None,
    'OFS.Image.Image': None,
    'OFS.Image.Pdata': dict_processor,
    'Persistence.PersistentMapping': dict_processor,
    'Products.ATContentTypes.content.document.ATDocument': None,
    'Products.ATContentTypes.content.file.ATFile': None,
    'Products.ATContentTypes.content.folder.ATBTreeFolder': None,
    'Products.ATContentTypes.content.folder.ATFolder': None,
    'Products.ATContentTypes.content.image.ATImage': None,
    'Products.ATContentTypes.content.topic.ATTopic': None,
    'Products.ATContentTypes.criteria.date.ATDateCriteria': None,
    'Products.ATContentTypes.criteria.portaltype.ATPortalTypeCriterion': None,
    'Products.ATContentTypes.criteria.simplestring.ATSimpleStringCriterion': None,
    'Products.ATContentTypes.criteria.sort.ATSortCriterion': None,
    'Products.Archetypes.BaseUnit.BaseUnit': None,
    'Products.Archetypes.Field.Image': None,
    'Products.CMFDefault.DiscussionItem.DiscussionItemContainer': None,
    'Products.CMFDefault.Document.Document': None,
    'Products.CMFDefault.File.File': None,
    'Products.CMFDefault.Image.Image': None,
    'Products.CMFPhoto.Photo.Photo': None,
    'Products.CMFPhotoAlbum.PhotoAlbum.PhotoAlbum': None,
    'Products.CMFPlone.LargePloneFolder.LargePloneFolder': None,
    'Products.CMFPlone.PloneFolder.PloneFolder': None,
    'Products.CMFTopic.SimpleStringCriterion.SimpleStringCriterion': None,
    'Products.CMFTopic.SortCriterion.SortCriterion': None,
    'Products.CMFTopic.Topic.Topic': None,
    'Products.ExternalFile.ExternalFile.ExternalFile': None,
    'Products.ExternalMethod.ExternalMethod.ExternalMethod': None,
    'Products.PythonScripts.PythonScript.PythonScript': None,
    'Products.Quills.Weblog.Weblog': None,
    'Products.Quills.WeblogArchive.WeblogArchive': None,
    'Products.Quills.WeblogDrafts.WeblogDrafts': None,
    'Products.Quills.WeblogEntry.WeblogEntry': None,
    'Products.Quills.WeblogTopic.WeblogTopic': None,
    'Products.SimpleBlog.Blog.Blog': None,
    'Products.SimpleBlog.BlogEntry.BlogEntry': None,
    'Products.SimpleBlog.BlogFolder.BlogFolder': None,
    'Shared.DC.Scripts.Bindings.NameAssignments': None,
    'Shared.DC.Scripts.Signature.FuncCode': None,
    'ZPublisher.BeforeTraverse.NameCaller': None,
    '__builtin__.dict': dict_processor,
    '__builtin__.list': list_processor,
    '__builtin__.long': long_processor,
    '__builtin__.tuple': tuple_processor,
    })


def main():
    logging.basicConfig(level=logging.INFO)
    filename = os.path.expanduser(sys.argv[1])
    export_path = os.path.expanduser(sys.argv[2])
    export_name = os.path.basename(export_path)
    export_path = os.path.abspath(os.path.dirname(export_path))
    traverse_path = sys.argv[3:]
    root = Node(DB(FileStorage(filename, read_only=True)).open().root())
    site = root
    for item in traverse_path:
        site = site[item]
    folder(site, export_path, export_name)
