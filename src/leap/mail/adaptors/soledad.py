# -*- coding: utf-8 -*-
# soledad.py
# Copyright (C) 2014 LEAP
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
Soledadad MailAdaptor module.
"""
import re
from collections import defaultdict, namedtuple
from email import message_from_string

from pycryptopp.hash import sha256
from twisted.internet import defer
from zope.interface import implements

from leap.common.check import leap_assert, leap_assert_type

from leap.mail import walk
from leap.mail.adaptors import soledad_indexes as indexes
from leap.mail.constants import INBOX_NAME
from leap.mail.adaptors import models
from leap.mail.imap.mailbox import normalize_mailbox
from leap.mail.utils import lowerdict, first
from leap.mail.utils import stringify_parts_map
from leap.mail.interfaces import IMailAdaptor, IMessageWrapper


# TODO
# [ ] Convenience function to create mail specifying subject, date, etc?


_MSGID_PATTERN = r"""<([\w@.]+)>"""
_MSGID_RE = re.compile(_MSGID_PATTERN)


class DuplicatedDocumentError(Exception):
    """
    Raised when a duplicated document is detected.
    """
    pass


class SoledadDocumentWrapper(models.DocumentWrapper):

    # TODO we could also use a _dirty flag (in models)

    # We keep a dictionary with DeferredLocks, that will be
    # unique to every subclass of SoledadDocumentWrapper.
    _k_locks = defaultdict(defer.DeferredLock)

    @classmethod
    def _get_klass_lock(cls):
        """
        Get a DeferredLock that is unique for this subclass name.
        Used to lock the access to indexes in the `get_or_create` call
        for a particular DocumentWrapper.
        """
        return cls._k_locks[cls.__name__]

    def __init__(self, **kwargs):
        doc_id = kwargs.pop('doc_id', None)
        self._doc_id = doc_id
        self._lock = defer.DeferredLock()
        super(SoledadDocumentWrapper, self).__init__(**kwargs)

    def create(self, store):
        """
        Create the documents for this wrapper.
        Since this method will not check for duplication, the
        responsibility of avoiding duplicates is left to the caller.

        You might be interested in using `get_or_create` classmethod
        instead (that's the preferred way of creating documents from
        the wrapper object).

        :return: a deferred that will fire when the underlying
                 Soledad document has been created.
        :rtype: Deferred
        """
        leap_assert(self._doc_id is None,
                    "This document already has a doc_id!")

        def update_doc_id(doc):
            self._doc_id = doc.doc_id
            return doc
        d = store.create_doc(self.serialize())
        d.addCallback(update_doc_id)
        return d

    def update(self, store):
        """
        Update the documents for this wrapper.

        :return: a deferred that will fire when the underlying
                 Soledad document has been updated.
        :rtype: Deferred
        """
        # the deferred lock guards against revision conflicts
        return self._lock.run(self._update, store)

    def _update(self, store):
        leap_assert(self._doc_id is not None,
                    "Need to create doc before updating")

        def update_and_put_doc(doc):
            doc.content.update(self.serialize())
            return store.put_doc(doc)

        d = store.get_doc(self._doc_id)
        d.addCallback(update_and_put_doc)
        return d

    def delete(self, store):
        """
        Delete the documents for this wrapper.

        :return: a deferred that will fire when the underlying
                 Soledad document has been deleted.
        :rtype: Deferred
        """
        # the deferred lock guards against conflicts while updating
        return self._lock.run(self._delete, store)

    def _delete(self, store):
        leap_assert(self._doc_id is not None,
                    "Need to create doc before deleting")
        # XXX might want to flag this DocumentWrapper to avoid
        # updating it by mistake. This could go in models.DocumentWrapper

        def delete_doc(doc):
            return store.delete_doc(doc)

        d = store.get_doc(self._doc_id)
        d.addCallback(delete_doc)
        return d

    @classmethod
    def get_or_create(cls, store, index, value):
        """
        Get a unique DocumentWrapper by index, or create a new one if the
        matching query does not exist.
        """
        return cls._get_klass_lock().run(
            cls._get_or_create, store, index, value)

    @classmethod
    def _get_or_create(cls, store, index, value):
        assert store is not None
        assert index is not None
        assert value is not None

        def get_main_index():
            try:
                return cls.model.__meta__.index
            except AttributeError:
                raise RuntimeError("The model is badly defined")

        def try_to_get_doc_from_index(indexes):
            values = []
            idx_def = dict(indexes)[index]
            if len(idx_def) == 1:
                values = [value]
            else:
                main_index = get_main_index()
                fields = cls.model.serialize()
                for field in idx_def:
                    if field == main_index:
                        values.append(value)
                    else:
                        values.append(fields[field])
            d = store.get_from_index(index, *values)
            return d

        def get_first_doc_if_any(docs):
            if not docs:
                return None
            if len(docs) > 1:
                raise DuplicatedDocumentError
            return docs[0]

        def wrap_existing_or_create_new(doc):
            if doc:
                return cls(**doc.content)
            else:
                return create_and_wrap_new_doc()

        def create_and_wrap_new_doc():
            # XXX use closure to store indexes instead of
            # querying for them again.
            d = store.list_indexes()
            d.addCallback(get_wrapper_instance_from_index)
            d.addCallback(return_wrapper_when_created)
            return d

        def get_wrapper_instance_from_index(indexes):
            init_values = {}
            idx_def = dict(indexes)[index]
            if len(idx_def) == 1:
                init_value = {idx_def[0]: value}
                return cls(**init_value)
            main_index = get_main_index()
            fields = cls.model.serialize()
            for field in idx_def:
                if field == main_index:
                    init_values[field] = value
                else:
                    init_values[field] = fields[field]
            return cls(**init_values)

        def return_wrapper_when_created(wrapper):
            d = wrapper.create(store)
            d.addCallback(lambda doc: wrapper)
            return d

        d = store.list_indexes()
        d.addCallback(try_to_get_doc_from_index)
        d.addCallback(get_first_doc_if_any)
        d.addCallback(wrap_existing_or_create_new)
        return d

    @classmethod
    def get_all(cls):
        # TODO confirm we want this
        # get a collection of wrappers around all the documents belonging
        # to this kind.
        pass


# a very thin wrapper to hide the u1db document id in methods
# where we don't need to get the full document from the database.
#U1DBDocWrapper = namedtuple('u1dbDocWrapper', ['doc_id'])


#
# Message documents
#

class FlagsDocWrapper(SoledadDocumentWrapper):

    class model(models.SerializableModel):
        type_ = "flags"
        chash = ""

        mbox = "inbox"
        seen = False
        deleted = False
        recent = False
        multi = False
        flags = []
        tags = []
        size = 0

        class __meta__(object):
            index = "mbox"


class HeaderDocWrapper(SoledadDocumentWrapper):

    class model(models.SerializableModel):
        type_ = "head"
        chash = ""

        date = ""
        subject = ""
        headers = {}
        part_map = {}
        body = ""  # link to phash of body
        msgid = ""

        class __meta__(object):
            index = "chash"


class ContentDocWrapper(SoledadDocumentWrapper):

    class model(models.SerializableModel):
        type_ = "cnt"
        phash = ""

        ctype = ""  # XXX index by ctype too?
        lkf = []  # XXX not implemented yet!
        raw = ""

        class __meta__(object):
            index = "phash"


class MessageWrapper(object):
    # TODO generalize wrapper composition

    implements(IMessageWrapper)

    def __init__(self, fdoc, hdoc, cdocs=None):
        """
        Need at least a flag-document and a header-document to instantiate a
        MessageWrapper. Content-documents can be retrieved lazily.
        """
        # TODO must enforce we're receiving DocWrappers
        # XXX - or, maybe, wrap them here...
        self.fdoc = fdoc
        self.hdoc = hdoc
        if cdocs is None:
            cdocs = {}
        self.cdocs = cdocs

#
# Mailboxes
#


class MailboxWrapper(SoledadDocumentWrapper):

    class model(models.SerializableModel):
        type_ = "mbox"
        mbox = INBOX_NAME
        flags = []
        closed = False
        subscribed = False
        # XXX rw should be bool instead, and convert to int in imap
        rw = 1

        class __meta__(object):
            index = "mbox"


#
# Soledad Adaptor
#

# TODO make this an interface?
class SoledadIndexMixin(object):
    """
    this will need a class attribute `indexes`, that is a dictionary containing
    the index definitions for the underlying u1db store underlying soledad.

    It needs to be in the following format:
    {'index-name': ['field1', 'field2']}
    """
    # TODO could have a wrapper class for indexes, supporting introspection
    # and __getattr__
    indexes = {}

    store_ready = False
    _index_creation_deferreds = []

    # TODO we might want to move this logic to soledad itself
    # so that each application can pass a set of indexes for their data model.
    # TODO check also the decorator used in keymanager for waiting for indexes
    # to be ready.

    def initialize_store(self, store):
        """
        Initialize the indexes in the database.

        :param store: store
        :returns: a Deferred that will fire when the store is correctly
                  initialized.
        :rtype: deferred
        """
        # TODO I think we *should* get another deferredLock in here, but
        # global to the soledad namespace, to protect from several points
        # initializing soledad indexes at the same time.

        leap_assert(store, "Need a store")
        leap_assert_type(self.indexes, dict)
        self._index_creation_deferreds = []

        def _on_indexes_created(ignored):
            self.store_ready = True

        def _create_index(name, expression):
            d = store.create_index(name, *expression)
            self._index_creation_deferreds.append(d)

        def _create_indexes(db_indexes):
            db_indexes = dict(db_indexes)

            for name, expression in self.indexes.items():
                if name not in db_indexes:
                    # The index does not yet exist.
                    _create_index(name, expression)
                    continue

                if expression == db_indexes[name]:
                    # The index exists and is up to date.
                    continue
                # The index exists but the definition is not what expected, so
                # we delete it and add the proper index expression.
                d1 = store.delete_index(name)
                d1.addCallback(lambda _: _create_index(name, expression))

            all_created = defer.gatherResults(self._index_creation_deferreds)
            all_created.addCallback(_on_indexes_created)
            return all_created

        # Ask the database for currently existing indexes, and create them
        # if not found.
        d = store.list_indexes()
        d.addCallback(_create_indexes)
        return d


class SoledadMailAdaptor(SoledadIndexMixin):

    implements(IMailAdaptor)
    store = None

    indexes = indexes.MAIL_INDEXES

    #@staticmethod
    def get_msg_from_string(MessageClass, raw_msg):
        """
        :rtype: MessageClass instance.
        """
        assert(MessageClass is not None)
        fdoc, hdoc, cdocs = _split_into_parts(raw_msg)
        return SoledadMailAdaptor.msg_from_docs(
            MessageClass, MessageWrapper(fdoc, hdoc, cdocs))

    #@staticmethod
    def get_msg_from_docs(MessageClass, msg_wrapper):
        """
        :rtype: MessageClass instance.
        """
        assert(MessageClass is not None)
        return MessageClass(msg_wrapper)

    # XXX TO BE DECIDED YET ---- see interfaces...
    # These methods will contain the create/put, but they should
    # be invoked from a clear point in the public API.
    # Maybe the best place is MessageCollection (since Mailbox
    # will contain one of that).

    def create_msg(self, store, msg_wrapper):
        # XXX iterate through parts of the msg_wrapper
        # and create them.
        # 1. cdocs should not be empty
        # 2. fdoc/hdoc should not have doc_id
        # 3. if cdocs exist, it might already have the doc_id
        pass

    def update_msg(self, store, msg_wrapper):
        return msg_wrapper.update(store)

    # mailbox methods in adaptor too???
    # for symmetry, sounds good to have

    def get_or_create_mbox(self, store, name):
        index = self.indexes.TYPE_MBOX_IDX
        mbox = normalize_mailbox(name)
        return MailboxWrapper.get_or_create(index, mbox)

    def update_mbox(self, store, mbox_wrapper):
        return mbox_wrapper.update(store)

    def get_all_mboxes(self, store):
        def get_names(docs):
            return [doc.content["mbox"] for doc in docs]

        # XXX --- use SoledadDocumentWrapper.get_all instead
        # XXX --- use u1db.get_index_keys ???
        d = store.get_from_index(self.indexes.TYPE_IDX, "mbox")
        d.addCallback(get_names)
        # XXX --- return MailboxWrapper instead
        return d


def _split_into_parts(raw):
    # TODO signal that we can delete the original message!-----
    # when all the processing is done.
    # TODO add the linked-from info !
    # TODO add reference to the original message?
    # TODO populate Default FLAGS/TAGS (unseen?)
    # TODO seed propely the content_docs with defaults??

    msg, parts, chash, size, multi = _parse_msg(raw)
    body_phash_fun = [walk.get_body_phash_simple,
                      walk.get_body_phash_multi][int(multi)]
    body_phash = body_phash_fun(walk.get_payloads(msg))
    parts_map = walk.walk_msg_tree(parts, body_phash=body_phash)

    fdoc = _build_flags_doc(chash, size, multi)
    hdoc = _build_headers_doc(msg, chash, parts_map)

    # The MessageWrapper expects a dict, one-indexed
    cdocs = dict(enumerate(walk.get_raw_docs(msg, parts), 1))

    # XXX convert each to_dicts...
    return fdoc, hdoc, cdocs


def _parse_msg(raw):
    msg = message_from_string(raw)
    parts = walk.get_parts(msg)
    size = len(raw)
    chash = sha256.SHA256(raw).hexdigest()
    multi = msg.is_multipart()
    return msg, parts, chash, size, multi


def _build_flags_doc(chash, size, multi):
    _fdoc = FlagsDocWrapper(chash=chash, size=size, multi=multi)
    return _fdoc.serialize()


def _build_headers_doc(msg, chash, parts_map):
    """
    Assemble a headers document from the original parsed message, the
    content-hash, and the parts map.

    It takes into account possibly repeated headers.
    """
    headers = defaultdict(list)
    for k, v in msg.items():
        headers[k].append(v)

    # "fix" for repeated headers.
    for k, v in headers.items():
        newline = "\n%s: " % (k,)
        headers[k] = newline.join(v)

    lower_headers = lowerdict(headers)
    msgid = first(_MSGID_RE.findall(
        lower_headers.get('message-id', '')))

    _hdoc = HeaderDocWrapper(
        chash=chash, headers=headers, msgid=msgid)

    def copy_attr(headers, key, doc):
        if key in headers:
            setattr(doc, key, headers[key])

    copy_attr(headers, "subject", _hdoc)
    copy_attr(headers, "date", _hdoc)

    hdoc = _hdoc.serialize()
    # add parts map to header doc
    # (body, multi, part_map)
    for key in parts_map:
        hdoc[key] = parts_map[key]
    return stringify_parts_map(hdoc)


#def _build_mbox_wrapper():
    #_mbox_doc = MailboxWrapper()
    #return _mbox_doc.serialize()
