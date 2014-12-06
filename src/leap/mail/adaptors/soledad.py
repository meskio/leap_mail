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
from collections import defaultdict
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
# [ ] get_or_create_doc_wrapper(klass, index):
#     should query by and index, create the document if it does not exist, and
#     return the document wrapper.


_MSGID_PATTERN = r"""<([\w@.]+)>"""
_MSGID_RE = re.compile(_MSGID_PATTERN)


class SoledadDocumentWrapper(models.DocumentWrapper):

    # TODO we could also use a _dirty flag
    # it could be add to models.
    # Having a update_attrs_after_read would also allow
    # to avoid RevisionConflicts if several objects are modifying the
    # same document.

    def __init__(self, **kwargs):
        doc_id = kwargs.pop('doc_id', None)
        self._doc_id = doc_id
        self._lock = defer.DeferredLock()
        super(SoledadDocumentWrapper, self).__init__(**kwargs)

    def create(self, store):
        def update_doc_id(doc):
            self._doc_id = doc.doc_id
            return doc
        d = store.create_doc(self.serialize())
        d.addCallback(update_doc_id)
        return d

    def update(self, store):
        # the deferred lock guards against revision conflicts
        return self._lock.run(self._update, store)

    def _update(self, store):
        leap_assert(self._doc_id is not None,
                    "Need to create doc before update")

        def update_and_put_doc(doc):
            doc.content.update(self.serialize())
            return store.put_doc(doc)

        d = store.get_doc(self._doc_id)
        d.addCallback(update_and_put_doc)
        return d


#
# Message documents
#


class _FlagsDoc(models.SerializableModel):
    """
    Flags Document.
    """
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


class _HeaderDoc(models.SerializableModel):
    """
    Headers Document.
    """
    type_ = "head"
    chash = ""

    date = ""
    subject = ""
    headers = {}
    part_map = {}
    body = ""  # link to phash of body
    msgid = ""


class _ContentDoc(models.SerializableModel):
    """
    Content Document.
    """
    type_ = "cnt"
    phash = ""

    ctype = ""  # XXX index by ctype too?
    lkf = []  # XXX not implemented yet!
    raw = ""


class MessageWrapper(object):
    implements(IMessageWrapper)

    def __init__(self, fdoc, hdoc, cdocs=None):
        """
        Need at least a flag-document and a header-document to instantiate a
        MessageWrapper. Content-documents can be retrieved lazily.
        """
        self.fdoc = fdoc
        self.hdoc = hdoc
        if cdocs is None:
            cdocs = {}
        self.cdocs = cdocs

#
# Mailboxes
#


class _MailboxDoc(models.SerializableModel):
    """
    Mailbox Document.
    """
    type_ = "mbox"
    mbox = INBOX_NAME
    flags = []
    closed = False
    subscribed = False
    # XXX rw should be bool instead, and convert to int in imap
    rw = 1


class MailboxWrapper(SoledadDocumentWrapper):
    model = _MailboxDoc


#
# Soledad Adaptor
#


class SoledadMailAdaptor(object):

    implements(IMailAdaptor)
    store = None

    """
    indexes is a dictionary containing the index definitions for the underlying
    u1db store underlying soledad. It needs to be in the following format:
    {'index-name': ['field1', 'field2']}
    """
    indexes = indexes.MAIL_INDEXES
    store_ready = False

    _index_creation_deferreds = []

    # TODO we might want to move this logic to soledad itself
    # so that each application can pass a set of indexes for their data model.
    def initialize_store(self, store):
        """
        Initialize the indexes in the database.

        :param store: store
        :returns: a Deferred that will fire when the store is correctly
                  initialized.
        :rtype: deferred
        """
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

    def create_msg_docs(self, store, msg_wrapper):
        pass

    def update_msg_flags(self, store, msg_wrapper):
        pass

    def update_msg_tags(self, store, msg_wrapper):
        pass

    # mailbox methods in adaptor too???
    # for symmetry, sounds good to have

    def get_mbox_by_name(self, MailboxClass, store, name):
        def get_first_if_any(docs):
            return docs[0] if docs else None

        def wrap_mbox(mbox):
            if mbox is None:
                return None
            return MailboxClass(MailboxWrapper(mbox))

        d = store.get_from_index(
            self.indexes.TYPE_MBOX_IDX,
            "mbox", normalize_mailbox(name))
        d.addCallback(get_first_if_any)
        return d

    def get_all_mboxes(self, store):
        def get_names(docs):
            return [doc.content["mbox"] for doc in docs]

        d = store.get_from_index(self.indexes.TYPE_IDX, "mbox")
        d.addCallback(get_names)
        return d

    def create_mbox_doc(self, store, mbox_wrapper):
        # XXX avoid duplication somehow?
        empty_mbox_doc = _build_mbox_doc()
        d = store.create_doc(empty_mbox_doc)
        return d

    def update_mbox_doc(self, store, mbox_wrapper):
        # XXX use a (deferred) lock to update the doc
        pass


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
    _fdoc = _FlagsDoc()
    _fdoc.chash = chash
    _fdoc.size = size
    _fdoc.multi = multi
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

    _hdoc = _HeaderDoc()
    _hdoc.chash = chash
    _hdoc.headers = headers
    _hdoc.msgid = msgid

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


def _build_mbox_doc():
    _mbox_doc = _MailboxDoc()
    return _mbox_doc.serialize()
