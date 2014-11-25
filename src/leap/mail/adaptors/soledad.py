# -*- coding: utf-8 -*-
# interfaces.py
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
from leap.mail.constants import INBOX_NAME
from leap.mail.adaptors.soledad_indexes import MAIL_INDEXES
from leap.mail.imap.mailbox import normalize_mailbox
from leap.mail.utils import lowerdict, first
from leap.mail.utils import stringify_parts_map
from leap.mail.interfaces import IMailAdaptor, IMessageWrapper


# TODO
# [ ] Convenience function to create mail specifying subject, date, etc?


_MSGID_PATTERN = r"""<([\w@.]+)>"""
_MSGID_RE = re.compile(_MSGID_PATTERN)


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


class MailboxWrapper(object):

    def __init__(self, mbox_doc):
        self.mbox_doc = mbox_doc
        self.model = _MailboxDoc


class _DocumentModel(object):
    """
    A Generic document model, that can be serialized into a dictionary.
    """

    @classmethod
    def get_dict(klass):
        """
        Get a dictionary representation of the public attributes in the model
        class.
        """
        # XXX fix SUFFIX foo_ (type)
        return dict(
            [(k, v) for k, v in klass.__class__.__dict__.items()
             if not k.startswith('_')])


class _FlagsDoc(_DocumentModel):
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

    # XXX deprecated
    # uid = None


class _HeaderDoc(_DocumentModel):
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


class _ContentDoc(_DocumentModel):
    """
    Content Document.
    """
    type_ = "cnt"
    phash = ""

    ctype = ""  # XXX index by ctype too?
    lkf = []  # XXX not implemented yet!
    raw = ""


class _MailboxDoc(_DocumentModel):
    """
    Mailbox Document.
    """
    type_ = "mbox"
    mbox = INBOX_NAME
    subject = ""
    flags = []
    closed = False
    subscribed = False
    # XXX rw should be bool instead, and convert to int in imap
    rw = 1


class SoledadMailAdaptor(object):

    implements(IMailAdaptor)
    store = None

    """
    indexes is a dictionary containing the index definitions for the underlying
    u1db store underlying soledad. It needs to be in the following format:
    {'index-name': ['field1', 'field2']}
    """
    indexes = MAIL_INDEXES
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

    @classmethod
    def get_msg_from_string(cls, MessageClass, raw_msg):
        """
        :rtype: MessageClass instance.
        """
        assert(MessageClass is not None)
        fdoc, hdoc, cdocs = _split_into_parts(raw_msg)
        return cls.msg_from_docs(
            MessageClass, MessageWrapper(fdoc, hdoc, cdocs))

    @classmethod
    def get_msg_from_docs(cls, MessageClass, msg_wrapper):
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
        # XXX avoid duplication?
        d = store.create_doc(mbox_wrapper.to_dict())
        return d

    def update_mbox_doc(self, store, mbox_wrapper):
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
    return _fdoc.to_dict()


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

    hdoc = _hdoc.to_dict()
    # add parts map to header doc
    # (body, multi, part_map)
    for key in parts_map:
        hdoc[key] = parts_map[key]
    return stringify_parts_map(hdoc)
