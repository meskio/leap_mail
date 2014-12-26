# -*- coding: utf-8 -*-
# mail.py
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
Generic Access to Mail objects: Public LEAP Mail API.
"""
from twisted.internet import defer

from leap.mail.constants import INBOX_NAME
from leap.mail.adaptors.soledad import SoledadMailAdaptor


# TODO
# [ ] Probably change the name of this module to "api" or "account", mail is
#     too generic (there's also IncomingMail, and OutgoingMail


class Message(object):

    def __init__(self, wrapper):
        """
        :param wrapper: an instance of an implementor of IMessageWrapper
        """
        self._wrapper = wrapper

    def get_wrapper(self):
        return self._wrapper

    # imap.IMessage methods

    def get_flags(self):
        """
        """
        return tuple(self._wrapper.fdoc.flags)

    def get_internal_date(self):
        """
        """
        return self._wrapper.fdoc.date

    # imap.IMessageParts

    def get_headers(self):
        """
        """
        # XXX process here? from imap.messages
        return self._wrapper.hdoc.headers

    def get_body_file(self):
        """
        """

    def get_size(self):
        """
        """
        return self._wrapper.fdoc.size

    def is_multipart(self):
        """
        """
        return self._wrapper.fdoc.multi

    def get_subpart(self, part):
        """
        """
        # XXX ??? return MessagePart?

    # Custom methods.

    def get_tags(self):
        """
        """
        return tuple(self._wrapper.fdoc.tags)


class MessageCollection(object):
    """
    A generic collection of messages. It can be messages sharing the same
    mailbox, tag, the result of a given query, or just a bunch of ids for
    master documents.

    Since LEAP Mail is primarily oriented to store mail in Soledad, the default
    (and, so far, only) implementation of the store is contained in this
    Soledad Mail Adaptor. If you need to use a different adaptor, change the
    adaptor class attribute in your Account object.

    Store is a reference to a particular instance of the message store (soledad
    instance or proxy, for instance).
    """

    # TODO
    # [ ] look at IMessageSet methods
    # [ ] make constructor with a per-instance deferredLock to use on
    #     creation/deletion?
    # [ ] Think about passing a mailbox wrapper to this collection, and flag it
    #     as a "MailboxCollection"
    #     --- is_mailbox_colllection(eq
    #     has_mbox_wrapper). This could be used to autoincrement the UID when
    #     we add a document.
    #     ---  or maybe have a "collection_type" attribute
    # [ ] instead of a mailbox, we could pass an arbitrary container with
    #     pointers to different doc_ids (type: foo)
    # [ ] To guarantee synchronicity of the documents sent together during a
    #     sync, we could get hold of a deferredLock that inhibits
    #     synchronization while we are updating (think more about this!)

    # Account should provide an adaptor instance when creating this collection.
    adaptor = None
    store = None
    messageklass = Message

    # Get message

    def get_message_by_doc_id(self, doc_id):
        # XXX get from adaptor method
        # --- get by UID (mailbox)
        # --- get by
        return self.adaptor.from_docs(self.messageklass, self.store)

    def get_message_by_mailbox_uid(self, uid, cdocs=False):
        # XXX get from local table
        # XXX get fdoc, hdoc...
        # If cdocs, get cdocs also into wrapper
        # return Message(Wrapper)
        pass

    def get_message_by_content_hash(self, chash):
        # XXX
        pass

    #

    def add_msg(self, raw_msg):
        # XXX
        # 1. get msg from string...
        # 2. create message --- need to serialize creation
        # 3. return deferred
        pass

    def copy_msg(self, msg):
        # XXX
        # copy the message to this collection. (it only makes sense for mailbox
        # collections)
        # 1. copy the fdoc ----> I think it's better if Wrapper has a copy
        #    method.
        # 2. remove the doc_id of that fdoc
        # 3. create it
        wrapper = msg.get_wrapper()

    def delete_msg(self, msg):
        wrapper = msg.get_wrapper()
        d = wrapper.delete(self.store)
        return d

    def udpate_flags(self, msg, flags, mode):
        wrapper = msg.get_wrapper()
        # 1. update the flags in the message wrapper --- stored where???
        # 2. update the special flags in the wrapper (seen, etc)
        # 3. call adaptor.update_msg(store)
        pass

    def update_tags(self, msg, tags, mode):
        wrapper = msg.get_wrapper()
        # 1. update the tags in the message wrapper --- stored where???
        # 2. call adaptor.update_msg(store)
        pass

    # TODO add delete methods here?


class Account(object):
    """
    Account is the top level abstraction to access collections of messages
    associated with a LEAP Mail Account.

    It primarily handles creation and access of Mailboxes, which will be the
    basic collection handled by traditional MUAs, but it can also handle other
    types of Collections (tag based, for instance).

    leap.mail.imap.SoledadBackedAccount partially proxies methods in this
    class.
    """

    # Adaptor is passed to the returned MessageCollections, so if you want to
    # use a different adaptor this is the place to change it, by subclassing
    # the Account class.

    adaptor_class = SoledadMailAdaptor
    store = None
    mailboxes = None

    def __init__(self, store):
        self.store = store
        self.adaptor = self.adaptor_class()

        self.__mailboxes = set([])  # XXX needed?
        self._initialized = False
        self._deferred_initialization = defer.Deferred()

        self._initialize_storage()

    def _initialize_storage(self):

        def add_mailbox_if_none(result):
            # every user should have the right to an inbox folder
            # at least, so let's make one!
            if not self.mailboxes:
                self.add_mailbox(INBOX_NAME)

        def finish_initialization(result):
            self._initialized = True
            self._deferred_initialization.callback(None)

        def load_mbox_cache(result):
            d = self._load_mailboxes()
            d.addCallback(lambda _: result)
            return d

        d = self.adaptor.initialize_store(self.store)
        d.addCallback(load_mbox_cache)
        d.addCallback(add_mailbox_if_none)
        d.addCallback(finish_initialization)

    def callWhenReady(self, cb):
        # XXX this could use adaptor.store_ready instead...??
        if self._initialized:
            cb(self)
            return defer.succeed(None)
        else:
            self._deferred_initialization.addCallback(cb)
            return self._deferred_initialization

    # XXX needed ? ------------------------------------------

    @property
    def mailboxes(self):
        """
        A list of the current mailboxes for this account.
        :rtype: set
        """
        return sorted(self.__mailboxes)

    def _load_mailboxes(self):

        def update_mailboxes(mbox_names):
            self.__mailboxes.update(mbox_names)

        d = self.adaptor.get_all_mboxes(self.store)
        d.addCallback(update_mailboxes)
        return d

    # XXX needed ? ------------------------------------------

    #
    # Public API Starts
    #

    # TODO separate into get_all_mboxes and get_all_names ?
    def list_mailboxes(self):
        d = self.adaptor.get_all_mboxes(self.store)
        return d

    def add_mailbox(self, name):
        d = self.adaptor.__class__.get_or_create(name)
        return d

    def delete_mailbox(self, name):
        d = self.adaptor.delete_mbox(self.store)
        return d

    def rename_mailbox(self, oldname, newname):
        def _rename_mbox(wrapper):
            wrapper.mbox = newname
            return wrapper.update()

        d = self.adaptor.__class__.get_or_create(oldname)
        d.addCallback(_rename_mbox)
        return d

    # FIXME yet to be decided if it belongs here...

    def get_collection_by_mailbox(self, name):
        """
        :rtype: MessageCollection
        """
        # imap select will use this, passing the collection to SoledadMailbox
        # XXX pass adaptor to MessageCollection
        # XXX keep a reference to the collection?
        # Will we need to "close" a collection? (ie, delete the object from
        # memory)
        # 1. get mailbox document
        # 2. get collection of pointers? how much memory footprint is that?
        pass

    def get_collection_by_docs(self, docs):
        """
        :rtype: MessageCollection
        """
        # get a collection of docs by a list of doc_id
        # XXX where is this doc_id set coming from?
        # XXX pass adaptor to MessageCollection
        pass

    def get_collection_by_tag(self, tag):
        """
        :rtype: MessageCollection
        """
        raise NotImplementedError()
