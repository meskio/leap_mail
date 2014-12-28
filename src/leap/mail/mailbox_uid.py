# -*- coding: utf-8 -*-
# mailbox_uid.py
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
Local tables to store the message UIDs for a given mailbox.
"""


class MailboxUID(object):
    """
    This class contains the commands needed to create, modify and alter the
    local-only UID tables for a given mailbox.
    Its purpouse is to keep a local-only index with the messages in each
    mailbox, mainly to satisfy the demands of the IMAP specification, but
    useful too for any effective listing of the messages in a mailbox.

    Since the incoming mail can be processed at any time in any replica, it's
    preferred not to attempt to maintain a global chronological global index.
    """
    # The uids are expected to be 32-bits values, but the ROWIDs in sqlite
    # are 64-bit values. I *don't* think it really matters for any
    # practical use, but it's good to remmeber we've got that difference going
    # on.

    store = None
    table_preffix = "leapmail_uid_"

    def _query(self, *args, **kw):
        assert self.store is not None
        return self.store.raw_sqlcipher_query(*args, **kw)

    def create_table(self, mailbox):
        assert mailbox
        sql = ("CREATE TABLE if not exists {preffix}{name}( "
               "uid  INTEGER PRIMARY KEY, "
               "hash TEXT UNIQUE NOT NULL)".format(
                   preffix=self.table_preffix, name=mailbox))
        return self._query(sql)

    def delete_table(self, mailbox):
        assert mailbox
        sql = ("DROP TABLE if exists {preffix}{name}".format(
            preffix=self.table_preffix, name=mailbox))
        return self._query(sql)

    def rename_table(self, oldmailbox, newmailbox):
        assert oldmailbox
        assert newmailbox
        assert oldmailbox != newmailbox
        sql = ("ALTER TABLE {preffix}{old} "
               "RENAME TO {preffix}{new}".format(
                   preffix=self.table_preffix,
                   old=oldmailbox, new=newmailbox))
        return self._query(sql)

    def insert_doc(self, mailbox, doc_id):
        # XXX assert doc_id is in the expected format
        # XXX SHOULD return the autoincremented value
        assert mailbox
        assert doc_id

        def get_rowid(result):
            return result[0][0]

        sql = ("INSERT INTO {preffix}{name} VALUES ("
               "NULL, ?)".format(
                   preffix=self.table_preffix, name=mailbox))
        lastrowid_sql = ("SELECT MAX(rowid) FROM {preffix}{name} "
                         "LIMIT 1;").format(
            preffix=self.table_preffix, name=mailbox)
        values = (doc_id,)
        d = self._query(sql, values)
        d.addCallback(lambda _: self._query(lastrowid_sql))
        d.addCallback(get_rowid)
        return d

    def delete_doc_by_uid(self, mailbox, uid):
        assert mailbox
        assert uid
        sql = ("DELETE FROM {preffix}{name} "
               "WHERE uid=?".format(
                   preffix=self.table_preffix, name=mailbox))
        values = (uid,)
        return self._query(sql, values)

    def delete_doc_by_hash(self, mailbox, doc_id):
        assert mailbox
        assert doc_id
        sql = ("DELETE FROM {preffix}{name} "
               "WHERE hash=?".format(
                   preffix=self.table_preffix, name=mailbox))
        values = (doc_id,)
        return self._query(sql, values)

    def get_doc_from_uid(self, mailbox, uid):
        def get_hash(result):
            return result[0][0]

        sql = ("SELECT hash from {preffix}{name} "
               "WHERE uid=?".format(
                   preffix=self.table_preffix, name=mailbox))
        values = (uid,)
        d = self._query(sql, values)
        d.addCallback(get_hash)
        return d

    def get_docs_from_uids(self, mailbox, uids):
        # XXX dereference the range (n,*)
        raise NotImplementedError()

    def count(self, mailbox):
        def get_count(result):
            return result[0][0]

        sql = ("SELECT Count(*) FROM {preffix}{name};".format(
            preffix=self.table_preffix, name=mailbox))
        d = self._query(sql)
        d.addCallback(get_count)
        return d
