# -*- coding: utf-8 -*-
# test_mailbox_uid.py
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
Tests for the mailbox_uid module.
"""
from functools import partial

from twisted.trial import unittest

from leap.mail import mailbox_uid
from leap.mail.tests.common import SoledadTestMixin

hash_test0 = '590c9f8430c7435807df8ba9a476e3f1295d46ef210f6efae2043a4c085a569e'
hash_test1 = '1b4f0e9851971998e732078544c96b36c3d01cedf7caa332359d6f1d83567014'
hash_test2 = '60303ae22b998861bce3b28f33eec1be758a213c86c93c076dbe9f558c11c752'
hash_test3 = 'fd61a03af4f77d870fc21e05e7e80678095c92d808cfb3b5c279ee04c74aca13'
hash_test4 = 'a4e624d686e03ed2767c0abd85c14426b0b1157d2ce81d27bb4fe4f6f01d688a'


class MailboxUIDTestCase(unittest.TestCase, SoledadTestMixin):
    """
    Tests for the MailboxUID class.
    """
    def get_mbox_uid(self):
        m_uid = mailbox_uid.MailboxUID()
        m_uid.store = self._soledad
        return m_uid

    def list_mail_tables_cb(self, ignored):
        def filter_mailuid_tables(tables):
            filtered = [
                table[0] for table in tables if
                table[0].startswith(mailbox_uid.MailboxUID.table_preffix)]
            return filtered

        sql = "SELECT name FROM sqlite_master WHERE type='table';"
        d = self._soledad.raw_sqlcipher_query(sql)
        d.addCallback(filter_mailuid_tables)
        return d

    def select_uid_rows(self, mailbox):
        sql = "SELECT * FROM %s%s;" % (
            mailbox_uid.MailboxUID.table_preffix, mailbox)
        d = self._soledad.raw_sqlcipher_query(sql)
        return d

    def test_create_table(self):
        def assert_table_created(tables):
            self.assertEqual(
                tables, ["leapmail_uid_inbox"])

        m_uid = self.get_mbox_uid()
        d = m_uid.create_table('inbox')
        d.addCallback(self.list_mail_tables_cb)
        d.addCallback(assert_table_created)
        return d

    def test_create_and_delete_table(self):
        def assert_table_deleted(tables):
            self.assertEqual(tables, [])

        m_uid = self.get_mbox_uid()
        d = m_uid.create_table('inbox')
        d.addCallback(lambda _: m_uid.delete_table('inbox'))
        d.addCallback(self.list_mail_tables_cb)
        d.addCallback(assert_table_deleted)
        return d

    def test_rename_table(self):
        def assert_table_renamed(tables):
            self.assertEqual(
                tables, ["leapmail_uid_foomailbox"])

        m_uid = self.get_mbox_uid()
        d = m_uid.create_table('inbox')
        d.addCallback(lambda _: m_uid.rename_table('inbox', 'foomailbox'))
        d.addCallback(self.list_mail_tables_cb)
        d.addCallback(assert_table_renamed)
        return d

    def test_insert_doc(self):
        m_uid = self.get_mbox_uid()
        mbox = 'foomailbox'

        def assert_uid_rows(rows):
            expected = [(1, hash_test0),
                        (2, hash_test1),
                        (3, hash_test2),
                        (4, hash_test3),
                        (5, hash_test4)]
            self.assertEquals(rows, expected)

        d = m_uid.create_table(mbox)
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test0))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test1))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test2))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test3))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test4))
        d.addCallback(lambda _: self.select_uid_rows(mbox))
        d.addCallback(assert_uid_rows)
        return d

    def test_insert_doc_return(self):
        m_uid = self.get_mbox_uid()
        mbox = 'foomailbox'

        def assert_rowid(rowid, expected=None):
            self.assertEqual(rowid, expected)

        d = m_uid.create_table(mbox)
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test0))
        d.addCallback(partial(assert_rowid, expected=1))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test1))
        d.addCallback(partial(assert_rowid, expected=2))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test2))
        d.addCallback(partial(assert_rowid, expected=3))
        return d

    def test_delete_doc(self):
        m_uid = self.get_mbox_uid()
        mbox = 'foomailbox'

        def assert_uid_rows(rows):
            expected = [(4, hash_test3),
                        (5, hash_test4)]
            self.assertEquals(rows, expected)

        d = m_uid.create_table(mbox)
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test0))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test1))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test2))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test3))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test4))

        d.addCallbacks(lambda _: m_uid.delete_doc_by_uid(mbox, 1))
        d.addCallbacks(lambda _: m_uid.delete_doc_by_uid(mbox, 2))
        d.addCallbacks(lambda _: m_uid.delete_doc_by_hash(mbox, hash_test2))

        d.addCallback(lambda _: self.select_uid_rows(mbox))
        d.addCallback(assert_uid_rows)
        return d

    def test_get_doc_from_uid(self):
        m_uid = self.get_mbox_uid()
        mbox = 'foomailbox'

        def assert_doc_hash(res):
            self.assertEqual(res, hash_test0)

        d = m_uid.create_table(mbox)
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test0))
        d.addCallback(lambda _: m_uid.get_doc_from_uid(mbox, 1))
        d.addCallback(assert_doc_hash)
        return d

    def test_count(self):
        m_uid = self.get_mbox_uid()
        mbox = 'foomailbox'
        d = m_uid.create_table(mbox)
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test0))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test1))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test2))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test3))
        d.addCallback(lambda _: m_uid.insert_doc(mbox, hash_test4))

        def assert_count_after_inserts(count):
            self.assertEquals(count, 5)

        d.addCallback(lambda _: m_uid.count(mbox))
        d.addCallback(assert_count_after_inserts)

        d.addCallbacks(lambda _: m_uid.delete_doc_by_uid(mbox, 1))
        d.addCallbacks(lambda _: m_uid.delete_doc_by_uid(mbox, 2))

        def assert_count_after_deletions(count):
            self.assertEquals(count, 3)

        d.addCallback(lambda _: m_uid.count(mbox))
        d.addCallback(assert_count_after_deletions)
        return d
