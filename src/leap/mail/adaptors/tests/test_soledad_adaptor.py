# -*- coding: utf-8 -*-
# test_soledad_adaptor.py
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
Tests for the Soledad Adaptor module - leap.mail.adaptors.soledad
"""
import os
import shutil
import tempfile

from twisted.internet import defer
from twisted.trial import unittest

from leap.common.testing.basetest import BaseLeapTest
from leap.mail.adaptors import models
from leap.mail.adaptors.soledad import SoledadDocumentWrapper
from leap.soledad.client import Soledad

TEST_USER = "testuser@leap.se"
TEST_PASSWD = "1234"


def initialize_soledad(email, gnupg_home, tempdir):
    """
    Initializes soledad by hand

    :param email: ID for the user
    :param gnupg_home: path to home used by gnupg
    :param tempdir: path to temporal dir
    :rtype: Soledad instance
    """

    uuid = "foobar-uuid"
    passphrase = u"verysecretpassphrase"
    secret_path = os.path.join(tempdir, "secret.gpg")
    local_db_path = os.path.join(tempdir, "soledad.u1db")
    server_url = "https://provider"
    cert_file = ""

    soledad = Soledad(
        uuid,
        passphrase,
        secret_path,
        local_db_path,
        server_url,
        cert_file,
        syncable=False)

    return soledad


# TODO move to common module
# XXX remove duplication
class SoledadTestMixin(BaseLeapTest):
    """
    It is **VERY** important that this base is added *AFTER* unittest.TestCase
    """

    def setUp(self):
        self.results = []

        self.old_path = os.environ['PATH']
        self.old_home = os.environ['HOME']
        self.tempdir = tempfile.mkdtemp(prefix="leap_tests-")
        self.home = self.tempdir
        bin_tdir = os.path.join(
            self.tempdir,
            'bin')
        os.environ["PATH"] = bin_tdir
        os.environ["HOME"] = self.tempdir

        # Soledad: config info
        self.gnupg_home = "%s/gnupg" % self.tempdir
        self.email = 'leap@leap.se'

        # initialize soledad by hand so we can control keys
        self._soledad = initialize_soledad(
            self.email,
            self.gnupg_home,
            self.tempdir)

    def tearDown(self):
        """
        tearDown method called after each test.
        """
        self.results = []
        try:
            self._soledad.close()
        except Exception:
            print "DEBUG ME: ERROR WHILE CLOSING SOLEDAD :("
        finally:
            os.environ["PATH"] = self.old_path
            os.environ["HOME"] = self.old_home
            # safety check
            assert 'leap_tests-' in self.tempdir
            shutil.rmtree(self.tempdir)


class CounterWrapper(SoledadDocumentWrapper):
    class model(models.SerializableModel):
        counter = 0
        flag = None


class SoledadDocWrapperTestCase(unittest.TestCase, SoledadTestMixin):

    def test_multiple_updates(self):

        store = self._soledad
        wrapper = CounterWrapper(counter=1)
        MAX = 100

        def assert_doc_id(doc):
            self.assertTrue(wrapper._doc_id is not None)
            return doc

        def assert_counter_initial_ok(doc):
            self.assertEqual(wrapper.counter, 1)

        def increment_counter(ignored):
            d1 = []

            def record_revision(revision):
                rev = int(revision.split(':')[1])
                self.results.append(rev)

            for i in list(range(MAX)):
                wrapper.counter += 1
                wrapper.flag = i % 2 == 0
                d = wrapper.update(store)
                d.addCallback(record_revision)
                d1.append(d)

            return defer.gatherResults(d1)

        def assert_counter_final_ok(doc):
            self.assertEqual(doc.content['counter'], MAX + 1)
            self.assertEqual(doc.content['flag'], False)

        def assert_results_ordered_list(ignored):
            self.assertEqual(self.results, sorted(range(2, MAX + 2)))

        d = wrapper.create(store)
        d.addCallback(assert_doc_id)
        d.addCallback(assert_counter_initial_ok)
        d.addCallback(increment_counter)
        d.addCallback(lambda _: store.get_doc(wrapper._doc_id))
        d.addCallback(assert_counter_final_ok)
        d.addCallback(assert_results_ordered_list)
        return d
