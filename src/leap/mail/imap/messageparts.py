# messageparts.py
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
MessagePart implementation. Used from IMAPMessage.
"""
import logging

from zope.interface import implements
from twisted.mail import imap4

logger = logging.getLogger(__name__)


class MessagePart(object):
    """
    IMessagePart implementor, to be passed to several methods
    of the IMAP4Server.
    It takes a subpart message and is able to find
    the inner parts.

    See the interface documentation.
    """

    implements(imap4.IMessagePart)

    def __init__(self, msgpart):
        """
        Initializes the MessagePart.
        """
        # TODO
        # It would be good to pass the uid/mailbox also
        # for references while debugging.

        # We have a problem on bulk moves, and is
        # that when the fetch on the new mailbox is done
        # the parts maybe are not complete.
        # So we should be able to fail with empty
        # docs until we solve that. The ideal would be
        # to gather the results of the deferred operations
        # to signal the operation is complete.

        self.msgpart = msgpart

    def getSize(self):
        """
        Return the total size, in octets, of this message part.

        :return: size of the message, in octets
        :rtype: int
        """
        return self.msgpart.get_size()

    def getBodyFile(self):
        """
        Retrieve a file object containing only the body of this message.

        :return: file-like object opened for reading
        :rtype: StringIO
        """
        return self.msgpart.get_body_file()

        #fd = StringIO.StringIO()
        #if not empty(self._pmap):
            #multi = self._pmap.get('multi')
            #if not multi:
                #phash = self._pmap.get("phash", None)
            #else:
                #pmap = self._pmap.get('part_map')
                #first_part = pmap.get('1', None)
                #if not empty(first_part):
                    #phash = first_part['phash']
                #else:
                    #phash = None
#
            #if phash is None:
                #logger.warning("Could not find phash for this subpart!")
                #payload = ""
            #else:
                #payload = self._get_payload_from_document_memoized(phash)
                #if empty(payload):
                    #payload = self._get_payload_from_document(phash)
#
        #else:
            #logger.warning("Message with no part_map!")
            #payload = ""
#
        #if payload:
            #content_type = self._get_ctype_from_document(phash)
            #charset = find_charset(content_type)
            #if charset is None:
                #charset = self._get_charset(payload)
            #try:
                #if isinstance(payload, unicode):
                    #payload = payload.encode(charset)
            #except UnicodeError as exc:
                #logger.error(
                    #"Unicode error, using 'replace'. {0!r}".format(exc))
                #payload = payload.encode(charset, 'replace')
#
        #fd.write(payload)
        #fd.seek(0)
        #return fd

    # FIXME ---
    def getHeaders(self, negate, *names):
        """
        Retrieve a group of message headers.

        :param names: The names of the headers to retrieve or omit.
        :type names: tuple of str

        :param negate: If True, indicates that the headers listed in names
                       should be omitted from the return value, rather
                       than included.
        :type negate: bool

        :return: A mapping of header field names to header field values
        :rtype: dict
        """
        # XXX refactor together with MessagePart method
        if not self._pmap:
            logger.warning("No pmap in Subpart!")
            return {}
        headers = dict(self._pmap.get("headers", []))

        names = map(lambda s: s.upper(), names)
        if negate:
            cond = lambda key: key.upper() not in names
        else:
            cond = lambda key: key.upper() in names

        # default to most likely standard
        charset = find_charset(headers, "utf-8")
        headers2 = dict()
        for key, value in headers.items():
            # twisted imap server expects *some* headers to be lowercase
            # We could use a CaseInsensitiveDict here...
            if key.lower() == "content-type":
                key = key.lower()

            if not isinstance(key, str):
                key = key.encode(charset, 'replace')
            if not isinstance(value, str):
                value = value.encode(charset, 'replace')

            # filter original dict by negate-condition
            if cond(key):
                headers2[key] = value
        return headers2

    def isMultipart(self):
        """
        Return True if this message is multipart.
        """
        return self.msgpart.is_multipart()

    def getSubPart(self, part):
        """
        Retrieve a MIME submessage
        """
        return MessagePart(self.msgpart.get_subpart(part))
