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
import StringIO

from zope.interface import implements
from twisted.mail import imap4

from leap.common.decorators import memoized_method
from leap.common.mail import get_email_charset
from leap.mail.utils import empty, find_charset

logger = logging.getLogger(__name__)


# TODO -- use messagewrapper

class MessagePart(object):
    """
    IMessagePart implementor, to be passed to several methods
    of the IMAP4Server.
    It takes a subpart message and is able to find
    the inner parts.

    See the interface documentation.
    """

    implements(imap4.IMessagePart)

    def __init__(self, soledad, part_map):
        """
        Initializes the MessagePart.

        :param soledad: Soledad instance.
        :type soledad: Soledad
        :param part_map: a dictionary containing the parts map for this
                         message
        :type part_map: dict
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

        self._soledad = soledad
        self._pmap = part_map

    def getSize(self):
        """
        Return the total size, in octets, of this message part.

        :return: size of the message, in octets
        :rtype: int
        """
        if empty(self._pmap):
            return 0
        size = self._pmap.get('size', None)
        if size is None:
            logger.error("Message part cannot find size in the partmap")
            size = 0
        return size

    def getBodyFile(self):
        """
        Retrieve a file object containing only the body of this message.

        :return: file-like object opened for reading
        :rtype: StringIO
        """
        fd = StringIO.StringIO()
        if not empty(self._pmap):
            multi = self._pmap.get('multi')
            if not multi:
                phash = self._pmap.get("phash", None)
            else:
                pmap = self._pmap.get('part_map')
                first_part = pmap.get('1', None)
                if not empty(first_part):
                    phash = first_part['phash']
                else:
                    phash = None

            if phash is None:
                logger.warning("Could not find phash for this subpart!")
                payload = ""
            else:
                payload = self._get_payload_from_document_memoized(phash)
                if empty(payload):
                    payload = self._get_payload_from_document(phash)

        else:
            logger.warning("Message with no part_map!")
            payload = ""

        if payload:
            content_type = self._get_ctype_from_document(phash)
            charset = find_charset(content_type)
            if charset is None:
                charset = self._get_charset(payload)
            try:
                if isinstance(payload, unicode):
                    payload = payload.encode(charset)
            except UnicodeError as exc:
                logger.error(
                    "Unicode error, using 'replace'. {0!r}".format(exc))
                payload = payload.encode(charset, 'replace')

        fd.write(payload)
        fd.seek(0)
        return fd

    # TODO should memory-bound this memoize!!!
    @memoized_method
    def _get_payload_from_document_memoized(self, phash):
        """
        Memoized method call around the regular method, to be able
        to call the non-memoized method in case we got a None.

        :param phash: the payload hash to retrieve by.
        :type phash: str or unicode
        :rtype: str or unicode or None
        """
        return self._get_payload_from_document(phash)

    def _get_payload_from_document(self, phash):
        """
        Return the message payload from the content document.

        :param phash: the payload hash to retrieve by.
        :type phash: str or unicode
        :rtype: str or unicode or None
        """
        # XXX use adaptor
        return NotImplementedError()

    # TODO should memory-bound this memoize!!!
    @memoized_method
    def _get_ctype_from_document(self, phash):
        """
        Reeturn the content-type from the content document.

        :param phash: the payload hash to retrieve by.
        :type phash: str or unicode
        :rtype: str or unicode
        """
        # XXX use adaptor
        return NotImplementedError()

    @memoized_method
    def _get_charset(self, stuff):
        # TODO put in a common class with LeapMessage
        """
        Gets (guesses?) the charset of a payload.

        :param stuff: the stuff to guess about.
        :type stuff: str or unicode
        :return: charset
        :rtype: unicode
        """
        # XXX existential doubt 2. shouldn't we make the scope
        # of the decorator somewhat more persistent?
        # ah! yes! and put memory bounds.
        return get_email_charset(stuff)

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
        if empty(self._pmap):
            logger.warning("Could not get part map!")
            return False
        multi = self._pmap.get("multi", False)
        return multi

    def getSubPart(self, part):
        """
        Retrieve a MIME submessage

        :type part: C{int}
        :param part: The number of the part to retrieve, indexed from 0.
        :raise IndexError: Raised if the specified part does not exist.
        :raise TypeError: Raised if this message is not multipart.
        :rtype: Any object implementing C{IMessagePart}.
        :return: The specified sub-part.
        """
        if not self.isMultipart():
            raise TypeError

        sub_pmap = self._pmap.get("part_map", {})
        try:
            part_map = sub_pmap[str(part + 1)]
        except KeyError:
            logger.debug("getSubpart for %s: KeyError" % (part,))
            raise IndexError

        # XXX check for validity
        return MessagePart(self._soledad, part_map)
