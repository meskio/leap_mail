# -*- coding: utf-8 -*-
# soledad_indexes.py
# Copyright (C) 2013, 2014 LEAP
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
Fields for Mailbox and Message.
"""

# TODO
# [ ] hide most of the constants here

# Document Type, for indexing

TYPE_KEY = "type"
TYPE_MBOX_VAL = "mbox"
TYPE_FLAGS_VAL = "flags"
TYPE_HEADERS_VAL = "head"
TYPE_CONTENT_VAL = "cnt"
TYPE_RECENT_VAL = "rct"
TYPE_HDOCS_SET_VAL = "hdocset"

INCOMING_KEY = "incoming"
ERROR_DECRYPTING_KEY = "errdecr"

# indexing keys
CONTENT_HASH_KEY = "chash"
PAYLOAD_HASH_KEY = "phash"
MSGID_KEY = "msgid"
UID_KEY = "uid"


# Index  types
# --------------

TYPE_IDX = 'by-type'
TYPE_MBOX_IDX = 'by-type-and-mbox'
TYPE_MBOX_UID_IDX = 'by-type-and-mbox-and-uid'
TYPE_SUBS_IDX = 'by-type-and-subscribed'
TYPE_MSGID_IDX = 'by-type-and-message-id'
TYPE_MBOX_SEEN_IDX = 'by-type-and-mbox-and-seen'
TYPE_MBOX_RECT_IDX = 'by-type-and-mbox-and-recent'
TYPE_MBOX_DEL_IDX = 'by-type-and-mbox-and-deleted'
TYPE_MBOX_C_HASH_IDX = 'by-type-and-mbox-and-contenthash'
TYPE_C_HASH_IDX = 'by-type-and-contenthash'
TYPE_C_HASH_PART_IDX = 'by-type-and-contenthash-and-partnumber'
TYPE_P_HASH_IDX = 'by-type-and-payloadhash'

# Soledad index for incoming mail, without decrypting errors.
# and the backward-compatible index, will be deprecated at 0.7
JUST_MAIL_IDX = "just-mail"
JUST_MAIL_COMPAT_IDX = "just-mail-compat"

# Tomas created the `recent and seen index`, but the semantic is not too
# correct since the recent flag is volatile --- XXX review and delete.
TYPE_MBOX_RECT_SEEN_IDX = 'by-type-and-mbox-and-recent-and-seen'

KTYPE = TYPE_KEY
MBOX_VAL = TYPE_MBOX_VAL
CHASH_VAL = CONTENT_HASH_KEY
PHASH_VAL = PAYLOAD_HASH_KEY

MAIL_INDEXES = {
    # generic
    TYPE_IDX: [KTYPE],
    TYPE_MBOX_IDX: [KTYPE, MBOX_VAL],
    TYPE_MBOX_UID_IDX: [KTYPE, MBOX_VAL, UID_KEY],

    # mailboxes
    TYPE_SUBS_IDX: [KTYPE, 'bool(subscribed)'],

    # fdocs uniqueness
    TYPE_MBOX_C_HASH_IDX: [KTYPE, MBOX_VAL, CHASH_VAL],

    # headers doc - search by msgid.
    TYPE_MSGID_IDX: [KTYPE, MSGID_KEY],

    # content, headers doc
    TYPE_C_HASH_IDX: [KTYPE, CHASH_VAL],

    # attachment payload dedup
    TYPE_P_HASH_IDX: [KTYPE, PHASH_VAL],

    # messages
    TYPE_MBOX_SEEN_IDX: [KTYPE, MBOX_VAL, 'bool(seen)'],
    TYPE_MBOX_RECT_IDX: [KTYPE, MBOX_VAL, 'bool(recent)'],
    TYPE_MBOX_DEL_IDX: [KTYPE, MBOX_VAL, 'bool(deleted)'],
    TYPE_MBOX_RECT_SEEN_IDX: [KTYPE, MBOX_VAL,
                              'bool(recent)', 'bool(seen)'],

    # incoming queue
    JUST_MAIL_IDX: [INCOMING_KEY,
                    "bool(%s)" % (ERROR_DECRYPTING_KEY,)],

    # the backward-compatible index, will be deprecated at 0.7
    JUST_MAIL_COMPAT_IDX: [INCOMING_KEY],
}
