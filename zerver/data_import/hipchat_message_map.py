import glob
import json
import logging
import os

from typing import Callable, Dict, List, Tuple

'''
This is kind of a hacky thing we need to do to
get message ids ordered by time for HipChat.

A couple things are at play here:

    * Zulip assumes messages are implicitly
      in time order by message id.  This makes
      queries faster, and in a pure Zulip
      situation, it just falls out naturally
      that ids are written in order, so it's
      an easy invariant to preserve.

    * Hipchat uses uuid's for their message ids,
      so we can't simply borrow their ids to
      get the same semantics.

    * The message tables are pretty huge, so
      the import utility likes to have N
      message files, and we therefore write
      N message files during the Hipchat conversion.
      Having all the little files, and dealing
      with batches in general, makes it a bit
      tricky to re-sort ids after the fact.  Also,
      message_id is a foreign key in other tables.

Because of all the above, and maybe just some
expediency, we just sweep all the Hipchat files
that have messages pretty early in the process
to create a mapping of Hipchat id -> Zulip id,
which we'll use later.
'''

def create_message_id_map(data_dir: str,
                          date_to_float: Callable[[str], float]) -> Dict[str, int]:
    logging.info('Starting to build message id map.')
    rooms_glob = os.path.join(data_dir, 'rooms', '*', 'history.json')
    user_glob = os.path.join(data_dir, 'users', '*', 'history.json')
    all_files = glob.glob(rooms_glob) + glob.glob(user_glob)

    tups = []  # List[Tuple[float, str]]

    for fn in all_files:
        with open(fn) as f:
            data = json.load(f)

        for item in data:
            key = list(item.keys())[0]
            if key == 'PrivateUserMessage':
                continue

            assert(key in ['PrivateUserMessage',
                           'NotificationMessage',
                           'TopicRoomMessage',
                           'GuestAccessMessage',
                           'UserMessage'])
            msg = item[key]
            pub_date = date_to_float(msg['timestamp']),
            hipchat_id = msg['id']
            tups.append((pub_date, hipchat_id))

    logging.info('Sorting message dates')
    tups.sort()

    id_map = dict()

    for i, (pub_date, hipchat_id) in enumerate(tups):
        zulip_id = i + 1
        if hipchat_id in id_map:
            raise Exception('non-unique hipchat_id: ' + hipchat_id)
        id_map[hipchat_id] = zulip_id

    logging.info('Done making message map')

    return id_map
