import os
import socket
import tempfile
import unittest
from pathlib import Path

from loom_core.errors import LoomError
from loom_core.service import Caller

from loom_service.peer import MAX_CHAIN, caller_of, identify, peer_credentials

SERVER = 10
NOBODY = b'HOME=/home/user\0PATH=/usr/bin\0'


def agent(thread_id: str = 't1') -> bytes:
    return b'HOME=/home/user\0LOOM_THREAD=' + thread_id.encode() + b'\0PATH=/usr/bin\0'


class ProcTree:
    def __init__(self, root: Path) -> None:
        self.root = root

    def add(self, pid: int, ppid: int, environ: bytes | None = NOBODY, name: str = 'python',
            stat: bool = True) -> None:
        directory = self.root / str(pid)
        directory.mkdir()
        if stat:
            (directory / 'stat').write_text(f'{pid} ({name}) S {ppid} {pid} {pid} 0 -1 4194560 0')
        if environ is not None:
            (directory / 'environ').write_bytes(environ)

    def chain(self, *pids: int, environs: dict[int, bytes | None] | None = None) -> None:
        environs = environs or {}
        for pid, ppid in zip(pids, (*pids[1:], 0)):
            self.add(pid, ppid, environs.get(pid, NOBODY))

    def caller(self, pid: int, server_pid: int = SERVER) -> Caller:
        return caller_of(pid, self.root, server_pid)


class PeerRuleTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tree = ProcTree(Path(tmp.name))

    def test_the_peer_itself_with_loom_thread(self) -> None:
        self.tree.chain(100, SERVER, 1, environs={100: agent('t-peer')})
        self.assertEqual(self.tree.caller(100), Caller(thread_id='t-peer'))

    def test_an_ancestor_below_the_server_with_loom_thread(self) -> None:
        self.tree.chain(100, 60, 50, SERVER, 1, environs={50: agent('t-agent'), 60: NOBODY})
        self.assertEqual(self.tree.caller(100), Caller(thread_id='t-agent'))

    def test_the_nearest_agent_below_the_server_wins(self) -> None:
        self.tree.chain(100, 60, 50, SERVER, 1, environs={60: agent('t-near'), 50: agent('t-far')})
        self.assertEqual(self.tree.caller(100), Caller(thread_id='t-near'))

    def test_an_ancestor_at_or_above_the_server_is_not_an_agent(self) -> None:
        self.tree.chain(100, SERVER, 5, 1, environs={SERVER: agent('t-server'), 5: agent('t-up')})
        self.assertEqual(self.tree.caller(100), Caller())

    def test_a_chain_that_never_reaches_the_server_looks_only_at_the_peer(self) -> None:
        self.tree.chain(100, 50, 1, environs={50: agent('t-agent')})
        self.assertEqual(self.tree.caller(100), Caller())
        self.tree.add(200, 50, agent('t-peer'))
        self.assertEqual(self.tree.caller(200), Caller(thread_id='t-peer'))

    def test_an_empty_loom_thread_is_not_an_agent(self) -> None:
        self.tree.chain(100, SERVER, 1, environs={100: b'LOOM_THREAD=\0PATH=/usr/bin\0'})
        self.assertEqual(self.tree.caller(100), Caller())

    def test_a_later_non_empty_loom_thread_counts(self) -> None:
        self.tree.chain(100, SERVER, 1, environs={100: b'LOOM_THREAD=\0LOOM_THREAD=t2\0'})
        self.assertEqual(self.tree.caller(100), Caller(thread_id='t2'))

    def test_a_similar_name_is_not_loom_thread(self) -> None:
        self.tree.chain(100, SERVER, 1, environs={100: b'XLOOM_THREAD=t1\0LOOM_THREADS=t1\0'})
        self.assertEqual(self.tree.caller(100), Caller())

    def test_a_command_name_with_parens_and_spaces_parses(self) -> None:
        self.tree.add(100, 50, name='evil) (S 1 2 3')
        self.tree.add(50, SERVER, agent('t-agent'), name=') ) (')
        self.tree.add(SERVER, 1)
        self.tree.add(1, 0)
        self.assertEqual(self.tree.caller(100), Caller(thread_id='t-agent'))

    def test_a_missing_peer_stat_is_unverified(self) -> None:
        self.tree.add(100, SERVER, agent(), stat=False)
        self.tree.add(SERVER, 1)
        self.assertEqual(self.tree.caller(100), Caller(verified=False))

    def test_a_missing_peer_environ_is_unverified(self) -> None:
        self.tree.add(100, SERVER, None)
        self.tree.add(SERVER, 1)
        self.assertEqual(self.tree.caller(100), Caller(verified=False))

    def test_a_garbled_peer_stat_is_unverified(self) -> None:
        self.tree.add(100, SERVER)
        (self.tree.root / '100' / 'stat').write_text('100 (python')
        self.assertEqual(self.tree.caller(100), Caller(verified=False))

    def test_peer_pid_zero_is_unverified(self) -> None:
        self.assertEqual(self.tree.caller(0), Caller(verified=False))

    def test_an_unreadable_ancestor_environ_is_skipped(self) -> None:
        self.tree.chain(100, 60, 50, SERVER, 1, environs={60: None, 50: agent('t-agent')})
        self.assertEqual(self.tree.caller(100), Caller(thread_id='t-agent'))
        (self.tree.root / '60' / 'environ').mkdir()
        self.assertEqual(self.tree.caller(100), Caller(thread_id='t-agent'))

    def test_an_unreadable_ancestor_stat_ends_the_chain(self) -> None:
        self.tree.add(100, 50)
        self.tree.add(50, 40, agent('t-agent'), stat=False)
        self.tree.add(40, SERVER)
        self.tree.add(SERVER, 1)
        self.assertEqual(self.tree.caller(100), Caller())

    def test_a_loop_stops(self) -> None:
        self.tree.add(100, 50)
        self.tree.add(50, 40, agent('t-agent'))
        self.tree.add(40, 100)
        self.assertEqual(self.tree.caller(100), Caller())

    def test_the_chain_stops_after_64_processes(self) -> None:
        pids = list(range(1000, 1000 + 2 * MAX_CHAIN))
        self.tree.chain(*pids, environs={1001: agent('t-agent')})
        self.assertEqual(MAX_CHAIN, 64)
        self.assertEqual(self.tree.caller(1000, server_pid=pids[MAX_CHAIN - 1]),
                         Caller(thread_id='t-agent'))
        self.assertEqual(self.tree.caller(1000, server_pid=pids[MAX_CHAIN]), Caller())

    def test_the_server_as_its_own_peer_is_a_person(self) -> None:
        self.tree.chain(SERVER, 1, environs={SERVER: agent('t-server')})
        self.assertEqual(self.tree.caller(SERVER), Caller())


class CredentialsTest(unittest.TestCase):
    def test_peer_credentials_read_so_peercred(self) -> None:
        first, second = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        with first, second:
            self.assertEqual(peer_credentials(first), (os.getpid(), os.geteuid(), os.getegid()))

    def test_a_peer_of_another_user_is_forbidden(self) -> None:
        with self.assertRaises(LoomError) as caught:
            identify(os.getpid(), os.geteuid() + 1)
        self.assertEqual(caught.exception.code, 'forbidden')
        with self.assertRaises(LoomError):
            identify(os.getpid(), 1000, own_uid=1001)

    def test_a_peer_of_the_same_user_is_identified(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tree = ProcTree(Path(tmp))
            tree.chain(100, SERVER, 1, environs={100: agent('t-peer')})
            self.assertEqual(identify(100, 1000, own_uid=1000, proc=tmp, server_pid=SERVER),
                             Caller(thread_id='t-peer'))

    def test_the_real_proc_sees_this_process(self) -> None:
        caller = caller_of(os.getpid(), server_pid=os.getpid())
        self.assertEqual(caller, Caller())


if __name__ == '__main__':
    unittest.main()
