"""Windows interactive-login identity; no token creation, credential storage or elevation."""

import ctypes
from ctypes import wintypes as w
import os


class SessionError(ValueError):
    """Session identity cannot be established unambiguously."""


def _windows():
    """Load native APIs only when explicitly inspecting a Windows session."""
    if os.name != 'nt':
        raise SessionError('Windows interactive session required')
    return ctypes.WinDLL('wtsapi32', use_last_error=True)


class _Session(ctypes.Structure):
    _fields_ = [('id', w.DWORD), ('station', w.LPWSTR), ('state', ctypes.c_int)]


class _Info(ctypes.Structure):
    _fields_ = [('state', ctypes.c_int), ('id', w.DWORD),
                ('incoming', w.DWORD), ('outgoing', w.DWORD),
                ('in_frames', w.DWORD), ('out_frames', w.DWORD),
                ('in_compressed', w.DWORD), ('out_compressed', w.DWORD),
                ('station', w.WCHAR * 32), ('domain', w.WCHAR * 17),
                ('user', w.WCHAR * 21), ('connect', ctypes.c_longlong),
                ('disconnect', ctypes.c_longlong), ('last_input', ctypes.c_longlong),
                ('logon', ctypes.c_longlong), ('now', ctypes.c_longlong)]


def account_sid(account):
    """Resolve an exact local/domain account to its SID without looking up credentials."""
    _windows()
    api = ctypes.WinDLL('advapi32', use_last_error=True)
    api.LookupAccountNameW.argtypes = [w.LPCWSTR, w.LPCWSTR, ctypes.c_void_p,
        ctypes.POINTER(w.DWORD), w.LPWSTR, ctypes.POINTER(w.DWORD), ctypes.POINTER(w.DWORD)]
    api.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(w.LPWSTR)]
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    size, domain_size, kind = w.DWORD(), w.DWORD(), w.DWORD()
    api.LookupAccountNameW(None, account, None, ctypes.byref(size), None, ctypes.byref(domain_size), ctypes.byref(kind))
    if not size.value:
        raise SessionError('Account SID unavailable')
    sid, domain = ctypes.create_string_buffer(size.value), ctypes.create_unicode_buffer(domain_size.value)
    if not api.LookupAccountNameW(None, account, sid, ctypes.byref(size), domain, ctypes.byref(domain_size), ctypes.byref(kind)):
        raise SessionError('Account SID unavailable')
    text = w.LPWSTR()
    if not api.ConvertSidToStringSidW(sid, ctypes.byref(text)):
        raise SessionError('Account SID conversion failed')
    try:
        return text.value
    finally:
        kernel.LocalFree(text)


def sessions(expected_sid):
    """Return active/disconnected logins owned by the approved SID, not RunAs processes."""
    api = _windows()
    api.WTSEnumerateSessionsW.argtypes = [w.HANDLE, w.DWORD, w.DWORD,
        ctypes.POINTER(ctypes.POINTER(_Session)), ctypes.POINTER(w.DWORD)]
    api.WTSQuerySessionInformationW.argtypes = [w.HANDLE, w.DWORD, ctypes.c_int,
        ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(w.DWORD)]
    api.WTSFreeMemory.argtypes = [ctypes.c_void_p]
    entries, count = ctypes.POINTER(_Session)(), w.DWORD()
    if not api.WTSEnumerateSessionsW(None, 0, 1, ctypes.byref(entries), ctypes.byref(count)):
        raise SessionError('Session inventory unavailable')
    found = []
    try:
        for entry in entries[:count.value]:
            if entry.id <= 0 or entry.state not in (0, 4):
                continue
            buffer, size = ctypes.c_void_p(), w.DWORD()
            if not api.WTSQuerySessionInformationW(None, entry.id, 24, ctypes.byref(buffer), ctypes.byref(size)):
                raise SessionError('Session login metadata unavailable')
            try:
                if size.value < ctypes.sizeof(_Info):
                    raise SessionError('Incomplete session login metadata')
                info = ctypes.cast(buffer, ctypes.POINTER(_Info)).contents
                if not info.user:
                    continue
                if account_sid(info.domain + '\\' + info.user) == expected_sid:
                    if info.logon <= 0:
                        raise SessionError('Session login time unavailable')
                    found.append({'sid': expected_sid, 'session_id': entry.id, 'logon': info.logon})
            finally:
                api.WTSFreeMemory(buffer)
    finally:
        api.WTSFreeMemory(entries)
    return found


def selected_session(expected_sid):
    """Require exactly one approved interactive login, including a disconnected desktop."""
    found = sessions(expected_sid)
    if len(found) != 1:
        raise SessionError('Exactly one logged-in runtime desktop is required')
    return found[0]


def process_session(pid):
    """Read a process session ID, rejecting missing or inaccessible identities."""
    _windows()
    api = ctypes.WinDLL('kernel32', use_last_error=True)
    api.ProcessIdToSessionId.argtypes = [w.DWORD, ctypes.POINTER(w.DWORD)]
    session = w.DWORD()
    if not api.ProcessIdToSessionId(pid, ctypes.byref(session)):
        raise SessionError('Process session unavailable')
    return session.value


def assert_account(expected_sid):
    """Require the fixed nonadministrator account; process ownership alone is not a desktop."""
    import psutil
    if account_sid(psutil.Process().username()) != expected_sid:
        raise SessionError('Use the approved runtime account')
    api = ctypes.WinDLL('shell32', use_last_error=True)
    if api.IsUserAnAdmin():
        raise SessionError('Elevated workers are forbidden')


def assert_current(binding):
    """Recheck login generation and process placement immediately before native connection."""
    assert_account(binding['sid'])
    if selected_session(binding['sid']) != binding or process_session(os.getpid()) != binding['session_id']:
        raise SessionError('Worker must run inside the approved runtime login')
