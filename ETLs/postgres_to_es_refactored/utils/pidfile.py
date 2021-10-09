import errno
import os


class AlreadyRunningError(Exception):
    """Process is already running."""


class InvalidPIDFileError(Exception):
    """Non-integer PID file value (possibly existing not-pidfile)."""


class PIDFile:
    """Process ID file manager."""

    def __init__(self, file_path: str):
        self.file_path = file_path

    def __enter__(self):
        """Try enter a context by checking and creating a pidfile."""
        if self.isRunning():
            raise AlreadyRunningError()
        self.write(os.getpid())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit a context by removing pidfile."""
        self.remove()

    def read(self) -> int:
        """Read contents of pidfile as a single integer."""
        with open(self.file_path) as pf:
            return int(pf.read())

    def write(self, pid: int):
        """Write process ID to pidfile."""
        with open(self.file_path, 'w') as pf:
            pf.write(str(pid))

    def remove(self):
        """Delete pidfile."""
        os.remove(self.file_path)

    def isRunning(self) -> bool:
        """Check whether process with ID from pidfile is running."""
        try:
            pid = self.read()
        except OSError as e:
            if e.errno == errno.ENOENT:
                return False  # this is the only process
        except ValueError:
            raise InvalidPIDFileError()

        try:
            # with signal 0 we check whether process is running
            # and we have permission to send signals to it
            os.kill(pid, 0)
        except OSError as e:
            if e.errno == errno.ESRCH:
                return False  # no such process (pidfile was not removed or something)
            elif e.errno == errno.EPERM:
                return True  # there is other process running
            else:
                raise
        else:
            return True  # this is the current process
