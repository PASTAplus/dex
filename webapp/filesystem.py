import contextlib
import os
import pathlib
import re
import sys
import urllib.parse

FILENAME_SAFE_CHARS = " @$,~*&"


def get_safe_reversible_path(*path_list):
    """Escape characters that are not allowed or often cause issues when used in file-
    or directory names, then join the arguments to a filesystem path.

    This generates a string that is reversible but may not be easy to read.

    Args:
        path_list (list):

    Returns:
        (:obj:`str` or :obj:`Path`): A path safe for use as a as a file- or directory name.

    See Also:
        If a reversible path is not required, see :func:`get_safe_lossy_path`, which is
        not reversible, but may be easier to read.

        To get get the original string from the path, see :func:`get_original_path`.
    """
    # noinspection PyTypeChecker
    return os.path.join(*[get_safe_reversible_path_element(p) for p in path_list])


def get_safe_reversible_path_element(s):
    """Replace characters that are not allowed, have special semantics, or may cause
     security issues, when used in file- or directory names, with filesystem safe
     reversible codes

     On Unix, names starting with period are usually hidden in the filesystem. We don't
     want there to be a chance of generating hidden files by using this function. But
     we also don't want to escape dots in general since that makes the filenames much
     harder to read. So we escape the dot only when it's at the start of the string.

    Args:
         s (str): Any Unicode string

     Returns:
         str: A string safe for use as a file- or directory name.
    """
    out_str = urllib.parse.quote(s.encode("utf-8"), safe=FILENAME_SAFE_CHARS)
    if out_str.startswith("."):
        out_str = f"%2e{out_str[1:]}"
    return out_str


def get_original_path(*path_list):
    """Return the original path that was passed to :func:`get_safe_reversible_path`"""
    return os.path.join(*[get_original_path_element(p) for p in path_list])


def get_original_path_element(s):
    """Return the original string that was passed to
    :func:`get_safe_reversible_path_element`
    """
    return urllib.parse.unquote(s)


def get_safe_lossy_path_element(s):
    """Like :func:`get_safe_reversible_path_element`, but generates a string that is not
    reversible, instead prioritizing readability.
    """
    return pathlib.Path(re.sub("[^\w\d]+", "-", s.strip(" .\n\t/\\")))


def get_safe_lossy_path(*path_list):
    """Like :func:`get_safe_reversible_path`, but generates a string that is not
    reversible, instead prioritizing readability.
    """
    return os.path.join(*[get_safe_lossy_path_element(p) for p in path_list])


def create_missing_directories_for_file(file_path):
    """Create any directories in ``dir_path`` that do not yet exist.

    Args:
        file_path (str): Relative or absolute path to a file that may or may not exist.

            Must be a file path, as any directory element at the end of the path will
              not be created.

    See Also:
        create_missing_directories_for_dir()
    """
    create_missing_directories_for_dir(os.path.dirname(file_path))


def create_missing_directories_for_dir(dir_path):
    """Create any directories in ``dir_path`` that do not yet exist.

    Args:
        dir_path (str): Relative or absolute path to a directory that may or may not
          exist.

            Must be a directory path, as any filename element at the end of the path
              will also be created as a directory.

    See Also:
        create_missing_directories_for_file()
    """
    os.makedirs(dir_path, exist_ok=True)


def abs_path_from_base(base_path, rel_path):
    """Join a base and a relative path and return an absolute path to the resulting
    location.

    Args:
        base_path (str): Relative or absolute path to prepend to ``rel_path``.
        rel_path (str): Path relative to the location of the module file from which this
          function is called.

    Returns:
        (:obj:`str` or :obj:`Path`): Absolute path to the location specified by ``rel_path``.
    """
    # noinspection PyProtectedMember,PyUnresolvedReferences
    return os.path.abspath(
        os.path.join(
            os.path.dirname(sys._getframe(1).f_code.co_filename),
            base_path,
            rel_path,
        )
    )


def abs_path(rel_path):
    """Convert a path that is relative to the module from which this function is called,
    to an absolute path.

    Args:
        rel_path (str): Path relative to the location of the module file from which this
          function is called.

    Returns:
        (:obj:`str` or :obj:`Path`): Absolute path to the location specified by ``rel_path``.
    """
    # noinspection PyProtectedMember,PyUnresolvedReferences
    return os.path.abspath(
        os.path.join(os.path.dirname(sys._getframe(1).f_code.co_filename), rel_path)
    )


def safe_path_exists(o):
    """Check if `o` is a path to an existing file.

    ``pathlib.Path(o).is_file()`` and ``os.path.exists()`` raise various types of
    exceptions if unable to convert `o` to a value suitable for use as a path. This
    method aims to allow checking any object without raising exceptions.

    Args:
        o (object): An object that may be a path.

    Returns:
        bool: True if `o` is a path.
    """
    with contextlib.suppress(Exception):
        if isinstance(o, pathlib.Path):
            return o.is_file()
        if not isinstance(o, (str, bytes)):
            return False
        if len(o) > 1024:
            return False
        if isinstance(o, bytes):
            o = o.decode("utf-8")
        return pathlib.Path(o).is_file()
    return False
