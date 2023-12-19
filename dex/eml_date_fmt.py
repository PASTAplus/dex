"""Given a datetime format string from an EML document, return a parser and formatter
for the format.
"""
import datetime
import functools
import logging
import re

log = logging.getLogger(__name__)


def get_datetime_parser_and_formatter(col_name, iso_str):
    date_format_fn_dict = EML_DATE_FORMAT_TO_CUSTOM_DATETIME_PARSER_DICT.get(iso_str)
    if date_format_fn_dict:
        return mk_fn_dict2(date_format_fn_dict)

    date_format_c_str = EML_DATE_FORMAT_TO_CTIME_DICT.get(iso_str)
    if date_format_c_str:
        return mk_fn_dict(date_format_c_str)

    date_format_c_str = iso8601_to_c_format(iso_str)
    if date_format_c_str:
        return mk_fn_dict(date_format_c_str)

    # For many "year" columns, there is no date format string in the EML. In
    # these cases, we make a guess at the format.
    if col_name.upper() == 'YEAR':
        return 'YYYY'


def mk_fn_dict(c_format_str):
    def fn_wrapper(dt, fn):
        try:
            return fn(dt, c_format_str)
        except Exception:
            return None

    return {
        'parser': functools.partial(fn_wrapper, fn=datetime.datetime.strptime),
        'formatter': functools.partial(fn_wrapper, fn=datetime.datetime.strftime),
    }


def mk_fn_dict2(fn_dict):
    def fn_wrapper(dt, fn):
        try:
            return fn(dt)
        except Exception:
            return None

    return {
        'parser': functools.partial(fn_wrapper, fn=fn_dict['parser']),
        'formatter': functools.partial(fn_wrapper, fn=fn_dict['formatter']),
    }


# def mk_fn_dict(c_format_str):
#     return {
#         'parser': functools.partial(date_parser, format_str=c_format_str),
#         'formatter': functools.partial(date_formatter, format_str=c_format_str),
#     }
#


def iso8601_to_c_format(iso_str):
    """Convert an ISO8601-style datetime format spec, as used in EML, to a C-style
    format spec, as used by the Python datetime parsing and formatting functions.

    Args:
        iso_str (str):
            ISO8601-style datetime format spec from EML

    Returns:
        str: C-style format spec, as used by the Python datetime formatting functions
        None: Returned if 'iso_str' contains one or more sequences that we are unable to
        translate.
    """
    c_list = []
    # We look for keys in longest to shortest order, with key string itself as tie breaker.
    key_list = list(sorted(ISO8601_TO_CTIME_DICT.keys(), key=lambda s: (-len(s), s)))
    rx_str = '|'.join(key_list)
    rx_str = f'({rx_str})'
    for iso_str in re.split(rx_str, iso_str, flags=re.IGNORECASE):
        if iso_str:
            try:
                c_str = ISO8601_TO_CTIME_DICT[iso_str]
            except KeyError:
                if iso_str in list(' _-/,:T{}()[]'):
                    c_str = iso_str
                else:
                    return
            c_list.append(c_str)
            # log.debug(f'Translating datetime format string. ISO8601="{iso_str}" C="{c_str}"')
    c_format_str = ''.join(c_list)
    log.debug(f'Translated datetime format string. ISO8601="{iso_str}" C="{c_format_str}"')
    return c_format_str


def has_full_date(iso_str):
    """Return True if ISO8601-style datetime format string has a full date component,
    False otherwise.
    """
    c_format_str = iso8601_to_c_format(iso_str)
    return '%Y' in c_format_str and '%m' in c_format_str and '%d' in c_format_str

def has_full_time(iso_str):
    """Return True if ISO8601-style datetime format string has a full time component,
    False otherwise.
    """
    c_format_str = iso8601_to_c_format(iso_str)
    return '%H' in c_format_str and '%M' in c_format_str and '%S' in c_format_str

def has_full_datetime(iso_str):
    """Return True if ISO8601-style datetime format string has a full date and time
    component, False otherwise.
    """
    return has_full_date(iso_str) and has_full_time(iso_str)


def datetime_has_tz(dt):
    """Return True if datetime object has a timezone, False otherwise."""
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


# Dict of special cases for date format strings that appear in LTER and EDI EML
# documents but cannot be translated with the generic procedure implemented in
# iso8601_to_c_format(). The numbers in comments are number of occurrences found in a
# set of LTER EML documents.
EML_DATE_FORMAT_TO_CTIME_DICT = {
    # 1296
    '#####EMPTY#####': '',
    # 662
    'integer': None,
    # 657
    'YYYY-MM-DD hh:mm:ss (MST)': None,
    # 422
    'numeric': None,
    # 254
    'kkmm': None,
    # 131
    'alphabetic': None,
    # 39
    'DD/MM/YYYY hh:mm:ss.sss': None,
    # 30
    'hh:mm[:ss AA]': None,
    # 22
    'julian': None,
    # 20
    'YYYY-%j': '%Y-%j',
    # 19
    'm/d/yyyy': '%m/%d/%Y',
    # 15
    'number': None,
    # 14
    'YYYY-MM-DDT-8hh:mm': None,
    # 14
    'hh:mm:ss.s': None,
    # 12
    'JD': None,
    # 12
    'HH:MM:SS A/P': None,
    # 12
    'HH:MM:SS AM/PM': None,
    # 12
    '00:00:00': None,
    # 11
    '%j': '%j',
    # 11
    'd': None,
    # 10
    'HH:MM:SS AP': None,
    # 10
    'hh:mm:ss – 07': None,
    # 9
    'YYYY-MM-DDThh:mm:ss+hhmm': None,
    # 9
    'nominalMinute': None,
    # 9
    'hh:mm:ss AM/PM': None,
    # 9
    '1': None,
    # 8
    'nominalDay': None,
    # 8
    'ddd.dd': None,
    # 7
    'ww': None,
    # 6
    'YYYY-MM-DD hh:mm:ss – 07': None,
    # 6
    'NULL': None,
    # 6
    'minute': None,
    # 6
    'julian day': None,
    # 6
    'hh:mm:ss A/P': None,
    # 6
    'HH:MM:SS: AM/PM': None,
    # 6
    'hh:mm A/P': None,
    # 6
    'HH24:Mi': None,
    # 5
    'YYYYM': '%Y%m',
    # 5
    'none': None,
    # 5
    'MM/DD/YY HH:MM (AM|PM)': None,
    # 5
    'HH24:Mi:SS': None,
    # 5
    'DDMonYYYY': None,
    # 4
    'MST': None,
    # 4
    'Mon': None,
    # 4
    'MM/DD/YYYY HH24:Mi:ss': None,
    # 4
    'MM/DD/YYYY HH24:Mi:s': None,
    # 4
    '[m]m/[d]d/yy': None,
    # 4
    'M.D.YY': None,
    # 4
    'M/DD/YYYY': None,
    # 4
    'HH:MM AA': None,
    # 4
    '##:##': None,
    # 3
    'µsec': None,
    # 3
    'YYYY-MN-DD': None,
    # 3
    'YYYY-MM-DD hh:mm:ss.ss': None,
    # 3
    'YYYY-MM-DD hh:mm:ss+hh:mm': None,
    # 3
    'YYYY.MM.DD': None,
    # 3
    'Y-M-D': None,
    # 3
    'Year': None,
    # 3
    'MonthDayYear': None,
    # 3
    'MM-DD-YYYYY hh:mm': None,
    # 3
    'mm/dd/YYYY h:M:S': None,
    # 3
    'MM/DD/YY 00:00': None,
    # 3
    'Julian': None,
    # 3
    'HH:MM am/pm': None,
    # 3
    'Degree': None,
    # 2
    'YYYY-MM-DD-TT:TT': None,
    # 2
    'yyyy-mm-dd hh:mm:ss-z': None,
    # 2
    'YYYY-MM-DD HH:mm:SS.SSS+Z': None,
    # 2
    'YYYY-MM-DD hh:mm:ss-oo': None,
    # 2
    'YYYY-MM-DD  hh:mm:ss': None,
    # 2
    'YYYY--MM-DD': None,
    # 2
    'yyyy.mm.dd': None,
    # 2
    'Y-M-D h:M:s': None,
    # 2
    'YMD': None,
    # 2
    'years': None,
    # 2
    'Y': None,
    # 2
    'Q greater 500 cfs': None,
    # 2
    'Q greater 100 cfs': None,
    # 2
    'Q greater 1000 cfs': None,
    # 2
    'Month, YYYY': None,
    # 2
    'month': None,
    # 2
    'MM\DD\YYYY': None,
    # 2
    'mm/dd/yy%H:%M:%S': None,
    # 2
    'mm/dd/yy H:M:S': None,
    # 2
    'MM/DD//YY': None,
    # 2
    '(M/D/YYYY)': None,
    # 2
    'M/DD/YY': None,
    # 2
    'Hm': None,
    # 2
    'hh:mm:ss a/p': None,
    # 2
    'd/m/yyyy': None,
    # 2
    'dmm/dd/yy hr:mm.': None,
    # 2
    'DD-Mounth-YY': None,
    # 2
    '0:00:00': None,
    # 1
    'YYYY-NMM-DD': None,
    # 1
    'YYYY-MM-DD hh:mm:ss.sss': None,
    # 1
    'YYYY-MM-DD  hh:mm:ss.ss': None,
    # 1
    'YYYY-MM-DD hh:mm:ss -hhmm': None,
    # 1
    'YYYY- MM-DD': None,
    # 1
    'YYYY-M-D h:M:s': None,
    # 1
    'YYYY0MM0DD': None,
    # 1
    'YYY:DDD:hh:mm:ss.sssss': None,
    # 1
    'year': None,
    # 1
    'mmm.yyyy': None,
    # 1
    'MM/DD/YYYY H:S': None,
    # 1
    'MM/DD/YYYY H:m': None,
    # 1
    'MM/DD/YYYY HH:MM UTC+13': None,
    # 1
    'MM/DD/YYYY HH:MI24': None,
    # 1
    'mm/dd/yy hh:nn': None,
    # 1
    'mm/dd/y': None,
    # 1
    'M/D/YY': None,
    # 1
    'M/d/yy': None,
    # 1
    '"hh:mm:ss: A/P"': None,
    # 1
    'hh:mm:ss AA': None,
    # 1
    'hhmm(MST)': None,
    # 1
    'HH:MM A/P': None,
    # 1
    'hh:mm A': None,
    # 1
    'HH:MI24': None,
    # 1
    'graMPerMetersquareDPerYear': None,
    # 1
    'D-M-Y': None,
    # 1
    'DegreeFareinheit': None,
    # 1
    'dd-M-yy': None,
    # 1
    'dd-mon-yyyy hr:mm.': None,
    # 1
    'dd-mon-yyyy hh24:mi.': None,
    # 1
    'DD-Mon-YYYY': None,
    # 1
    'dd--mon-yyyy': None,
    # 1
    'DD-Mon-YY': None,
    # 1
    'dd-mom-yyyy': None,
    # 1
    'DDD.HHHH': None,
    # 1
    'DDD.dddd': None,
}


# Dict of custom parsers and formatters for special cases for date format strings that
# appear in LTER and EDI EML documents but cannot be parsed with strptime.
EML_DATE_FORMAT_TO_CUSTOM_DATETIME_PARSER_DICT = {
    'YYYY-MM-DDThh:mm:ss-hh': {
        'parser': lambda s: datetime.datetime.strptime(s + '00', '%Y-%m-%dT%H:%M:%S%z'),
        'formatter': lambda d: d.strftime('%Y-%m-%dT%H:%M:%S%z'),
    },
    'YYYY-MM-DDThh:mm:ss+hh': {
        'parser': lambda s: datetime.datetime.strptime(s + '00', '%Y-%m-%dT%H:%M:%S%z'),
        'formatter': lambda d: d.strftime('%Y-%m-%dT%H:%M:%S%z'),
    },
}

ISO8601_TO_CTIME_DICT = {
    # This dict was created based on an analysis of the full LTER and EDI corpus of
    # CSV files.
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    # -- %a
    # Weekday as locale’s abbreviated name.
    # Sun, Mon, …, Sat (en_US);
    # So, Mo, …, Sa (de_DE)
    'w': '%a',
    'W': '%a',
    'DDD': '%a',
    'WWW': '%a',
    # -- %A
    # Weekday as locale’s full name.
    # Sunday, Monday, …, Saturday (en_US);
    # Sonntag, Montag, …, Samstag (de_DE)
    # -- %w
    # Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.
    # 0, 1, …, 6
    'WW': '%w',
    # -- %d
    # Day of the month as a zero-padded decimal number.
    # 01, 02, …, 31
    'DD': '%d',
    'dd': '%d',
    'D': '%d',
    # -- %b
    # Month as locale’s abbreviated name.
    # Jan, Feb, …, Dec (en_US);
    # Jan, Feb, …, Dez (de_DE)
    'Month': '%b',
    'mmm': '%b',
    'MMM': '%b',
    'MON': '%b',
    'mon': '%b',
    # -- %B
    # Month as locale’s full name.
    # January, February, …, December (en_US);
    # Januar, Februar, …, Dezember (de_DE)
    # -- %m
    # Month as a zero-padded decimal number.
    # 01, 02, …, 12
    'MM': '%m',
    # -- %y
    # Year without century as a zero-padded decimal number.
    # 00, 01, …, 99
    'YYY': '%y',
    'YY': '%y',
    'yy': '%y',
    # -- %Y
    # Year with century as a decimal number.
    # 0001, 0002, …, 2013, 2014, …, 9998, 9999
    'YYYY': '%Y',
    'yyyy': '%Y',
    # -- %H
    # Hour (24-hour clock) as a zero-padded decimal number.
    # 00, 01, …, 23
    'H': '%H',
    'HH': '%H',
    'HH24': '%H',
    'h': '%H',
    'hh': '%H',
    'hh24': '%H',
    # -- %I
    # Hour (12-hour clock) as a zero-padded decimal number.
    # 01, 02, …, 12
    # -- %p
    # Locale’s equivalent of either AM or PM.
    # AM, PM (en_US);
    # am, pm (de_DE)
    # -- %M
    # Minute as a zero-padded decimal number.
    # 00, 01, …, 59
    'M': '%M',
    'MI': '%M',
    'm': '%M',
    'mi': '%M',
    'mm': '%M',
    # -- %S
    # Second as a zero-padded decimal number.
    'SS': '%S',
    'ss': '%S',
    's': '%S',
    # -- %f
    # Microsecond as a decimal number, zero-padded to 6 digits.
    # 000000, 000001, …, 999999
    # -- %z
    # UTC offset in the form ±HHMM[SS[.ffffff]] (empty string if the object is naive).
    # (empty), +0000, -0400, +1030, +063415, -030712.345216
    '\+hhmm': '%z',
    # -- %Z
    # Time zone name (empty string if the object is naive).
    # (empty), UTC, GMT
    'UTC': '%Z',
    'GMT': '%Z',
    # -- %j
    # Day of the year as a zero-padded decimal number.
    # 001, 002, …, 366
    'ddd': '%j',
    'DDDD': '%j',
    # -- %U
    # Week number of the year (Sunday as the first day of the week) as a zero-padded
    # decimal number. All days in a new year preceding the first Sunday are considered
    # to be in week 0.
    # 00, 01, …, 53
    # -- %W
    # Week number of the year (Monday as the first day of the week) as a zero-padded
    # decimal number. All days in a new year preceding the first Monday are considered
    # to be in week 0.
    # 00, 01, …, 53
    # -- %c
    # Locale’s appropriate date and time representation.
    # Tue Aug 16 21:30:00 1988 (en_US);
    # Di 16 Aug 21:30:00 1988 (de_DE)
    # -- %x
    # Locale’s appropriate date representation.
    # 08/16/88 (None);
    # 08/16/1988 (en_US);
    # 16.08.1988 (de_DE)
    # -- %X
    # Locale’s appropriate time representation.
    # 21:30:00 (en_US);
    # 21:30:00 (de_DE)
    # -- %%
    # A literal '%' character.
    # -- %
    # -- %G
    # ISO 8601 year with century representing the year that contains the greater part of
    # the ISO week (%V).
    # 0001, 0002, …, 2013, 2014, …, 9998, 9999
    # -- %u
    # ISO 8601 weekday as a decimal number where 1 is Monday.
    # 1, 2, …, 7
    # -- %V
    # ISO 8601 week as a decimal number with Monday as the first day of the week. Week
    # 01 is the week containing Jan 4.
    # 01, 02, …, 53
    # Weekday as locale’s full name.
    # Sunday, Monday, …, Saturday (en_US);
    # Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.
    # 0, 1, …, 6
    # Literals to preserve
    'Z': 'Z',
}
