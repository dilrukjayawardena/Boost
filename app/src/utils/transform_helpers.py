import polars as pl
import math
import re
from datetime import datetime
from dateutil import parser

def safe_number(value):
    """
    Convert value to float if possible, else return 0.
    Works for strings, None, or invalid numbers.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def truncate_amount(value):
    """
    Truncate float number to .2f
    """
    return math.floor(value * 100) / 100

def handle_paid_amount(value):
    return truncate_amount(safe_number(value))

def str_to_bool(value):
    """
    Convert value to boolean.
    """
    clean = re.sub(r"[^A-Za-z ]", "", value)
    return True if "true" in str(clean).lower() else False

def convert_to_timestamp(value):
    """
    Convert value to timestamp.
    if cannot convert return current date time.
    cannot return null as polars infer the type itself.
    """
    try:
        if value == '20001-01-01':
            value='2001-01-01'
        cleaned_date_string = re.sub(r'(\d+)(st|nd|rd|th|TEST)', r'\1', value)
        
        format_string = "%Y-%m-%d %H:%M:%S"
        dt_object = parser.parse(cleaned_date_string)
        
        # Use datetime.strftime() to output the unique format
        standardized_string = dt_object.strftime(format_string)
        timestamp = dt_object.timestamp()
        dt =datetime.strptime(standardized_string, format_string)
        if dt ==datetime.strptime('1970-01-01 00:00:00', format_string) :
            return datetime.now()
        return dt

    except Exception as e:
        return datetime.now()
    
def convert_date(value):
    """
    Convert value to Date.
    """
    try:
        format_string = "%Y-%m-%d"
        return datetime.strptime(value, format_string)
    except Exception as e:
        # print(value,e)
        return datetime.now()

def set_current_date(value):
    pass

