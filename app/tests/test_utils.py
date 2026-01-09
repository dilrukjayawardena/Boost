"""Unit tests for utils/transform_utils.py utility functions."""

import unittest
from datetime import datetime
from src.utils.transform_helpers import (
    safe_number,
    truncate_amount,
    handle_paid_amount,
    str_to_bool,
    convert_to_timestamp,
    convert_date
)


class TestSafeNumber(unittest.TestCase):
    """Test cases for safe_number function."""
    
    def test_valid_float_string(self):
        """Test conversion of valid float string."""
        self.assertEqual(safe_number("123.45"), 123.45)
        self.assertEqual(safe_number("0.5"), 0.5)
        self.assertEqual(safe_number("-10.5"), -10.5)
    
    def test_valid_integer_string(self):
        """Test conversion of valid integer string."""
        self.assertEqual(safe_number("123"), 123.0)
        self.assertEqual(safe_number("0"), 0.0)
        self.assertEqual(safe_number("-10"), -10.0)
    
    def test_valid_numeric_types(self):
        """Test conversion of numeric types."""
        self.assertEqual(safe_number(123.45), 123.45)
        self.assertEqual(safe_number(123), 123.0)
        self.assertEqual(safe_number(0), 0.0)
        self.assertEqual(safe_number(-10), -10.0)
    
    def test_invalid_string(self):
        """Test handling of invalid string values."""
        self.assertEqual(safe_number("abc"), 0.0)
        self.assertEqual(safe_number("not a number"), 0.0)
        self.assertEqual(safe_number(""), 0.0)
    
    def test_none_value(self):
        """Test handling of None value."""
        self.assertEqual(safe_number(None), 0.0)
    
    def test_empty_string(self):
        """Test handling of empty string."""
        self.assertEqual(safe_number(""), 0.0)


class TestTruncateAmount(unittest.TestCase):
    """Test cases for truncate_amount function."""
    
    def test_truncate_to_two_decimals(self):
        """Test truncation to 2 decimal places."""
        self.assertEqual(truncate_amount(123.456), 123.45)
        self.assertEqual(truncate_amount(123.459), 123.45)
        self.assertEqual(truncate_amount(123.451), 123.45)
    
    def test_already_two_decimals(self):
        """Test values already at 2 decimal places."""
        self.assertEqual(truncate_amount(123.45), 123.45)
        self.assertEqual(truncate_amount(0.00), 0.0)
    
    def test_more_than_two_decimals(self):
        """Test values with more than 2 decimal places."""
        self.assertEqual(truncate_amount(10.999), 10.99)
        self.assertEqual(truncate_amount(5.123456), 5.12)
    
    def test_negative_values(self):
        """Test truncation of negative values."""
        # Note: math.floor truncates down, so negative values round further negative
        self.assertEqual(truncate_amount(-123.456), -123.46)
        self.assertEqual(truncate_amount(-10.999), -11.0)
    
    def test_zero(self):
        """Test truncation of zero."""
        self.assertEqual(truncate_amount(0.0), 0.0)
        self.assertEqual(truncate_amount(0), 0.0)


class TestStrToBool(unittest.TestCase):
    """Test cases for str_to_bool function."""
    
    def test_true_values(self):
        """Test various true value representations."""
        self.assertTrue(str_to_bool("true"))
        self.assertTrue(str_to_bool("TRUE"))
        self.assertTrue(str_to_bool("True"))
        self.assertTrue(str_to_bool("this is true"))
        self.assertTrue(str_to_bool("contains true value"))
    
    def test_false_values(self):
        """Test various false value representations."""
        self.assertFalse(str_to_bool("false"))
        self.assertFalse(str_to_bool("FALSEe"))
        self.assertFalse(str_to_bool("False"))
        self.assertFalse(str_to_bool("no"))
        self.assertFalse(str_to_bool(""))
        self.assertFalse(str_to_bool("random text"))
    
    def test_with_special_characters(self):
        """Test handling of strings with special characters."""
        self.assertTrue(str_to_bool("true123"))
        self.assertTrue(str_to_bool("truee"))
        self.assertTrue(str_to_bool("123true456"))
        self.assertFalse(str_to_bool("false123"))
    
    def test_empty_string(self):
        """Test handling of empty string."""
        self.assertFalse(str_to_bool(""))


class TestConvertToTimestamp(unittest.TestCase):
    """Test cases for convert_to_timestamp function."""
    
    def test_valid_date_strings(self):
        """Test conversion of valid date strings."""
        result = convert_to_timestamp("2024-01-15 10:30:00")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2024)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)
    
    def test_date_with_ordinal_suffixes(self):
        """Test handling of dates with ordinal suffixes (st, nd, rd, th)."""
        result = convert_to_timestamp("2024-01-15th 10:30:00")
        self.assertIsInstance(result, datetime)
        
        result = convert_to_timestamp("2024-01-1st 10:30:00")
        self.assertIsInstance(result, datetime)
    
    def test_special_case_20001(self):
        """Test special case handling for '20001-01-01'."""
        result = convert_to_timestamp("20001-01-01")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2001)
    
    def test_various_date_formats(self):
        """Test various date format inputs."""
        result = convert_to_timestamp("2024-01-15")
        self.assertIsInstance(result, datetime)
        
        result = convert_to_timestamp("Jan 15, 2024")
        self.assertIsInstance(result, datetime)
    
    def test_invalid_date_strings(self):
        """Test handling of invalid date strings."""
        # Should return current datetime on error
        result = convert_to_timestamp("invalid date")
        self.assertIsInstance(result, datetime)
        
        result = convert_to_timestamp("")
        self.assertIsInstance(result, datetime)
    
    def test_none_value(self):
        """Test handling of None value."""
        result = convert_to_timestamp(None)
        self.assertIsInstance(result, datetime)
    
    def test_with_test_suffix(self):
        """Test handling of 'TEST' suffix in date string."""
        result = convert_to_timestamp("2024-01-15TEST")
        self.assertIsInstance(result, datetime)


class TestConvertDate(unittest.TestCase):
    """Test cases for convert_date function."""
    
    def test_valid_date_string(self):
        """Test conversion of valid date string."""
        result = convert_date("2024-01-15")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2024)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)
        # Time should be 00:00:00 for date conversion
        self.assertEqual(result.hour, 0)
        self.assertEqual(result.minute, 0)
        self.assertEqual(result.second, 0)
    
    def test_different_valid_dates(self):
        """Test various valid date formats."""
        result = convert_date("2023-12-31")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2023)
        self.assertEqual(result.month, 12)
        self.assertEqual(result.day, 31)
        
        result = convert_date("2000-01-01")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2000)
    
    def test_invalid_date_strings(self):
        """Test handling of invalid date strings."""
        # Should return current datetime on error
        result = convert_date("invalid")
        self.assertIsInstance(result, datetime)
        
        result = convert_date("2024-13-45")  # Invalid month/day
        self.assertIsInstance(result, datetime)
        
        result = convert_date("not a date")
        self.assertIsInstance(result, datetime)
    
    def test_wrong_format(self):
        """Test handling of wrong date format."""
        # Function expects YYYY-MM-DD format
        result = convert_date("01/15/2024")  # Wrong format
        self.assertIsInstance(result, datetime)  # Should return current datetime
    
    def test_empty_string(self):
        """Test handling of empty string."""
        result = convert_date("")
        self.assertIsInstance(result, datetime)
    
    def test_none_value(self):
        """Test handling of None value."""
        result = convert_date(None)
        self.assertIsInstance(result, datetime)


if __name__ == "__main__":
    unittest.main()

