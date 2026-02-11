"""
Silver Layer Cleaner.

Applies cleaning rules from config to standardize data.
"""

from typing import Dict, Any, Optional
from datetime import datetime
from dateutil import parser as date_parser
import re

from loguru import logger


class SilverCleaner:
    """
    Applies cleaning rules to standardize data.
    
    Supports:
    - Date normalization (ISO format)
    - Boolean normalization
    - Case normalization
    - Phone normalization
    """
    
    def __init__(self, cleaning_rules: Dict[str, Any]):
        """
        Initialize cleaner with rules from config.
        
        Args:
            cleaning_rules: Cleaning rules from cleaning_rules.yaml
        """
        self.rules = cleaning_rules.get("cleaners", {})
        self.logger = logger.bind(component="SilverCleaner")
    
    def clean_value(
        self,
        value: Any,
        rule_name: str,
    ) -> tuple[Any, bool, Optional[str]]:
        """
        Clean a single value using the specified rule.
        
        Args:
            value: Value to clean
            rule_name: Name of cleaning rule to apply
            
        Returns:
            Tuple of (cleaned_value, was_changed, change_description)
        """
        if value is None or value == "":
            return value, False, None
        
        rule = self.rules.get(rule_name)
        if not rule:
            return value, False, None
        
        rule_type = rule.get("type")
        
        if rule_type == "date":
            return self._clean_date(value, rule)
        elif rule_type == "boolean":
            return self._clean_boolean(value, rule)
        elif rule_type == "case":
            return self._clean_case(value, rule)
        elif rule_type == "phone":
            return self._clean_phone(value, rule)
        elif rule_type == "string":
            return self._clean_string(value, rule)
        
        return value, False, None
    
    def _clean_date(
        self,
        value: Any,
        rule: Dict[str, Any],
    ) -> tuple[Any, bool, Optional[str]]:
        """Clean date to ISO format."""
        original = str(value).strip()
        output_format = rule.get("output_format", "%Y-%m-%d")
        
        try:
            # Handle numeric timestamp
            import re
            if isinstance(original, (int, float)) or (isinstance(original, str) and re.match(r"^\d+(\.\d+)?$", original)):
                try:
                    ts = float(original)
                    # Heuristic: if ts > 3000 (year), it's likely a timestamp
                    if ts > 3000:
                         # Assume execution time is seconds. If > 1e11, maybe milliseconds?
                         # 1e10 is year 2286. 1e12 is milliseconds.
                         if ts > 1e11: 
                             ts /= 1000
                         parsed = datetime.fromtimestamp(ts)
                         return parsed.strftime(output_format), True, f"timestamp '{original}' → ISO"
                except (ValueError, TypeError):
                    pass

            # Parse with dateutil
            # Removed global dayfirst=True to avoid misinterpreting US dates (MM/DD/YYYY)
            parsed = date_parser.parse(original, fuzzy=False)
            cleaned = parsed.strftime(output_format)
            
            if cleaned != original:
                return cleaned, True, f"'{original}' → '{cleaned}'"
            return cleaned, False, None
            
        except (ValueError, TypeError):
            return value, False, None
    
    def _clean_boolean(
        self,
        value: Any,
        rule: Dict[str, Any],
    ) -> tuple[Any, bool, Optional[str]]:
        """Normalize boolean values."""
        true_values = rule.get("true_values", [True, "true", "1", 1])
        false_values = rule.get("false_values", [False, "false", "0", 0])
        
        if value in true_values:
            if value is not True:
                return True, True, f"'{value}' → True"
            return True, False, None
        elif value in false_values:
            if value is not False:
                return False, True, f"'{value}' → False"
            return False, False, None
        
        return value, False, None
    
    def _clean_case(
        self,
        value: Any,
        rule: Dict[str, Any],
    ) -> tuple[Any, bool, Optional[str]]:
        """Normalize case."""
        original = str(value)
        case = rule.get("case", "lower")
        
        if case == "lower":
            cleaned = original.lower()
        elif case == "upper":
            cleaned = original.upper()
        elif case == "title":
            cleaned = original.title()
        else:
            cleaned = original
        
        if cleaned != original:
            return cleaned, True, f"'{original}' → '{cleaned}'"
        return cleaned, False, None
    
    def _clean_phone(
        self,
        value: Any,
        rule: Dict[str, Any],
    ) -> tuple[Any, bool, Optional[str]]:
        """Normalize phone numbers."""
        original = str(value)
        remove_chars = rule.get("remove_chars", "()- .+")
        
        cleaned = original
        for char in remove_chars:
            cleaned = cleaned.replace(char, "")
        
        if cleaned != original:
            return cleaned, True, f"'{original}' → '{cleaned}'"
        return cleaned, False, None
    
    def _clean_string(
        self,
        value: Any,
        rule: Dict[str, Any],
    ) -> tuple[Any, bool, Optional[str]]:
        """Clean string values."""
        original = str(value)
        cleaned = original
        
        operations = rule.get("operations", [])
        
        if "trim" in operations:
            cleaned = cleaned.strip()
        if "normalize_whitespace" in operations:
            cleaned = re.sub(r'\s+', ' ', cleaned)
        
        if cleaned != original:
            return cleaned, True, f"'{original}' → '{cleaned}'"
        return cleaned, False, None
