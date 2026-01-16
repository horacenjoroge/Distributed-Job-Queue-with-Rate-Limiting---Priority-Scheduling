"""
Cron expression parser for recurring task scheduling.
Supports standard cron syntax: minute hour day month weekday
"""
import re
from datetime import datetime, timedelta
from typing import Optional, List
from jobqueue.utils.logger import log


class CronParser:
    """
    Parse and evaluate cron expressions.
    
    Supports:
    - Standard cron fields: minute hour day month weekday
    - Wildcards: *
    - Lists: 1,2,3
    - Ranges: 1-5
    - Steps: */5, 1-10/2
    
    Examples:
        "*/5 * * * *" - Every 5 minutes
        "0 */2 * * *" - Every 2 hours
        "0 9 * * 1-5" - 9 AM on weekdays
        "30 8 1 * *" - 8:30 AM on 1st of every month
    """
    
    FIELD_NAMES = ["minute", "hour", "day", "month", "weekday"]
    FIELD_RANGES = {
        "minute": (0, 59),
        "hour": (0, 23),
        "day": (1, 31),
        "month": (1, 12),
        "weekday": (0, 6)  # 0 = Sunday
    }
    
    # Common cron shortcuts
    SHORTCUTS = {
        "@yearly": "0 0 1 1 *",
        "@annually": "0 0 1 1 *",
        "@monthly": "0 0 1 * *",
        "@weekly": "0 0 * * 0",
        "@daily": "0 0 * * *",
        "@midnight": "0 0 * * *",
        "@hourly": "0 * * * *"
    }
    
    def __init__(self, expression: str):
        """
        Initialize cron parser with expression.
        
        Args:
            expression: Cron expression string
        """
        self.original_expression = expression
        self.expression = self._expand_shortcuts(expression)
        self.fields = self._parse_expression()
        
        log.debug(f"Parsed cron expression: {expression}")
    
    def _expand_shortcuts(self, expression: str) -> str:
        """Expand shortcut expressions like @daily."""
        return self.SHORTCUTS.get(expression, expression)
    
    def _parse_expression(self) -> dict:
        """
        Parse cron expression into field values.
        
        Returns:
            Dictionary of field names to allowed values
        """
        parts = self.expression.split()
        
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression: {self.expression}. "
                "Expected 5 fields: minute hour day month weekday"
            )
        
        fields = {}
        for i, (field_name, field_value) in enumerate(zip(self.FIELD_NAMES, parts)):
            fields[field_name] = self._parse_field(
                field_value,
                field_name,
                self.FIELD_RANGES[field_name]
            )
        
        return fields
    
    def _parse_field(
        self,
        field_value: str,
        field_name: str,
        field_range: tuple
    ) -> List[int]:
        """
        Parse a single cron field.
        
        Args:
            field_value: Field value string
            field_name: Name of field
            field_range: (min, max) tuple
            
        Returns:
            List of allowed values
        """
        min_val, max_val = field_range
        
        # Wildcard - all values
        if field_value == "*":
            return list(range(min_val, max_val + 1))
        
        # List of values: 1,2,3
        if "," in field_value:
            values = []
            for part in field_value.split(","):
                values.extend(self._parse_field(part, field_name, field_range))
            return sorted(set(values))
        
        # Step values: */5 or 1-10/2
        if "/" in field_value:
            range_part, step = field_value.split("/")
            step = int(step)
            
            if range_part == "*":
                start, end = min_val, max_val
            elif "-" in range_part:
                start, end = map(int, range_part.split("-"))
            else:
                start = int(range_part)
                end = max_val
            
            return list(range(start, end + 1, step))
        
        # Range: 1-5
        if "-" in field_value:
            start, end = map(int, field_value.split("-"))
            return list(range(start, end + 1))
        
        # Single value
        return [int(field_value)]
    
    def matches(self, dt: datetime) -> bool:
        """
        Check if datetime matches cron expression.
        
        Args:
            dt: Datetime to check
            
        Returns:
            True if datetime matches
        """
        # Check each field
        if dt.minute not in self.fields["minute"]:
            return False
        if dt.hour not in self.fields["hour"]:
            return False
        if dt.day not in self.fields["day"]:
            return False
        if dt.month not in self.fields["month"]:
            return False
        
        # Weekday: 0 = Monday in Python, 0 = Sunday in cron
        # Convert: Python (0-6, Mon-Sun) -> Cron (0-6, Sun-Sat)
        cron_weekday = (dt.weekday() + 1) % 7
        if cron_weekday not in self.fields["weekday"]:
            return False
        
        return True
    
    def get_next_run(
        self,
        after: Optional[datetime] = None,
        max_iterations: int = 1000
    ) -> Optional[datetime]:
        """
        Calculate next execution time after given datetime.
        
        Args:
            after: Calculate next run after this time (default: now)
            max_iterations: Maximum iterations to prevent infinite loops
            
        Returns:
            Next execution datetime, or None if not found
        """
        if after is None:
            after = datetime.utcnow()
        
        # Start from next minute
        current = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        
        for _ in range(max_iterations):
            if self.matches(current):
                return current
            
            current += timedelta(minutes=1)
        
        log.warning(
            f"Could not find next run time for cron {self.expression} "
            f"after {max_iterations} iterations"
        )
        return None
    
    def get_previous_run(
        self,
        before: Optional[datetime] = None,
        max_iterations: int = 1000
    ) -> Optional[datetime]:
        """
        Calculate previous execution time before given datetime.
        
        Args:
            before: Calculate previous run before this time (default: now)
            max_iterations: Maximum iterations
            
        Returns:
            Previous execution datetime, or None if not found
        """
        if before is None:
            before = datetime.utcnow()
        
        current = before.replace(second=0, microsecond=0)
        
        for _ in range(max_iterations):
            if self.matches(current):
                return current
            
            current -= timedelta(minutes=1)
        
        return None


def parse_cron(expression: str) -> CronParser:
    """
    Parse a cron expression.
    
    Args:
        expression: Cron expression string
        
    Returns:
        CronParser instance
    """
    return CronParser(expression)


def validate_cron_expression(expression: str) -> tuple[bool, Optional[str]]:
    """
    Validate a cron expression.
    
    Args:
        expression: Cron expression to validate
        
    Returns:
        (is_valid, error_message) tuple
    """
    try:
        CronParser(expression)
        return True, None
    except Exception as e:
        return False, str(e)
