"""
Timezone utilities for task scheduling.
"""
from datetime import datetime, timezone, timedelta
from typing import Optional
import pytz
from jobqueue.utils.logger import log


class TimezoneConverter:
    """
    Handle timezone conversions for scheduled tasks.
    """
    
    def __init__(self, default_timezone: str = "UTC"):
        """
        Initialize timezone converter.
        
        Args:
            default_timezone: Default timezone name (default: UTC)
        """
        self.default_timezone = pytz.timezone(default_timezone)
        
        log.debug(f"Initialized timezone converter with default: {default_timezone}")
    
    def to_utc(self, dt: datetime, from_timezone: Optional[str] = None) -> datetime:
        """
        Convert datetime to UTC.
        
        Args:
            dt: Datetime to convert
            from_timezone: Source timezone name (if dt is naive)
            
        Returns:
            UTC datetime (naive)
        """
        # If already aware, convert to UTC
        if dt.tzinfo is not None:
            utc_dt = dt.astimezone(timezone.utc)
            return utc_dt.replace(tzinfo=None)
        
        # If naive, assume it's in from_timezone or default
        if from_timezone:
            tz = pytz.timezone(from_timezone)
        else:
            tz = self.default_timezone
        
        # Localize and convert to UTC
        localized = tz.localize(dt)
        utc_dt = localized.astimezone(timezone.utc)
        
        return utc_dt.replace(tzinfo=None)
    
    def from_utc(self, dt: datetime, to_timezone: str) -> datetime:
        """
        Convert UTC datetime to target timezone.
        
        Args:
            dt: UTC datetime (naive or aware)
            to_timezone: Target timezone name
            
        Returns:
            Datetime in target timezone (aware)
        """
        # Ensure dt is UTC aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        # Convert to target timezone
        target_tz = pytz.timezone(to_timezone)
        return dt.astimezone(target_tz)
    
    def now_in_timezone(self, tz_name: str) -> datetime:
        """
        Get current time in specified timezone.
        
        Args:
            tz_name: Timezone name
            
        Returns:
            Current datetime in timezone (aware)
        """
        tz = pytz.timezone(tz_name)
        return datetime.now(tz)
    
    def parse_datetime_with_timezone(
        self,
        dt_str: str,
        tz_name: Optional[str] = None,
        fmt: str = "%Y-%m-%d %H:%M:%S"
    ) -> datetime:
        """
        Parse datetime string with timezone.
        
        Args:
            dt_str: Datetime string
            tz_name: Timezone name (default: UTC)
            fmt: Datetime format
            
        Returns:
            UTC datetime (naive)
        """
        dt = datetime.strptime(dt_str, fmt)
        return self.to_utc(dt, from_timezone=tz_name)
    
    def format_datetime_in_timezone(
        self,
        dt: datetime,
        tz_name: str,
        fmt: str = "%Y-%m-%d %H:%M:%S %Z"
    ) -> str:
        """
        Format datetime in specific timezone.
        
        Args:
            dt: UTC datetime
            tz_name: Target timezone
            fmt: Output format
            
        Returns:
            Formatted datetime string
        """
        dt_in_tz = self.from_utc(dt, tz_name)
        return dt_in_tz.strftime(fmt)
    
    @staticmethod
    def get_available_timezones() -> list:
        """
        Get list of available timezone names.
        
        Returns:
            List of timezone names
        """
        return sorted(pytz.all_timezones)
    
    @staticmethod
    def validate_timezone(tz_name: str) -> bool:
        """
        Validate timezone name.
        
        Args:
            tz_name: Timezone name to validate
            
        Returns:
            True if valid
        """
        try:
            pytz.timezone(tz_name)
            return True
        except pytz.exceptions.UnknownTimeZoneError:
            return False
    
    @staticmethod
    def get_timezone_offset(tz_name: str, dt: Optional[datetime] = None) -> timedelta:
        """
        Get timezone offset from UTC.
        
        Args:
            tz_name: Timezone name
            dt: Datetime to check (default: now)
            
        Returns:
            Offset from UTC
        """
        if dt is None:
            dt = datetime.utcnow()
        
        tz = pytz.timezone(tz_name)
        
        # Localize datetime
        if dt.tzinfo is None:
            dt = pytz.utc.localize(dt)
        
        # Get offset
        dt_in_tz = dt.astimezone(tz)
        return dt_in_tz.utcoffset()


# Global timezone converter instance
timezone_converter = TimezoneConverter()


def schedule_task_at_time(
    hour: int,
    minute: int,
    tz_name: str = "UTC",
    days_ahead: int = 0
) -> datetime:
    """
    Create a schedule time for a specific time in a timezone.
    
    Args:
        hour: Hour (0-23)
        minute: Minute (0-59)
        tz_name: Timezone name
        days_ahead: Days in the future (0 = today)
        
    Returns:
        UTC datetime for scheduling
        
    Example:
        # Schedule for 9:00 AM EST tomorrow
        eta = schedule_task_at_time(9, 0, "America/New_York", days_ahead=1)
    """
    # Get current time in target timezone
    tz = pytz.timezone(tz_name)
    now_in_tz = datetime.now(tz)
    
    # Create target time
    target = now_in_tz.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    # Add days if needed
    if days_ahead > 0:
        target += timedelta(days=days_ahead)
    
    # If time has passed today and days_ahead=0, schedule for tomorrow
    if days_ahead == 0 and target <= now_in_tz:
        target += timedelta(days=1)
    
    # Convert to UTC
    return timezone_converter.to_utc(target.replace(tzinfo=None), from_timezone=tz_name)


def schedule_task_in_timezone(
    dt: datetime,
    tz_name: str
) -> datetime:
    """
    Convert a datetime in a specific timezone to UTC for scheduling.
    
    Args:
        dt: Datetime in target timezone (naive)
        tz_name: Timezone name
        
    Returns:
        UTC datetime for scheduling
    """
    return timezone_converter.to_utc(dt, from_timezone=tz_name)
