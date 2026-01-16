"""
Advanced serialization utilities for tasks.
Handles JSON, pickle, complex objects, and circular references.
"""
import json
import pickle
import base64
import inspect
from typing import Any, Dict, Tuple, Optional
from datetime import datetime, date, time
from decimal import Decimal
from enum import Enum
from jobqueue.utils.logger import log


class SerializationError(Exception):
    """Raised when serialization fails."""
    pass


class DeserializationError(Exception):
    """Raised when deserialization fails."""
    pass


class TaskSerializer:
    """
    Advanced task serializer with support for complex objects.
    """
    
    SERIALIZATION_VERSION = "1.0"
    
    def __init__(self, use_pickle_fallback: bool = True):
        """
        Initialize serializer.
        
        Args:
            use_pickle_fallback: Use pickle for non-JSON-serializable objects
        """
        self.use_pickle_fallback = use_pickle_fallback
    
    def serialize(self, data: Any, format: str = "json") -> str:
        """
        Serialize data to string.
        
        Args:
            data: Data to serialize
            format: Serialization format ('json' or 'pickle')
            
        Returns:
            Serialized string
            
        Raises:
            SerializationError: If serialization fails
        """
        try:
            if format == "json":
                return self._serialize_json(data)
            elif format == "pickle":
                return self._serialize_pickle(data)
            else:
                raise ValueError(f"Unsupported format: {format}")
        except Exception as e:
            log.error(f"Serialization error: {e}")
            raise SerializationError(f"Failed to serialize data: {e}")
    
    def deserialize(self, data: str, format: str = "json") -> Any:
        """
        Deserialize data from string.
        
        Args:
            data: Serialized string
            format: Serialization format ('json' or 'pickle')
            
        Returns:
            Deserialized data
            
        Raises:
            DeserializationError: If deserialization fails
        """
        try:
            if format == "json":
                return self._deserialize_json(data)
            elif format == "pickle":
                return self._deserialize_pickle(data)
            else:
                raise ValueError(f"Unsupported format: {format}")
        except Exception as e:
            log.error(f"Deserialization error: {e}")
            raise DeserializationError(f"Failed to deserialize data: {e}")
    
    def _serialize_json(self, data: Any) -> str:
        """Serialize data to JSON with custom encoder."""
        return json.dumps(data, cls=EnhancedJSONEncoder, ensure_ascii=False)
    
    def _deserialize_json(self, data: str) -> Any:
        """Deserialize JSON data with custom decoder."""
        return json.loads(data, object_hook=enhanced_object_hook)
    
    def _serialize_pickle(self, data: Any) -> str:
        """Serialize data to pickle and encode as base64."""
        pickled = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        encoded = base64.b64encode(pickled).decode('ascii')
        return encoded
    
    def _deserialize_pickle(self, data: str) -> Any:
        """Deserialize pickle data from base64 string."""
        decoded = base64.b64decode(data.encode('ascii'))
        return pickle.loads(decoded)
    
    def serialize_args(self, args: Tuple) -> str:
        """
        Serialize task arguments.
        
        Args:
            args: Tuple of arguments
            
        Returns:
            Serialized string
        """
        try:
            return self._serialize_json(list(args))
        except SerializationError:
            if self.use_pickle_fallback:
                log.warning("JSON serialization failed for args, using pickle")
                return self._serialize_pickle(args)
            raise
    
    def serialize_kwargs(self, kwargs: Dict) -> str:
        """
        Serialize task keyword arguments.
        
        Args:
            kwargs: Dictionary of keyword arguments
            
        Returns:
            Serialized string
        """
        try:
            return self._serialize_json(kwargs)
        except SerializationError:
            if self.use_pickle_fallback:
                log.warning("JSON serialization failed for kwargs, using pickle")
                return self._serialize_pickle(kwargs)
            raise
    
    def deserialize_args(self, data: str) -> Tuple:
        """
        Deserialize task arguments.
        
        Args:
            data: Serialized string
            
        Returns:
            Tuple of arguments
        """
        try:
            result = self._deserialize_json(data)
            return tuple(result) if isinstance(result, list) else result
        except DeserializationError:
            if self.use_pickle_fallback:
                log.warning("JSON deserialization failed for args, trying pickle")
                return self._deserialize_pickle(data)
            raise
    
    def deserialize_kwargs(self, data: str) -> Dict:
        """
        Deserialize task keyword arguments.
        
        Args:
            data: Serialized string
            
        Returns:
            Dictionary of keyword arguments
        """
        try:
            return self._deserialize_json(data)
        except DeserializationError:
            if self.use_pickle_fallback:
                log.warning("JSON deserialization failed for kwargs, trying pickle")
                return self._deserialize_pickle(data)
            raise


class EnhancedJSONEncoder(json.JSONEncoder):
    """
    Enhanced JSON encoder that handles complex Python objects.
    """
    
    def default(self, obj):
        """
        Convert non-serializable objects to JSON-serializable format.
        
        Args:
            obj: Object to encode
            
        Returns:
            JSON-serializable representation
        """
        # Handle datetime objects
        if isinstance(obj, datetime):
            return {
                "__type__": "datetime",
                "value": obj.isoformat()
            }
        
        if isinstance(obj, date):
            return {
                "__type__": "date",
                "value": obj.isoformat()
            }
        
        if isinstance(obj, time):
            return {
                "__type__": "time",
                "value": obj.isoformat()
            }
        
        # Handle Decimal
        if isinstance(obj, Decimal):
            return {
                "__type__": "decimal",
                "value": str(obj)
            }
        
        # Handle Enum
        if isinstance(obj, Enum):
            return {
                "__type__": "enum",
                "class": f"{obj.__class__.__module__}.{obj.__class__.__name__}",
                "value": obj.value
            }
        
        # Handle bytes
        if isinstance(obj, bytes):
            return {
                "__type__": "bytes",
                "value": base64.b64encode(obj).decode('ascii')
            }
        
        # Handle sets
        if isinstance(obj, set):
            return {
                "__type__": "set",
                "value": list(obj)
            }
        
        # Handle complex numbers
        if isinstance(obj, complex):
            return {
                "__type__": "complex",
                "real": obj.real,
                "imag": obj.imag
            }
        
        # For other objects, try to pickle them as fallback
        try:
            pickled = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
            encoded = base64.b64encode(pickled).decode('ascii')
            return {
                "__type__": "pickled",
                "value": encoded
            }
        except Exception:
            # If all else fails, convert to string
            return {
                "__type__": "string_repr",
                "value": str(obj)
            }


def enhanced_object_hook(dct: Dict) -> Any:
    """
    Custom object hook for JSON deserialization.
    
    Args:
        dct: Dictionary from JSON
        
    Returns:
        Reconstructed Python object
    """
    if "__type__" not in dct:
        return dct
    
    obj_type = dct["__type__"]
    
    # Handle datetime objects
    if obj_type == "datetime":
        return datetime.fromisoformat(dct["value"])
    
    if obj_type == "date":
        return date.fromisoformat(dct["value"])
    
    if obj_type == "time":
        return time.fromisoformat(dct["value"])
    
    # Handle Decimal
    if obj_type == "decimal":
        return Decimal(dct["value"])
    
    # Handle Enum
    if obj_type == "enum":
        # Note: This requires the enum class to be importable
        # In production, you'd want a registry of enum classes
        return dct["value"]
    
    # Handle bytes
    if obj_type == "bytes":
        return base64.b64decode(dct["value"].encode('ascii'))
    
    # Handle sets
    if obj_type == "set":
        return set(dct["value"])
    
    # Handle complex numbers
    if obj_type == "complex":
        return complex(dct["real"], dct["imag"])
    
    # Handle pickled objects
    if obj_type == "pickled":
        decoded = base64.b64decode(dct["value"].encode('ascii'))
        return pickle.loads(decoded)
    
    # Handle string representations
    if obj_type == "string_repr":
        return dct["value"]
    
    return dct


def detect_circular_reference(obj: Any, max_depth: int = 100) -> bool:
    """
    Detect circular references in an object.
    
    Args:
        obj: Object to check
        max_depth: Maximum recursion depth
        
    Returns:
        True if circular reference detected
    """
    seen = set()
    
    def _check(item, depth=0):
        if depth > max_depth:
            return True
        
        item_id = id(item)
        if item_id in seen:
            return True
        
        seen.add(item_id)
        
        if isinstance(item, (list, tuple)):
            for element in item:
                if _check(element, depth + 1):
                    return True
        elif isinstance(item, dict):
            for value in item.values():
                if _check(value, depth + 1):
                    return True
        
        return False
    
    return _check(obj)


def is_serializable(obj: Any, format: str = "json") -> bool:
    """
    Check if an object is serializable.
    
    Args:
        obj: Object to check
        format: Serialization format
        
    Returns:
        True if serializable
    """
    serializer = TaskSerializer()
    
    try:
        serialized = serializer.serialize(obj, format=format)
        deserialized = serializer.deserialize(serialized, format=format)
        return True
    except Exception:
        return False


# Global serializer instance
task_serializer = TaskSerializer()
