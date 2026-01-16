"""
Task signature validation utilities.
"""
import inspect
from typing import Callable, Any, Dict, Tuple, Optional
from jobqueue.utils.logger import log


class ValidationError(Exception):
    """Raised when task validation fails."""
    pass


class TaskSignatureValidator:
    """
    Validates task signatures and arguments.
    """
    
    def __init__(self):
        """Initialize validator."""
        pass
    
    def validate_task_signature(
        self,
        func: Callable,
        args: Tuple,
        kwargs: Dict
    ) -> bool:
        """
        Validate that args and kwargs match function signature.
        
        Args:
            func: Task function
            args: Positional arguments
            kwargs: Keyword arguments
            
        Returns:
            True if valid
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            sig = inspect.signature(func)
            
            # Try to bind arguments to signature
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            
            log.debug(
                f"Task signature validated for {func.__name__}",
                extra={"args": args, "kwargs": kwargs}
            )
            
            return True
            
        except TypeError as e:
            log.error(f"Task signature validation failed: {e}")
            raise ValidationError(f"Invalid arguments for task: {e}")
    
    def validate_partial_signature(
        self,
        func: Callable,
        args: Tuple,
        kwargs: Dict
    ) -> bool:
        """
        Validate that args and kwargs are compatible (allows partial binding).
        
        Args:
            func: Task function
            args: Positional arguments
            kwargs: Keyword arguments
            
        Returns:
            True if valid
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            sig = inspect.signature(func)
            
            # Try partial binding
            bound = sig.bind_partial(*args, **kwargs)
            
            log.debug(
                f"Task partial signature validated for {func.__name__}",
                extra={"args": args, "kwargs": kwargs}
            )
            
            return True
            
        except TypeError as e:
            log.error(f"Task partial signature validation failed: {e}")
            raise ValidationError(f"Invalid arguments for task: {e}")
    
    def get_signature_info(self, func: Callable) -> Dict[str, Any]:
        """
        Get information about function signature.
        
        Args:
            func: Task function
            
        Returns:
            Dictionary with signature information
        """
        sig = inspect.signature(func)
        
        params = {}
        for name, param in sig.parameters.items():
            params[name] = {
                "kind": str(param.kind),
                "default": param.default if param.default != inspect.Parameter.empty else None,
                "annotation": str(param.annotation) if param.annotation != inspect.Parameter.empty else None
            }
        
        return {
            "parameters": params,
            "return_annotation": str(sig.return_annotation) if sig.return_annotation != inspect.Signature.empty else None
        }
    
    def validate_args_types(
        self,
        func: Callable,
        args: Tuple,
        kwargs: Dict
    ) -> bool:
        """
        Validate argument types against type annotations (if present).
        
        Args:
            func: Task function
            args: Positional arguments
            kwargs: Keyword arguments
            
        Returns:
            True if valid (or no annotations)
            
        Raises:
            ValidationError: If type validation fails
        """
        sig = inspect.signature(func)
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        
        for name, value in bound.arguments.items():
            param = sig.parameters[name]
            
            # Skip if no annotation
            if param.annotation == inspect.Parameter.empty:
                continue
            
            # Get the expected type
            expected_type = param.annotation
            
            # Skip if annotation is a string (forward reference)
            if isinstance(expected_type, str):
                continue
            
            # Check type (simple validation, doesn't handle complex types)
            if not isinstance(value, expected_type):
                error_msg = (
                    f"Type mismatch for parameter '{name}': "
                    f"expected {expected_type.__name__}, got {type(value).__name__}"
                )
                log.error(error_msg)
                raise ValidationError(error_msg)
        
        return True
    
    def check_required_args(self, func: Callable, args: Tuple, kwargs: Dict) -> bool:
        """
        Check if all required arguments are provided.
        
        Args:
            func: Task function
            args: Positional arguments
            kwargs: Keyword arguments
            
        Returns:
            True if all required args provided
            
        Raises:
            ValidationError: If required args missing
        """
        sig = inspect.signature(func)
        
        # Get parameter names
        params = list(sig.parameters.keys())
        
        # Build a set of provided argument names
        provided = set()
        
        # Add positional args
        for i, arg in enumerate(args):
            if i < len(params):
                provided.add(params[i])
        
        # Add keyword args
        provided.update(kwargs.keys())
        
        # Check for missing required parameters
        missing = []
        for name, param in sig.parameters.items():
            if name not in provided and param.default == inspect.Parameter.empty:
                # Skip *args and **kwargs
                if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                    continue
                missing.append(name)
        
        if missing:
            error_msg = f"Missing required arguments: {', '.join(missing)}"
            log.error(error_msg)
            raise ValidationError(error_msg)
        
        return True


# Global validator instance
task_validator = TaskSignatureValidator()
