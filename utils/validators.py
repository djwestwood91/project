"""Security and validation utilities - no dependencies on other project modules."""


def validate_identifier(identifier_name, identifier_value):
    """
    Validate that an identifier contains only safe characters (alphanumeric and underscore).
    Prevents SQL injection attacks through environment variables.
    
    Args:
        identifier_name: Name of the identifier (for error messages)
        identifier_value: The identifier value to validate
        
    Returns:
        str: The validated identifier
        
    Raises:
        ValueError: If identifier contains unsafe characters
    """
    if not identifier_value or not all(c.isalnum() or c == '_' for c in identifier_value):
        raise ValueError(f"Invalid identifier '{identifier_name}': '{identifier_value}'. "
                        f"Identifiers must contain only alphanumeric characters and underscores.")
    return identifier_value


def validate_identifiers(*identifiers):
    """
    Validate multiple database identifiers (schema/table names) to prevent SQL injection attacks.
    
    Args:
        *identifiers: Variable number of identifier values to validate
        
    Raises:
        ValueError: If any identifier is invalid or empty
    """
    if not identifiers:
        raise ValueError("No identifiers provided for validation")
    for identifier in identifiers:
        if not identifier or not all(c.isalnum() or c == '_' for c in identifier):
            raise ValueError(f"Invalid identifier: {identifier}")
