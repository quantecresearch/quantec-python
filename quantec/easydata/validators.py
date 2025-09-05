"""Validation functions for Quantec EasyData API parameters."""

from typing import Union


def validate_dimension_filter(filter_dict: dict) -> None:
    """Validate a single dimension filter dictionary based on server constraints.
    
    Parameters
    ----------
    filter_dict : dict
        Dictionary containing dimension filter parameters.
        
    Raises
    ------
    ValueError
        If filter_dict is invalid or uses unsupported combinations.
    """
    if not isinstance(filter_dict, dict):
        raise ValueError("Dimension filter must be a dictionary")
    
    # Check required dimension field
    if "dimension" not in filter_dict:
        raise ValueError("Dimension filter must include 'dimension' field")
    
    dimension = filter_dict["dimension"]
    valid_dimensions = ["d1", "d2", "d3", "d4", "d5", "d6", "d7"]
    if dimension not in valid_dimensions:
        raise ValueError(f"Dimension must be one of {valid_dimensions}, got '{dimension}'")
    
    # Extract and validate field types
    codes = filter_dict.get("codes", [])
    levels = filter_dict.get("levels", [])
    children = filter_dict.get("children", False)
    children_include_self = filter_dict.get("children_include_self", False)
    
    # Validate field types
    if not isinstance(codes, list):
        raise ValueError("'codes' must be a list")
    if not isinstance(levels, list):
        raise ValueError("'levels' must be a list")
    if not isinstance(children, bool):
        raise ValueError("'children' must be a boolean")
    if not isinstance(children_include_self, bool):
        raise ValueError("'children_include_self' must be a boolean")
    
    if levels and not all(isinstance(level, int) for level in levels):
        raise ValueError("All items in 'levels' must be integers")
    
    # Validate combinations based on server logic
    # 1. codes only (codes=['FOO',...], no levels, no children flags)
    if (len(codes) > 0 and len(levels) == 0 and 
        not children and not children_include_self):
        return  # Valid
    
    # 2. levels only (levels=[0,...], no codes, no children flags)
    elif (len(codes) == 0 and len(levels) > 0 and 
          not children and not children_include_self):
        return  # Valid
    
    # 3. codes and levels (codes=['FOO',...], levels=[0,...], no children flags)
    elif (len(codes) > 0 and len(levels) > 0 and 
          not children and not children_include_self):
        return  # Valid
    
    # 4. single code with children (codes=['FOO'] only, children=True)
    elif (len(codes) == 1 and len(levels) == 0 and 
          children and not children_include_self):
        return  # Valid
    
    # 5. single code with children_include_self (codes=['FOO'] only, children_include_self=True)
    elif (len(codes) == 1 and len(levels) == 0 and 
          not children and children_include_self):
        return  # Valid
    
    else:
        # Invalid combination - check if no criteria provided
        if len(codes) == 0 and len(levels) == 0 and not children and not children_include_self:
            raise ValueError("At least one of 'codes', 'levels', 'children', or 'children_include_self' must be provided")
        else:
            raise ValueError(
                "Invalid filter combination. Supported patterns:\n"
                "1. codes only: {'codes': ['CODE1', ...]}\n"
                "2. levels only: {'levels': [1, 2, ...]}\n"
                "3. codes and levels: {'codes': ['CODE1'], 'levels': [1, 2]}\n"
                "4. single code with children: {'codes': ['CODE1'], 'children': True}\n"
                "5. single code with children_include_self: {'codes': ['CODE1'], 'children_include_self': True}"
            )


def validate_dimension_filters(selectdimensionnodes: Union[dict, list[dict]]) -> None:
    """Validate dimension filters (supports both single dict and list of dicts).
    
    Parameters
    ----------
    selectdimensionnodes : Union[dict, list[dict]]
        Single dimension filter dict or list of dimension filter dicts.
        
    Raises
    ------
    ValueError
        If selectdimensionnodes is invalid.
    """
    if isinstance(selectdimensionnodes, dict):
        validate_dimension_filter(selectdimensionnodes)
    elif isinstance(selectdimensionnodes, list):
        if not selectdimensionnodes:
            raise ValueError("selectdimensionnodes list cannot be empty")
        for filter_dict in selectdimensionnodes:
            validate_dimension_filter(filter_dict)
    else:
        raise ValueError("selectdimensionnodes must be a dict or list of dicts")