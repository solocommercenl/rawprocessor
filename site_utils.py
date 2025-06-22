"""
site_utils.py

Robust site detection utilities for rawprocessor.
Handles any domain format and provides consistent site key generation.
"""

import re
from typing import Optional
from loguru import logger

def extract_site_key(site_url: str) -> Optional[str]:
    """
    Extract a consistent site key from any domain format.
    
    Examples:
    - "solostaging.nl" -> "solostaging"
    - "mysite.com" -> "mysite" 
    - "sub.domain.co.uk" -> "sub_domain"
    - "example-site.de" -> "example_site"
    - "https://mysite.nl" -> "mysite"
    
    :param site_url: Domain or URL string
    :return: Clean site key or None if invalid
    """
    if not site_url or not isinstance(site_url, str):
        return None
    
    try:
        # Remove protocol if present
        clean_url = site_url.lower().strip()
        clean_url = re.sub(r'^https?://', '', clean_url)
        clean_url = re.sub(r'^www\.', '', clean_url)
        
        # Remove trailing slash and path
        clean_url = clean_url.split('/')[0]
        
        # Split by dots and remove TLD (last part)
        parts = clean_url.split('.')
        
        if len(parts) < 2:
            # No TLD found, use as-is
            domain_part = clean_url
        else:
            # Remove the TLD (last part)
            domain_part = '.'.join(parts[:-1])
        
        # Convert to valid identifier
        # Replace dots, hyphens, and other special chars with underscores
        site_key = re.sub(r'[^\w]', '_', domain_part)
        
        # Remove multiple consecutive underscores
        site_key = re.sub(r'_+', '_', site_key)
        
        # Remove leading/trailing underscores
        site_key = site_key.strip('_')
        
        # Validate result
        if not site_key or not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', site_key):
            logger.warning(f"Generated invalid site key '{site_key}' from '{site_url}'")
            return None
        
        return site_key
        
    except Exception as ex:
        logger.error(f"Error extracting site key from '{site_url}': {ex}")
        return None

def validate_site_key(site_key: str) -> bool:
    """
    Validate that a site key is properly formatted.
    
    :param site_key: Site key to validate
    :return: True if valid, False otherwise
    """
    if not site_key or not isinstance(site_key, str):
        return False
    
    # Must start with letter, contain only letters, numbers, underscores
    return bool(re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', site_key))

def normalize_site_url(site_url: str) -> Optional[str]:
    """
    Normalize a site URL to a standard format.
    
    :param site_url: Raw site URL
    :return: Normalized URL or None if invalid
    """
    if not site_url or not isinstance(site_url, str):
        return None
    
    try:
        # Remove protocol
        clean_url = site_url.lower().strip()
        clean_url = re.sub(r'^https?://', '', clean_url)
        clean_url = re.sub(r'^www\.', '', clean_url)
        
        # Remove trailing slash and path
        clean_url = clean_url.split('/')[0]
        
        # Basic domain validation
        if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9\-\.]*[a-zA-Z0-9]$', clean_url):
            return None
        
        return clean_url
        
    except Exception as ex:
        logger.error(f"Error normalizing site URL '{site_url}': {ex}")
        return None

# Test cases for validation
def test_site_key_extraction():
    """Test cases for the site key extraction logic."""
    test_cases = [
        ("solostaging.nl", "solostaging"),
        ("mysite.com", "mysite"),
        ("sub.domain.co.uk", "sub_domain"),
        ("example-site.de", "example_site"),
        ("https://mysite.nl", "mysite"),
        ("www.example.org", "example"),
        ("test_site.io", "test_site"),
        ("123invalid.com", None),  # Starts with number
        ("", None),  # Empty
        ("invalid", None),  # No TLD
    ]
    
    for input_url, expected in test_cases:
        result = extract_site_key(input_url)
        status = "✅" if result == expected else "❌"
        print(f"{status} '{input_url}' -> '{result}' (expected: '{expected}')")

if __name__ == "__main__":
    test_site_key_extraction()