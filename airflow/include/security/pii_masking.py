"""
PII Masking utilities for data security
"""
import hashlib
import os
from typing import Optional


class PIIMasker:
    """Masks Personally Identifiable Information"""
    
    def __init__(self):
        self.salt = os.getenv('MASKING_SALT', 'default_salt')
        self.enabled = os.getenv('ENABLE_PII_MASKING', 'false').lower() == 'true'
    
    def mask_email(self, email: Optional[str]) -> Optional[str]:
        """
        Masks email: john.doe@example.com → j***e@example.com
        """
        if not email or not self.enabled:
            return email
        
        try:
            local, domain = email.split('@')
            if len(local) <= 2:
                masked_local = local[0] + '***'
            else:
                masked_local = local[0] + '***' + local[-1]
            return f"{masked_local}@{domain}"
        except:
            return email
    
    def hash_email(self, email: Optional[str]) -> Optional[str]:
        """
        Hashes email irreversibly for analytics
        john.doe@example.com → 5f4dcc3b5aa765d61d8327deb882cf99
        """
        if not email:
            return None
        
        return hashlib.sha256(f"{email}{self.salt}".encode()).hexdigest()[:16]
    
    def mask_phone(self, phone: Optional[str]) -> Optional[str]:
        """
        Masks phone: +1 (212) 555-1234 → +1 (***) ***-1234
        """
        if not phone or not self.enabled:
            return phone
        
        # Keep last 4 digits
        digits = ''.join(filter(str.isdigit, phone))
        if len(digits) >= 4:
            return f"***-***-{digits[-4:]}"
        return "***-***-****"
    
    def mask_name(self, name: Optional[str]) -> Optional[str]:
        """
        Masks name: John Doe → J*** D***
        """
        if not name or not self.enabled:
            return name
        
        parts = name.split()
        masked_parts = [p[0] + '***' if len(p) > 1 else p for p in parts]
        return ' '.join(masked_parts)
    
    def mask_address(self, address: Optional[str]) -> Optional[str]:
        """
        Masks address: 123 Main St → *** Main St
        """
        if not address or not self.enabled:
            return address
        
        parts = address.split()
        if parts:
            parts[0] = '***'  # Mask street number
        return ' '.join(parts)


# Singleton instance
_masker = PIIMasker()

# Helper functions for direct usage
def mask_email(email: Optional[str]) -> Optional[str]:
    """Mask email using singleton instance"""
    return _masker.mask_email(email)


def mask_phone(phone: Optional[str]) -> Optional[str]:
    """Mask phone using singleton instance"""
    return _masker.mask_phone(phone)


def mask_name(name: Optional[str]) -> Optional[str]:
    """Mask name using singleton instance"""
    return _masker.mask_name(name)


def hash_email(email: Optional[str]) -> Optional[str]:
    """Hash email using singleton instance"""
    return _masker.hash_email(email)


def is_masking_enabled() -> bool:
    """Check if masking is enabled"""
    return _masker.enabled