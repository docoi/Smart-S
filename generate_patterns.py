def generate_email_patterns(first_name, last_name, domain):
    """Generate common email patterns for a person"""
    # Clean and normalize names
    f = first_name.lower().strip()
    l = last_name.lower().strip()
    
    # Remove any non-alphabetic characters
    import re
    f = re.sub(r'[^a-z]', '', f)
    l = re.sub(r'[^a-z]', '', l)
    
    if not f or not l:
        return []
    
    # Generate common email patterns
    patterns = [
        f"{f[0]}",
    f"{f[0]}",
    f"{l[0]}",
    f"{l[0]}",
    f"FirstName.LastName@domain",
    f"FirstName@domain",
    f"LastName@domain",
    f"FirstInitialLastName@domain",
    f"FirstNameLastInitial@domain",
    f"FirstInitial.LastName@domain",
    f"FirstName_LastName@domain",
    f"LastName.FirstName@domain",
    f"FirstNameMiddleInitialLastName@domain",
    f"FirstInitialMiddleInitialLastName@domain",
    f"FirstName.LastInitial@domain",
    f"FirstInitialLastInitial@domain",
    f"FirstNameLastName@domain",
    f"LastNameFirstInitial@domain",
    f"FirstName.MiddleInitialLastName@domain",
    f"F.LastName@domain",
    f"FirstNameL@domain",
    f"LastInitialFirstName@domain",
    f"FirstName-LastName@domain",
    f"FirstName.LastNameNumber@domain",
    f"{f[0]}.{l[0]}@domain",
    f"{f[0]}@domain",
    f"{l[0]}@domain",
    f"{f[0]}{l[0]}@domain",
    f"{f[0]}_{l[0]}@domain",
    f"{f[0]}.{l[0]}@domain",
    f"{f[0]}{l[0]}@domain",
    f"{f[0]}.{l[0]}@domain",
    f"{f[0]}{l[0]}@domain",
    f"{l[0]}.{f[0]}@domain",
    f"{l[0]}{f[0]}@domain",       
    ]
    
    # Filter out None values and duplicates
    unique_patterns = []
    seen = set()
    
    for pattern in patterns:
        if pattern and pattern not in seen:
            unique_patterns.append(pattern)
            seen.add(pattern)
    
    return unique_patterns