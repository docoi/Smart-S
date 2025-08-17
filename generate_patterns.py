import re

def _clean(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("’", "'")
    s = re.sub(r"[^a-z'\- ]", "", s)
    s = s.replace("-", " ").replace("'", "")
    s = re.sub(r"\s+", " ", s)
    return s

def generate_email_patterns(first_name, last_name, domain):
    """Generate common email patterns for a person."""
    f = _clean(first_name).replace(" ", "")
    l = _clean(last_name).replace(" ", "")
    if not f or not l or not domain:
        return []

    initials = f[0] if f else ""
    li = l[0] if l else ""

    candidates = [
        f"{f}.{l}@{domain}",     # first.last
        f"{f}@{domain}",         # first
        f"{l}@{domain}",         # last
        f"{f}{l}@{domain}",      # firstlast
        f"{f}_{l}@{domain}",     # first_last
        f"{initials}{l}@{domain}",   # flast
        f"{initials}.{l}@{domain}",  # f.last
        f"{f}{li}@{domain}",         # firstl
        f"{l}{f}@{domain}",          # lastfirst
        f"{l}.{f}@{domain}",         # last.first
        f"{l}_{f}@{domain}",         # last_first
    ]

    # Deduplicate while preserving order
    seen, out = set(), []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)

    return out

if __name__ == "__main__":
    print(generate_email_patterns("Scott", "Dynamite", "dynamiteevents.co.uk"))
