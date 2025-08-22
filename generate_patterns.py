# =============================================================================
# 1. FIXED generate_patterns.py - Complete replacement
# =============================================================================

def generate_email_patterns(first_name, last_name, domain):
    """Generate 33+ common email patterns for a person"""
    # Clean and normalize names
    f = first_name.lower().strip()
    l = last_name.lower().strip()
    
    # Remove any non-alphabetic characters
    import re
    f = re.sub(r'[^a-z]', '', f)
    l = re.sub(r'[^a-z]', '', l)
    
    if not f or not l:
        return []
    
    # Generate 33+ comprehensive email patterns
    patterns = [
        # Standard patterns
        f"{f}.{l}@{domain}",           # john.smith@domain.com
        f"{f}@{domain}",               # john@domain.com  
        f"{l}@{domain}",               # smith@domain.com
        f"{f}{l}@{domain}",            # johnsmith@domain.com
        f"{f}_{l}@{domain}",           # john_smith@domain.com
        
        # Initial + last name patterns
        f"{f[0]}{l}@{domain}",         # jsmith@domain.com
        f"{f[0]}.{l}@{domain}",        # j.smith@domain.com
        f"{f}{l[0]}@{domain}",         # johns@domain.com
        
        # Reverse patterns
        f"{l}{f}@{domain}",            # smithjohn@domain.com
        f"{l}.{f}@{domain}",           # smith.john@domain.com
        f"{l}_{f}@{domain}",           # smith_john@domain.com
        
        # Initial combinations
        f"{f[0]}{l[0]}@{domain}",      # js@domain.com
        f"{f[0]}.{l[0]}@{domain}",     # j.s@domain.com
        f"{f[0]}_{l[0]}@{domain}",     # j_s@domain.com
        f"{l[0]}{f[0]}@{domain}",      # sj@domain.com
        f"{l[0]}.{f[0]}@{domain}",     # s.j@domain.com
        f"{l[0]}_{f[0]}@{domain}",     # s_j@domain.com
        
        # Hyphenated patterns
        f"{f}-{l}@{domain}",           # john-smith@domain.com
        f"{l}-{f}@{domain}",           # smith-john@domain.com
        
        # Number variations (common)
        f"{f}.{l}1@{domain}",          # john.smith1@domain.com
        f"{f}{l}1@{domain}",           # johnsmith1@domain.com
        f"{f}1@{domain}",              # john1@domain.com
        f"{f}01@{domain}",             # john01@domain.com
        
        # Additional initial patterns
        f"{f[0]}{l}1@{domain}",        # jsmith1@domain.com
        f"{f}{l[0]}1@{domain}",        # johns1@domain.com
        
        # Department/role variations
        f"{f}.{l}.work@{domain}",      # john.smith.work@domain.com
        f"{f}.{l}.office@{domain}",    # john.smith.office@domain.com
        
        # Extended patterns
        f"{f[0:2]}{l}@{domain}",       # josmith@domain.com (first 2 letters)
        f"{f}{l[0:2]}@{domain}",       # johnsm@domain.com (last 2 letters)
        f"{f[0:3]}.{l[0:3]}@{domain}", # joh.smi@domain.com
        
        # Alternative separators
        f"{f}+{l}@{domain}",           # john+smith@domain.com
        f"{l}+{f}@{domain}",           # smith+john@domain.com
        
        # Single letter variations
        f"{f[0]}@{domain}",            # j@domain.com
        f"{l[0]}@{domain}",            # s@domain.com
    ]
    
    # Filter out None values and duplicates while preserving order
    unique_patterns = []
    seen = set()
    
    for pattern in patterns:
        if pattern and pattern not in seen and len(pattern.split('@')[0]) > 0:
            unique_patterns.append(pattern)
            seen.add(pattern)
    
    return unique_patterns

# =============================================================================
# 2. Cache Clearing Method - Add to WebsiteScraper class
# =============================================================================

def clear_all_cache_files(self):
    """🗑️ Clear ALL cache files before starting new domain to prevent contamination"""
    print("🗑️ CLEARING ALL CACHE FILES...")
    
    # Comprehensive cache file patterns
    cache_patterns = [
        "cache_*.txt", 
        "cache_*.json",
        "site_social_links*.json", 
        "staff_scrape_results*.json",
        "cache_external_urls*.txt",
        "cache_internal_urls*.txt", 
        "cache_social_urls*.txt",
        "cache_urls_all*.json",
        "cache_items_full*.json"
    ]
    
    cleared_count = 0
    
    for pattern in cache_patterns:
        for file in self.script_dir.glob(pattern):
            try:
                file.unlink()
                print(f"   🗑️ Deleted: {file.name}")
                cleared_count += 1
            except Exception as e:
                print(f"   ⚠️ Failed to delete {file.name}: {e}")
    
    print(f"✅ Cache clearing complete: {cleared_count} files removed")
    return cleared_count > 0

# =============================================================================
# 3. Updated scrape_website_for_staff_and_linkedin method
# =============================================================================

def scrape_website_for_staff_and_linkedin(self, website_url: str) -> Tuple[List[Dict], str]:
    """📋 MAIN METHOD: Enhanced website scraping with automatic domain-specific Part 0 integration"""
    
    print(f"🚀 ENHANCED WEBSITE MAPPING - PART 0 INTEGRATION")
    print(f"🌐 Target: {website_url}")
    print("=" * 60)
    
    # 🗑️ CRITICAL: Clear all cache files first to prevent contamination
    self.clear_all_cache_files()
    
    # Normalize URL
    normalized_url = self._normalize_www(website_url)
    
    # 🔧 AUTOMATIC: Set domain-specific cache files
    self.cache_files = self._get_domain_cache_files(normalized_url)
    domain_name = self.current_domain
    
    print(f"🗂️ Using domain-specific cache: {domain_name}")
    print(f"   📁 Cache files: *_{domain_name.replace('.', '_')}.*")
    
    # PHASE 1: Ensure Part 0 data exists (domain-specific)
    print(f"\n🔍 PHASE 1: PART 0 RECON DATA CHECK ({domain_name})")
    print("-" * 40)
    
    part0_needs_refresh = self._check_part0_needs_refresh()
    
    if part0_needs_refresh:
        print(f"📦 Part 0 data missing or incomplete for {domain_name} - running Part 0 pipeline...")
        success = self._run_part0_pipeline(normalized_url)
        if not success:
            print("⚠️ Part 0 pipeline failed - using fallback extraction")
            return self._basic_fallback_extraction(normalized_url)
    else:
        print(f"✅ Part 0 cache files found and complete for {domain_name} - proceeding with analysis")
    
    # PHASE 2: Load Part 0 data (domain-specific)
    print(f"\n📊 PHASE 2: LOADING PART 0 DATA ({domain_name})")
    print("-" * 40)
    part0_data = self._load_part0_data()
    
    if not part0_data or not any(part0_data.values()):
        print("❌ Failed to load Part 0 data - using basic fallback extraction")
        return self._basic_fallback_extraction(normalized_url)
    
    # PHASE 3: Enhanced analysis using Part 0 data
    print(f"\n🧠 PHASE 3: ENHANCED ANALYSIS WITH GPT-4O ({domain_name})")
    print("-" * 40)
    enhanced_staff = self._enhanced_staff_analysis(part0_data, normalized_url)
    linkedin_url = self._extract_linkedin_url(part0_data)
    
    print(f"\n✅ ENHANCED WEBSITE SCRAPING COMPLETE:")
    print(f"   🌐 Domain: {domain_name}")
    print(f"   👥 Staff found: {len(enhanced_staff)}")
    print(f"   🔗 LinkedIn URL: {linkedin_url if linkedin_url else 'None found'}")
    
    return enhanced_staff, linkedin_url

# =============================================================================
# 4. Pattern Testing Logic Fix - Single Person Testing
# =============================================================================

def test_golden_patterns_single_priority(self, staff_list, domain):
    """🧪 Test patterns on ONLY the #1 highest priority person first"""
    
    if not staff_list:
        return None, []
    
    # Sort by priority and get the TOP person only
    sorted_staff = sorted(staff_list, key=lambda x: self.get_priority_score(x), reverse=True)
    top_person = sorted_staff[0]
    
    print(f"🧪 Testing patterns for TOP PRIORITY person: {top_person['name']}")
    print(f"   📊 Priority Score: {self.get_priority_score(top_person)}")
    
    # Generate patterns for this person only
    name_parts = top_person['name'].split()
    if len(name_parts) >= 2:
        first_name = name_parts[0]
        last_name = name_parts[-1]
        
        # Use the FIXED pattern generator
        patterns = generate_email_patterns(first_name, last_name, domain)
        
        print(f"   📧 Testing {len(patterns)} golden email patterns:")
        
        # Test each pattern
        for i, pattern in enumerate(patterns, 1):
            print(f"      🔍 Testing pattern {i}/{len(patterns)}: {pattern}")
            
            if self.verify_email(pattern):  # Your existing email verification
                print(f"      ✅ VALID PATTERN FOUND: {pattern}")
                return pattern, patterns
            else:
                print(f"      ❌ Pattern invalid: {pattern}")
    
    print(f"   😞 No valid patterns found for {top_person['name']}")
    return None, []

def get_priority_score(self, person):
    """📊 Calculate priority score for a person based on their title"""
    title = person.get('title', '').lower()
    
    # MD/Operations/Executive = 90
    if any(term in title for term in ['managing director', 'operations director', 'executive director']):
        return 90
    
    # Managers = 75  
    if any(term in title for term in ['manager', 'head of']):
        return 75
    
    # Associates/Analysts = 50
    if any(term in title for term in ['associate', 'analyst']):
        return 50
    
    # Coordinators/Officers = 50
    if any(term in title for term in ['coordinator', 'officer']):
        return 50
    
    # Executives/Administrators = 10
    if any(term in title for term in ['executive', 'administrator']):
        return 10
    
    # Default
    return 25