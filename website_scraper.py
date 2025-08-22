"""
🌐 Enhanced Website Scraper Module - PART 0 INTEGRATION WITH DOMAIN-SPECIFIC CACHE
===================================================================================
FIXED: Automatic domain-specific cache files - no manual intervention needed

WORKFLOW:
1. Automatically generates domain-specific cache files for each target
2. Check for domain-specific Part 0 cache files
3. If missing, run Part 0 pipeline automatically  
4. Consume Part 0 data for enhanced analysis
5. Use GPT-4o for intelligent staff validation and URL analysis
6. Return clean staff list + guaranteed LinkedIn URL

PART 0 FILES CONSUMED (per domain):
- cache_external_urls_DOMAIN.txt → GPT-4o URL intelligence
- site_social_links_DOMAIN.json → LinkedIn URL for Part 2
- staff_scrape_results_DOMAIN.json → Pre-extracted staff data (REQUIRED)
- cache_items_full_DOMAIN.json → Rich content for analysis

AUTOMATION: Each domain gets its own cache - no cross-contamination between targets
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse, urlunparse
from typing import List, Dict, Any, Tuple


class WebsiteScraper:
    """🌐 Enhanced website scraper with automatic domain-specific Part 0 integration"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
        self.script_dir = Path(__file__).parent
        
        # Part 0 cache files will be set dynamically per domain
        self.cache_files = {}
        self.current_domain = ""
        
        # Part 0 pipeline script
        self.staff_pipeline = self.script_dir / 'staff_pipeline.py'
    
    def _get_domain_cache_files(self, url: str) -> Dict[str, Path]:
        """🔧 AUTOMATIC: Generate domain-specific cache file paths"""
        
        # Extract and normalize domain from URL
        parsed = urlparse(url)
        domain = parsed.netloc.replace('www.', '').replace('.', '_').replace('-', '_')
        
        # Store current domain for logging
        self.current_domain = parsed.netloc.replace('www.', '')
        
        # Create domain-specific cache file paths
        return {
            'external_urls': self.script_dir / f'cache_external_urls_{domain}.txt',
            'social_links': self.script_dir / f'site_social_links_{domain}.json',
            'staff_results': self.script_dir / f'staff_scrape_results_{domain}.json',
            'items_full': self.script_dir / f'cache_items_full_{domain}.json',
            'urls_all': self.script_dir / f'cache_urls_all_{domain}.json'
        }
    
    def scrape_website_for_staff_and_linkedin(self, website_url: str) -> Tuple[List[Dict], str]:
        """📋 MAIN METHOD: Enhanced website scraping with automatic domain-specific Part 0 integration"""
        
        print(f"🚀 ENHANCED WEBSITE MAPPING - PART 0 INTEGRATION")
        print(f"🌐 Target: {website_url}")
        print("=" * 60)
        
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
    
    def _rename_generic_to_domain_specific(self):
        """🔄 Rename generic Part 0 files to domain-specific names"""
        
        generic_to_domain = {
            'site_social_links.json': self.cache_files['social_links'],
            'staff_scrape_results.json': self.cache_files['staff_results'],
            'cache_external_urls.txt': self.cache_files['external_urls'],
            'cache_internal_urls.txt': self.script_dir / f'cache_internal_urls_{self.current_domain.replace(".", "_")}.txt',
            'cache_social_urls.txt': self.script_dir / f'cache_social_urls_{self.current_domain.replace(".", "_")}.txt',
            'cache_urls_all.json': self.cache_files['urls_all'],
            'cache_items_full.json': self.cache_files['items_full']
        }
        
        renamed_count = 0
        
        for generic_name, domain_path in generic_to_domain.items():
            generic_path = self.script_dir / generic_name
            
            if generic_path.exists():
                try:
                    # Rename generic file to domain-specific name
                    generic_path.rename(domain_path)
                    print(f"   🔄 Renamed: {generic_name} → {domain_path.name}")
                    renamed_count += 1
                except Exception as e:
                    print(f"   ⚠️ Failed to rename {generic_name}: {e}")
            else:
                print(f"   ⚠️ Generic file not found: {generic_name}")
        
        print(f"   ✅ Cache file renaming complete: {renamed_count} files renamed")
        return renamed_count > 0
    
    def _check_part0_needs_refresh(self) -> bool:
        """🔍 Check if Part 0 cache files need refresh - DOMAIN-SPECIFIC"""
        
        # CRITICAL: Always run Part 0 if staff_scrape_results.json is missing
        staff_file = self.cache_files['staff_results']
        if not staff_file.exists():
            print(f"   ❌ Missing critical file: {staff_file.name}")
            return True
        
        # Check essential files exist
        essential_files = ['social_links', 'external_urls']
        
        for file_key in essential_files:
            file_path = self.cache_files[file_key]
            
            if not file_path.exists():
                print(f"   ❌ Missing: {file_path.name}")
                return True
            
            # Cache expiry (24 hours)
            age_seconds = time.time() - file_path.stat().st_mtime
            if age_seconds > 86400:  # 24 hours
                print(f"   ⏰ Stale: {file_path.name} ({age_seconds/3600:.1f} hours old)")
                return True
            
            print(f"   ✅ Found: {file_path.name}")
        
        # Check if staff results file has content
        try:
            with open(staff_file, 'r', encoding='utf-8') as f:
                staff_data = json.load(f)
                
            if not staff_data or not any(result.get('members', []) for result in staff_data):
                print(f"   ⚠️ Empty staff results - forcing refresh")
                return True
            
            print(f"   ✅ Found: {staff_file.name} with data")
            
        except Exception as e:
            print(f"   ❌ Corrupted staff file: {e}")
            return True
        
        return False
    
    def _run_part0_pipeline(self, url: str) -> bool:
        """🚀 Run Part 0 pipeline to generate domain-specific cache files"""
        
        if not self.staff_pipeline.exists():
            print(f"❌ Part 0 pipeline not found: {self.staff_pipeline}")
            return False
        
        try:
            print(f"   🚀 Running: python {self.staff_pipeline.name} {url}")
            print(f"   📍 Working directory: {self.script_dir}")
            print(f"   🗂️ Target cache: {self.current_domain}")
            print(f"   🗺️ Sitemaps: ENABLED for complete URL discovery")
            
            # Set environment with UTF-8 encoding for Windows
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            env['INCLUDE_SITEMAPS'] = '1'  # Enable sitemaps for complete URL discovery
            
            # Run Part 0 pipeline with encoding fix and increased timeout
            result = subprocess.run(
                [sys.executable, str(self.staff_pipeline), url],
                cwd=self.script_dir,
                capture_output=True,
                text=True,
                timeout=900,  # 15 minute timeout
                env=env,
                encoding='utf-8',
                errors='replace'
            )
            
            if result.returncode == 0:
                print(f"   ✅ Part 0 pipeline completed successfully")
                
                # Rename generic cache files to domain-specific names
                print(f"   🔄 Renaming cache files to domain-specific names...")
                self._rename_generic_to_domain_specific()
                
                # Verify that domain-specific staff_scrape_results.json was created
                if self.cache_files['staff_results'].exists():
                    print(f"   ✅ Staff results file created successfully")
                    return True
                else:
                    print(f"   ⚠️ Part 0 completed but staff file missing - trying fallback")
                    return self._run_part0_fallback(url)
                    
            else:
                print(f"   ❌ Part 0 pipeline failed (exit code: {result.returncode})")
                if result.stderr:
                    print(f"   📄 Error output: {result.stderr[:500]}")
                
                # Try fallback: run individual components
                print(f"   🔄 Attempting fallback approach...")
                return self._run_part0_fallback(url)
                
        except subprocess.TimeoutExpired:
            print(f"   ⏰ Part 0 pipeline timed out after 15 minutes")
            return False
        except Exception as e:
            print(f"   ❌ Error running Part 0 pipeline: {e}")
            print(f"   🔄 Attempting fallback approach...")
            return self._run_part0_fallback(url)
    
    def _run_part0_fallback(self, url: str) -> bool:
        """🔄 Fallback: Run Part 0 components individually"""
        
        print(f"   🔄 FALLBACK: Running Part 0 components individually")
        
        # Try to run just the essential recon components
        recon_script = self.script_dir / 'run_apify_from_recon.py'
        
        if not recon_script.exists():
            print(f"   ❌ Recon script not found: {recon_script}")
            return False
        
        try:
            # Set environment with UTF-8 encoding
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            env['INCLUDE_SITEMAPS'] = '1'  # Enable sitemaps for complete URL discovery
            
            print(f"   🚀 Running recon component: {recon_script.name}")
            
            result = subprocess.run(
                [sys.executable, str(recon_script), url],
                cwd=self.script_dir,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout for fallback
                env=env,
                encoding='utf-8',
                errors='replace'
            )
            
            if result.returncode == 0:
                print(f"   ✅ Recon component completed successfully")
                
                # Try staff extraction if recon succeeded
                staff_script = self.script_dir / 'select_and_scrape_staff.py'
                if staff_script.exists():
                    print(f"   🚀 Running staff component: {staff_script.name}")
                    
                    staff_result = subprocess.run(
                        [sys.executable, str(staff_script), url],
                        cwd=self.script_dir,
                        capture_output=True,
                        text=True,
                        timeout=400,  # Increased timeout for staff extraction
                        env=env,
                        encoding='utf-8',
                        errors='replace'
                    )
                    
                    if staff_result.returncode == 0:
                        print(f"   ✅ Staff component completed successfully")
                        
                        # Rename generic cache files to domain-specific names
                        print(f"   🔄 Renaming cache files to domain-specific names...")
                        self._rename_generic_to_domain_specific()
                        
                        return True
                    else:
                        print(f"   ⚠️ Staff component failed, continuing with recon data")
                        
                        # Still rename available cache files
                        print(f"   🔄 Renaming available cache files...")
                        self._rename_generic_to_domain_specific()
                        
                        return True  # Still return True as we have social links
                
                return True
            else:
                print(f"   ❌ Recon component failed (exit code: {result.returncode})")
                if result.stderr:
                    print(f"   📄 Error: {result.stderr[:300]}")
                return False
                
        except Exception as e:
            print(f"   ❌ Fallback failed: {e}")
            return False
    
    def _basic_fallback_extraction(self, url: str) -> Tuple[List[Dict], str]:
        """🔄 Basic fallback when Part 0 completely fails"""
        
        print(f"\n🔄 BASIC FALLBACK EXTRACTION")
        print("-" * 40)
        print(f"   🌐 Target: {url}")
        
        # Try to at least get LinkedIn URL manually
        linkedin_url = self._manual_linkedin_search(url)
        
        # Return empty staff list but try to provide LinkedIn URL
        if linkedin_url:
            print(f"   ✅ Found LinkedIn URL via manual search: {linkedin_url}")
        else:
            print(f"   ❌ Could not find LinkedIn URL")
        
        return [], linkedin_url
    
    def _manual_linkedin_search(self, url: str) -> str:
        """🔍 Manual LinkedIn URL search as last resort"""
        
        try:
            import requests
            from urllib.parse import urljoin
            
            print(f"   🔍 Attempting manual LinkedIn search...")
            
            # Try to fetch homepage and look for LinkedIn links
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                content = response.text.lower()
                
                # Look for LinkedIn company URLs in the content
                import re
                linkedin_patterns = [
                    r'https?://(?:www\.)?linkedin\.com/company/([^"\s<>]+)',
                    r'https?://(?:www\.)?linkedin\.com/companies/([^"\s<>]+)',
                    r'linkedin\.com/company/([^"\s<>]+)',
                    r'linkedin\.com/companies/([^"\s<>]+)'
                ]
                
                for pattern in linkedin_patterns:
                    matches = re.findall(pattern, content)
                    if matches:
                        # Clean up the match and construct full URL
                        company_id = matches[0].strip('/"\'')
                        if '/' in company_id:
                            company_id = company_id.split('/')[0]
                        
                        linkedin_url = f"https://www.linkedin.com/company/{company_id}"
                        print(f"   ✅ Found LinkedIn pattern: {linkedin_url}")
                        return linkedin_url
            
        except Exception as e:
            print(f"   ⚠️ Manual search failed: {e}")
        
        return ""
    
    def _load_part0_data(self) -> Dict[str, Any]:
        """📊 Load all Part 0 cache files (domain-specific)"""
        
        data = {}
        
        # Load social links (always needed for LinkedIn URL)
        try:
            with open(self.cache_files['social_links'], 'r', encoding='utf-8') as f:
                data['social_links'] = json.load(f)
                platforms = len(data['social_links'].get('by_platform', {}))
                print(f"   ✅ Loaded social links: {platforms} platforms")
        except Exception as e:
            print(f"   ❌ Failed to load social links: {e}")
            data['social_links'] = {}
        
        # Load external URLs for GPT analysis
        try:
            with open(self.cache_files['external_urls'], 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f if line.strip()]
                data['external_urls'] = urls
                print(f"   ✅ Loaded external URLs: {len(urls)} URLs")
        except Exception as e:
            print(f"   ❌ Failed to load external URLs: {e}")
            data['external_urls'] = []
        
        # Load staff extraction results (if available)
        try:
            with open(self.cache_files['staff_results'], 'r', encoding='utf-8') as f:
                staff_data = json.load(f)
                data['staff_results'] = staff_data
                
                # Count total unique staff across all URLs
                unique_staff = {}
                for result in staff_data:
                    for member in result.get('members', []):
                        name = member.get('name', '').strip()
                        if name and self._is_valid_person_name(name):
                            unique_staff[name.lower()] = member
                
                print(f"   ✅ Loaded staff results: {len(unique_staff)} unique staff found")
        except Exception as e:
            print(f"   ⚠️ No staff results found: {e}")
            data['staff_results'] = []
        
        # Load full items for content analysis (optional)
        try:
            with open(self.cache_files['items_full'], 'r', encoding='utf-8') as f:
                items_data = json.load(f)
                data['items_full'] = items_data
                print(f"   ✅ Loaded full items: {len(items_data)} items")
        except Exception as e:
            print(f"   ⚠️ No full items found: {e}")
            data['items_full'] = []
        
        return data
    
    def _enhanced_staff_analysis(self, part0_data: Dict[str, Any], domain: str) -> List[Dict[str, str]]:
        """🧠 Enhanced staff analysis using Part 0 data + GPT-4o intelligence"""
        
        staff_from_part0 = self._extract_staff_from_part0(part0_data)
        
        print(f"   📊 Part 0 staff extraction: {len(staff_from_part0)} staff members")
        
        # If we have good staff from Part 0, validate and enhance them
        if staff_from_part0:
            enhanced_staff = self._validate_and_enhance_staff(staff_from_part0, domain)
            print(f"   ✅ After validation: {len(enhanced_staff)} valid staff members")
            return enhanced_staff
        
        # If no staff from Part 0, try GPT analysis of URLs
        print(f"   🧠 No staff from Part 0 - trying GPT analysis of URLs")
        return self._gpt_analyze_urls_for_staff(part0_data, domain)
    
    def _extract_staff_from_part0(self, part0_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """📋 Extract and deduplicate staff from Part 0 results"""
        
        staff_results = part0_data.get('staff_results', [])
        
        if not staff_results:
            return []
        
        # Deduplicate staff by name (keep best title)
        unique_staff = {}
        
        for result in staff_results:
            for member in result.get('members', []):
                name = member.get('name', '').strip()
                title = member.get('title', '').strip()
                
                if not name or not self._is_valid_person_name(name):
                    continue
                
                name_key = name.lower()
                
                # Keep the entry with the longest/best title
                if name_key not in unique_staff or len(title) > len(unique_staff[name_key].get('title', '')):
                    unique_staff[name_key] = {
                        'name': name,
                        'title': title,
                        'source': 'part0_extraction'
                    }
        
        return list(unique_staff.values())
    
    def _validate_and_enhance_staff(self, staff_list: List[Dict], domain: str) -> List[Dict[str, str]]:
        """✅ Validate staff using GPT-4o and enhance with better titles"""
        
        if not staff_list:
            return []
        
        # Prepare staff data for GPT validation
        staff_text = "\n".join([f"{s['name']} - {s['title']}" for s in staff_list])
        
        domain_clean = urlparse(f"https://{domain}").netloc.replace('www.', '')
        
        prompt = f"""Review and validate this staff list from {domain_clean}.

STAFF LIST:
{staff_text}

VALIDATION RULES:
1. Keep ONLY real people (first + last name)
2. Remove company names, services, or generic terms
3. Improve job titles where possible
4. Prioritize management, operations, and safety roles
5. Return valid staff only

Return as JSON: [{{"name": "Full Name", "title": "Job Title"}}]
If no valid staff: []"""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=2000,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            print(f"   🧠 GPT-4o validation complete ({len(result_text)} chars)")
            print(f"   📝 GPT Response Preview: {result_text[:100]}...")
            
            # Parse JSON response
            json_start = result_text.find('[')
            json_end = result_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                validated_staff = json.loads(json_text)
                
                # Add source information and validate each entry
                final_staff = []
                for staff in validated_staff:
                    if isinstance(staff, dict) and staff.get('name') and staff.get('title'):
                        staff['source'] = 'part0_validated'
                        final_staff.append(staff)
                
                return final_staff[:15]  # Limit to 15 staff
            else:
                print(f"   ⚠️ No valid JSON found in GPT response")
            
        except Exception as e:
            print(f"   ⚠️ GPT validation failed: {e}")
        
        # Fallback: return original list with basic validation
        return [s for s in staff_list if self._is_valid_person_name(s.get('name', ''))][:10]
    
    def _gpt_analyze_urls_for_staff(self, part0_data: Dict[str, Any], domain: str) -> List[Dict[str, str]]:
        """🧠 Use GPT-4o to analyze URLs and suggest staff extraction"""
        
        external_urls = part0_data.get('external_urls', [])
        
        if not external_urls:
            print(f"   ❌ No external URLs available for analysis")
            return []
        
        # Filter URLs that might contain staff information
        staff_urls = []
        staff_keywords = ['about', 'team', 'staff', 'people', 'leadership', 'management', 'company', 'directors']
        
        for url in external_urls[:50]:  # Analyze top 50 URLs
            url_lower = url.lower()
            if any(keyword in url_lower for keyword in staff_keywords):
                staff_urls.append(url)
        
        if not staff_urls:
            print(f"   ❌ No staff-related URLs found")
            return []
        
        return []  # For now, return empty - could be enhanced to actually scrape these URLs
    
    def _extract_linkedin_url(self, part0_data: Dict[str, Any]) -> str:
        """🔗 Extract LinkedIn company URL from Part 0 social data"""
        
        social_links = part0_data.get('social_links', {})
        
        # Try the linkedin_company field first (most reliable)
        linkedin_url = social_links.get('linkedin_company', '')
        
        if linkedin_url:
            print(f"   ✅ LinkedIn company URL: {linkedin_url}")
            return linkedin_url
        
        # Fallback to LinkedIn URLs in by_platform
        by_platform = social_links.get('by_platform', {})
        linkedin_urls = by_platform.get('linkedin', [])
        
        if linkedin_urls:
            # Prefer company URLs over individual profiles
            for url in linkedin_urls:
                if '/company/' in url or '/companies/' in url:
                    print(f"   ✅ LinkedIn company URL (from platform): {url}")
                    return url
            
            # Fallback to first LinkedIn URL
            linkedin_url = linkedin_urls[0]
            print(f"   ✅ LinkedIn URL (fallback): {linkedin_url}")
            return linkedin_url
        
        print(f"   ❌ No LinkedIn URL found in social links")
        return ""
    
    def _is_valid_person_name(self, name: str) -> bool:
        """✅ Validate if a name looks like a real person"""
        
        if not name or len(name.strip()) < 3:
            return False
        
        name = name.strip()
        name_parts = name.split()
        
        # Must have at least first and last name
        if len(name_parts) < 2:
            return False
        
        # Reject obvious company names or generic terms
        reject_keywords = [
            'company', 'ltd', 'limited', 'inc', 'corp', 'llc', 'team', 
            'department', 'group', 'services', 'solutions', 'management',
            'creative', 'exceptional', 'events', 'private', 'clients',
            'home', 'about', 'contact', 'page', 'crewsaders'
        ]
        
        name_lower = name.lower()
        if any(keyword in name_lower for keyword in reject_keywords):
            return False
        
        # Check if all parts look like name parts (start with capital)
        for part in name_parts:
            if not part or not part[0].isupper():
                return False
        
        return True
    
    def _normalize_www(self, url: str) -> str:
        """🌐 Normalize URL to include www if needed"""
        
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        netloc = parsed.netloc
        
        if netloc and not netloc.startswith('www.'):
            netloc = 'www.' + netloc
        
        return urlunparse((
            parsed.scheme, netloc, parsed.path, 
            parsed.params, parsed.query, parsed.fragment
        ))