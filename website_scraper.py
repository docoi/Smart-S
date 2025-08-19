"""
🌐 Website Scraper Module - ENHANCED TEAM PAGE DETECTION
=========================================================
Enhanced GPT-4o prompting to find team pages including:
- URL fragments (#team, #about-us)
- Carousel/dynamic content
- Multiple team page formats
"""

import json
import re
from urllib.parse import urlparse, urlunparse
from account_manager import get_working_apify_client_part1


class WebsiteScraper:
    """🌐 Enhanced website scraping with smart team page detection"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
    
    def scrape_website_staff(self, url: str) -> tuple:
        """🔍 Main method: Scrape website for staff and LinkedIn URL"""
        
        try:
            print("📋 PART 1: WEBSITE SCRAPING")
            print("----------------------------------------")
            
            # Step 1: Website mapping
            print("🗺️ Phase 1a: Website mapping...")
            sitemap_data = self._scrape_website_sitemap(url)
            
            # Step 2: Enhanced URL analysis with team page detection
            print("🧠 Phase 1b: Enhanced GPT-4o URL analysis...")
            selected_urls = self._analyze_urls_with_gpt4o_enhanced(sitemap_data, url)
            
            # Step 3: Content analysis
            print("🔍 Phase 1c: Content analysis...")
            all_staff = []
            linkedin_url = None
            
            for i, page_url in enumerate(selected_urls, 1):
                print(f"  📄 Processing {i}/{len(selected_urls)}: {page_url}")
                staff_data, found_linkedin = self._extract_staff_from_page(page_url, url)
                
                if staff_data:
                    all_staff.extend(staff_data)
                    print(f"    ✅ Found {len(staff_data)} staff members")
                
                if found_linkedin and not linkedin_url:
                    linkedin_url = found_linkedin
            
            # Extract LinkedIn URL from sitemap if not found in content
            if not linkedin_url:
                linkedin_url = self._extract_linkedin_from_sitemap(sitemap_data, url)
            
            print("✅ Website scraping complete:")
            print(f"   👥 Staff found: {len(all_staff)}")
            print(f"   🔗 LinkedIn URL: {linkedin_url if linkedin_url else 'Not found'}")
            
            return all_staff, linkedin_url
            
        except Exception as e:
            print(f"❌ Website scraping failed: {e}")
            return [], None

    def _analyze_urls_with_gpt4o_enhanced(self, sitemap_data: dict, domain: str) -> list:
        """🧠 Enhanced GPT-4o analysis to find team pages including fragments and carousel content"""
        
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            # Extract URLs from sitemap
            urls = sitemap_data.get('urls', [])
            
            # Add potential team page URLs with fragments that might be missed
            domain_name = self._extract_domain_name(domain)
            potential_team_urls = [
                f"{domain}#team",
                f"{domain}#the-team",
                f"{domain}#about-team",
                f"{domain}#team-behind-your-team",
                f"{domain}#the-team-behind-your-team",
                f"{domain}#our-team",
                f"{domain}#meet-the-team",
                f"{domain}#staff",
                f"{domain}#people",
                f"{domain}/team",
                f"{domain}/our-team", 
                f"{domain}/about/team",
                f"{domain}/company/team"
            ]
            
            # Combine all URLs and remove duplicates
            all_urls = list(set(urls + potential_team_urls))
            
            prompt = f"""Analyze these URLs from {domain} and select the 3-4 BEST pages to find company staff/team information.

URLS TO ANALYZE:
{json.dumps(all_urls, indent=2)}

ENHANCED TEAM PAGE DETECTION RULES:
1. **HIGH PRIORITY - Team/Staff Pages:**
   - URLs with: team, staff, people, about-us, who, leadership, management
   - URL fragments like: #team, #the-team-behind-your-team, #our-team
   - Look for carousel/dynamic content indicators

2. **MEDIUM PRIORITY - Contact/About Pages:**
   - contact, about, company info (might have staff listings)

3. **LOW PRIORITY - Homepage:**
   - Main domain (as fallback)

4. **EXCLUDE:**
   - LinkedIn URLs (we handle separately)
   - External domains
   - File downloads (pdf, doc, etc.)
   - Irrelevant pages (products, services, blog)

5. **SPECIAL ATTENTION:**
   - Look for pages that might contain carousel content
   - Pages with fragments (#) that indicate team sections
   - Multiple team-related pages (include the best ones)

TARGET COMPANY: {domain_name}

Return ONLY a JSON array of 3-4 URLs most likely to contain staff information, ordered by priority:
["url1", "url2", "url3"]"""

            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,
                temperature=0.1
            )
            
            gpt_response = response.choices[0].message.content.strip()
            
            try:
                # Extract JSON from response
                if '```json' in gpt_response:
                    json_start = gpt_response.find('```json') + 7
                    json_end = gpt_response.find('```', json_start)
                    json_str = gpt_response[json_start:json_end].strip()
                elif '[' in gpt_response and ']' in gpt_response:
                    json_start = gpt_response.find('[')
                    json_end = gpt_response.rfind(']') + 1
                    json_str = gpt_response[json_start:json_end]
                else:
                    json_str = gpt_response
                
                selected_urls = json.loads(json_str)
                
                # Validate URLs
                valid_urls = []
                for url in selected_urls:
                    if isinstance(url, str) and url.strip():
                        # Ensure URL is properly formatted
                        if not url.startswith('http'):
                            if url.startswith('#'):
                                url = domain + url
                            elif url.startswith('/'):
                                url = domain + url
                            else:
                                url = domain + '/' + url
                        valid_urls.append(url)
                
                print(f"🎯 GPT-4o selected {len(valid_urls)} valid URLs:")
                for i, url in enumerate(valid_urls, 1):
                    print(f"   {i}. {url}")
                
                return valid_urls[:4]  # Limit to 4 URLs
                
            except json.JSONDecodeError:
                print(f"⚠️ GPT-4o response parsing failed: {gpt_response}")
                # Fallback to basic URL selection
                return self._fallback_url_selection(urls, domain)
                
        except Exception as e:
            print(f"❌ GPT-4o analysis failed: {e}")
            return self._fallback_url_selection(sitemap_data.get('urls', []), domain)

    def _fallback_url_selection(self, urls: list, domain: str) -> list:
        """🔄 Fallback URL selection when GPT-4o fails"""
        
        priority_keywords = [
            'team', 'staff', 'people', 'about', 'who', 'contact', 
            'leadership', 'management', 'our-team', 'meet-the-team'
        ]
        
        scored_urls = []
        for url in urls:
            score = 0
            url_lower = url.lower()
            
            for keyword in priority_keywords:
                if keyword in url_lower:
                    score += 10
            
            if url == domain or url == domain + '/':
                score += 1  # Homepage as fallback
                
            scored_urls.append((score, url))
        
        # Sort by score and take top 3
        scored_urls.sort(reverse=True, key=lambda x: x[0])
        selected = [url for score, url in scored_urls[:3]]
        
        # Add homepage if not included
        if domain not in selected and len(selected) < 3:
            selected.append(domain)
        
        return selected

    def _extract_domain_name(self, url: str) -> str:
        """🌐 Extract clean domain name for prompting"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path
            # Remove www. and get main domain name
            domain = domain.replace('www.', '')
            return domain.split('.')[0] if '.' in domain else domain
        except:
            return 'company'

    def _scrape_website_sitemap(self, url: str) -> dict:
        """🗺️ Scrape website sitemap using Apify"""
        
        try:
            client = get_working_apify_client_part1()
            
            actor_input = {
                "startUrls": [{"url": url}],
                "maxRequestsPerCrawl": 20,
                "includeUrlGlobs": [{"glob": "*"}],
                "ignoreInternalLinks": False,
                "maxInternalLinks": 50,
                "pseudoUrls": [{"purl": "[.*]"}],
                "breakpointLocation": "NONE"
            }
            
            run = client.actor("drobnikj/website-sitemap-crawler").call(run_input=actor_input)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                urls = [item.get('url') for item in items if item.get('url')]
                return {'urls': list(set(urls))}  # Remove duplicates
            else:
                return {'urls': [url]}  # Fallback to main URL
                
        except Exception as e:
            print(f"⚠️ Sitemap crawling failed: {e}")
            return {'urls': [url]}

    def _extract_staff_from_page(self, page_url: str, domain: str) -> tuple:
        """📄 Extract staff from a specific page"""
        
        try:
            client = get_working_apify_client_part1()
            
            actor_input = {
                "startUrls": [{"url": page_url}],
                "maxRequestsPerCrawl": 1,
                "includePageContent": True
            }
            
            run = client.actor("drobnikj/extract-page-content").call(run_input=actor_input)
            
            if not run:
                return [], None
            
            items = client.dataset(run["defaultDatasetId"]).list_items().items
            
            if not items:
                return [], None
            
            page_content = items[0]
            text_content = page_content.get('text', '')
            
            # Use GPT-4o to extract staff with enhanced prompting
            staff_data = self._extract_staff_with_gpt4o_enhanced(text_content, domain, page_url)
            linkedin_url = self._extract_linkedin_from_content(text_content)
            
            return staff_data, linkedin_url
            
        except Exception as e:
            print(f"❌ Page extraction failed for {page_url}: {e}")
            return [], None

    def _extract_staff_with_gpt4o_enhanced(self, content: str, domain: str, page_url: str) -> list:
        """🧠 Enhanced GPT-4o staff extraction with carousel awareness"""
        
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            domain_name = self._extract_domain_name(domain)
            
            prompt = f"""Extract staff from this {domain_name} content with ENHANCED DETECTION.

PAGE URL: {page_url}
CONTENT:
{content[:8000]}

ENHANCED EXTRACTION RULES:
1. Find ONLY current employees of {domain_name}
2. Look for ALL formats:
   - Traditional lists
   - **CAROUSEL/SLIDER content** (multiple team members in rotating display)
   - Team grids/cards
   - Leadership sections
   - Staff directories
   
3. Extract: Full name + Job title
4. EXCLUDE: 
   - Clients, testimonials, external people
   - Company names mistaken as people
   - Generic roles without names

5. **CAROUSEL DETECTION:**
   - Look for multiple people with similar formatting
   - Team members in sequences (often 3-7 people)
   - Role patterns: Manager, Director, Officer, etc.

6. REQUIRED FORMAT:
   - Name must have first + last name
   - Title must be descriptive job role

Return JSON array ONLY:
[
  {{"name": "Full Name", "title": "Job Title"}},
  {{"name": "Full Name", "title": "Job Title"}}
]

If no staff found, return: []"""

            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000,
                temperature=0.1
            )
            
            gpt_response = response.choices[0].message.content.strip()
            
            print(f"\n🧠 GPT-4o RAW RESPONSE:")
            print("--------------------------------------------------")
            print(gpt_response)
            print("--------------------------------------------------")
            
            try:
                # Extract JSON from response
                if '```json' in gpt_response:
                    json_start = gpt_response.find('```json') + 7
                    json_end = gpt_response.find('```', json_start)
                    json_str = gpt_response[json_start:json_end].strip()
                elif '[' in gpt_response and ']' in gpt_response:
                    json_start = gpt_response.find('[')
                    json_end = gpt_response.rfind(']') + 1
                    json_str = gpt_response[json_start:json_end]
                else:
                    return []
                
                staff_list = json.loads(json_str)
                
                # Validate staff data
                valid_staff = []
                for person in staff_list:
                    if (isinstance(person, dict) and 
                        person.get('name') and 
                        person.get('title') and
                        len(person['name'].split()) >= 2):  # Must have first + last name
                        
                        valid_staff.append(person)
                        print(f"✅ Valid staff: {person['name']} - {person['title']}")
                
                return valid_staff
                
            except json.JSONDecodeError:
                print(f"⚠️ JSON parsing failed: {gpt_response}")
                return []
                
        except Exception as e:
            print(f"❌ GPT-4o staff extraction failed: {e}")
            return []

    def _extract_linkedin_from_content(self, content: str) -> str:
        """🔗 Extract LinkedIn URL from page content"""
        
        try:
            # Look for LinkedIn company URLs
            linkedin_patterns = [
                r'https?://(?:www\.)?linkedin\.com/company/[^/\s]+',
                r'linkedin\.com/company/[^/\s]+',
                r'https?://(?:uk\.)?linkedin\.com/company/[^/\s]+',
            ]
            
            for pattern in linkedin_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                if matches:
                    url = matches[0]
                    if not url.startswith('http'):
                        url = 'https://' + url
                    return self._clean_linkedin_url(url)
            
            return None
            
        except Exception as e:
            print(f"⚠️ LinkedIn extraction error: {e}")
            return None

    def _extract_linkedin_from_sitemap(self, sitemap_data: dict, domain: str) -> str:
        """🔍 Extract LinkedIn URL from sitemap data"""
        
        try:
            domain_name = self._extract_domain_name(domain)
            print(f"🔍 Searching for LinkedIn URL for company: {domain_name}")
            
            urls = sitemap_data.get('urls', [])
            linkedin_candidates = []
            
            # Look for LinkedIn URLs in sitemap
            for url in urls:
                if 'linkedin.com/company' in url.lower():
                    linkedin_candidates.append(url)
            
            if linkedin_candidates:
                print(f"   🔍 Found {len(linkedin_candidates)} LinkedIn URL candidates:")
                for i, url in enumerate(linkedin_candidates, 1):
                    cleaned = self._clean_linkedin_url(url)
                    print(f"      {i}. {url}")
                    print(f"      🧽 Cleaned: {url} → {cleaned}")
                
                # Return the first valid one
                selected = self._clean_linkedin_url(linkedin_candidates[0])
                print(f"   ✅ Selected LinkedIn URL: {selected}")
                return selected
            
            return None
            
        except Exception as e:
            print(f"❌ LinkedIn URL extraction failed: {e}")
            return None

    def _clean_linkedin_url(self, url: str) -> str:
        """🧽 Clean and standardize LinkedIn URL"""
        
        try:
            # Remove tracking parameters and fragments
            if '?' in url:
                url = url.split('?')[0]
            if '#' in url:
                url = url.split('#')[0]
            
            # Ensure proper protocol and domain
            if url.startswith('linkedin.com'):
                url = 'https://www.' + url
            elif url.startswith('www.linkedin.com'):
                url = 'https://' + url
            elif 'linkedin.com' in url and not url.startswith('http'):
                url = 'https://www.' + url.split('linkedin.com')[1]
                url = 'https://www.linkedin.com' + url
            
            # Standardize to www.linkedin.com
            url = url.replace('uk.linkedin.com', 'www.linkedin.com')
            url = url.replace('//linkedin.com', '//www.linkedin.com')
            
            # Ensure /company/ format
            if '/company/' in url:
                return url.rstrip('/')
            
            return url
            
        except Exception as e:
            print(f"⚠️ URL cleaning error: {e}")
            return url