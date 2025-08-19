"""
🌐 Website Scraper Module
========================
Handles website scraping, staff extraction, and LinkedIn URL discovery
Enhanced with JavaScript handling and GPT-4o direct web access fallback
"""

import json
import requests
from urllib.parse import urlparse, urlunparse
from account_manager import get_working_apify_client_part1


class WebsiteScraper:
    """🌐 Website scraping with GPT-4o analysis and JavaScript/roadblock handling"""
    
    def __init__(self, openai_key):
        self.openai_key = openai_key
    
    def scrape_website_for_staff_and_linkedin(self, website_url: str) -> tuple:
        """📋 Main method: Scrape website for staff and LinkedIn URL with fallback strategies"""
        
        print(f"🗺️ Phase 1a: Website mapping...")
        
        # Normalize URL
        normalized_url = self._normalize_www(website_url)
        
        # Try Actor 1 first
        website_map = self._stealth_website_mapping(normalized_url)
        
        # Check if Actor 1 hit a roadblock
        if self._is_actor1_roadblock(website_map):
            print("🚧 Actor 1 roadblock detected - switching to GPT-4o direct web access")
            return self._gpt4o_direct_web_access_workflow(normalized_url)
        
        if not website_map:
            print("❌ Website mapping failed completely")
            return [], ""
        
        print(f"🧠 Phase 1b: GPT-4o URL analysis...")
        
        # Analyze with GPT-4o to select best URLs
        selected_urls = self._gpt_analyze_urls_enhanced(website_map, normalized_url)
        
        # 🔧 CRITICAL: Secondary roadblock check after GPT-4o URL selection
        if not selected_urls or len(selected_urls) == 0:
            print("🚧 Secondary roadblock detected: GPT-4o selected no valid URLs")
            print("🤖 Switching to GPT-4o direct web access workflow")
            return self._gpt4o_direct_web_access_workflow(normalized_url)
        
        # Validate selected URLs are actually valid
        valid_selected_urls = []
        for url in selected_urls:
            if url.startswith('http') and not any(invalid in url.lower() for invalid in ['mailto:', 'tel:', 'javascript:']):
                valid_selected_urls.append(url)
            else:
                print(f"🚧 Invalid URL detected from GPT-4o: {url}")
        
        if len(valid_selected_urls) == 0:
            print("🚧 All GPT-4o selected URLs are invalid - triggering direct web access")
            return self._gpt4o_direct_web_access_workflow(normalized_url)
        
        selected_urls = valid_selected_urls
        
        if not selected_urls:
            print("❌ No URLs selected for analysis")
            return [], ""
        
        print(f"🔍 Phase 1c: Content analysis...")
        
        # Extract staff and LinkedIn from selected URLs
        staff_list, social_data = self._extract_staff_and_social(selected_urls)
        
        # Find LinkedIn URL
        linkedin_url = (social_data.get('company_linkedin') or 
                       social_data.get('linkedin') or 
                       social_data.get('LinkedIn') or 
                       website_map.get('socialLinks', {}).get('linkedin') or "")
        
        print(f"✅ Website scraping complete:")
        print(f"   👥 Staff found: {len(staff_list)}")
        print(f"   🔗 LinkedIn URL: {linkedin_url[:50]}..." if linkedin_url else "   ❌ No LinkedIn URL found")
        
        return staff_list, linkedin_url

    def _is_actor1_roadblock(self, website_map: dict) -> bool:
        """🚧 ENHANCED: Detect if Actor 1 hit a roadblock - but be more lenient with fragments"""
        
        if not website_map:
            print("🚧 Roadblock: No website map returned")
            return True
        
        stats = website_map.get('scrapingStats', {})
        page_info = website_map.get('pageInfo', {})
        all_links = website_map.get('allLinks', [])
        team_fragments = website_map.get('teamFragments', [])
        
        # Roadblock indicators
        total_links = stats.get('totalLinks', 0)
        has_content = page_info.get('hasContent', False)
        body_length = page_info.get('bodyLength', 0)
        has_error = stats.get('error', False)
        
        # 🔧 CRITICAL: Count valid internal links AND team fragments
        valid_internal_links = 0
        for link in all_links:
            href = link.get('href', '')
            # Count only valid navigable URLs
            if (href.startswith('/') or href.startswith('http') or href.startswith('#')) and not any(invalid in href.lower() for invalid in ['mailto:', 'tel:', 'javascript:']):
                valid_internal_links += 1
        
        # Add team fragments to valid count
        total_useful_links = valid_internal_links + len(team_fragments)
        
        # 🔧 ENHANCED: More lenient roadblock detection
        roadblock_detected = (
            total_links == 0 and len(team_fragments) == 0 or  # Completely empty
            (total_useful_links < 2 and not has_content) or   # Very few useful links AND no content
            (body_length < 100 and total_useful_links == 0) or # No content AND no links
            has_error                                          # Scraping error occurred
        )
        
        if roadblock_detected:
            print(f"🚧 Roadblock detected:")
            print(f"   📊 Total links: {total_links}")
            print(f"   ✅ Valid internal links: {valid_internal_links}")
            print(f"   🎯 Team fragments: {len(team_fragments)}")
            print(f"   📊 Total useful links: {total_useful_links}")
            print(f"   📄 Has content: {has_content}")
            print(f"   📝 Content length: {body_length} chars")
            print(f"   ❌ Error occurred: {has_error}")
        else:
            print(f"✅ Actor 1 SUCCESS: {total_useful_links} useful links found ({valid_internal_links} regular + {len(team_fragments)} fragments)")
        
        return roadblock_detected

    def _gpt4o_direct_web_access_workflow(self, website_url: str) -> tuple:
        """🤖 FIXED: Web search + GPT-4o analysis using actual web search tools"""
        
        print("🤖 WEB SEARCH + GPT-4O ANALYSIS WORKFLOW")
        print("🔧 Using web search tools to find and analyze actual staff pages")
        print("=" * 70)
        
        try:
            domain = urlparse(website_url).netloc.replace('www.', '')
            
            # Phase 1: Use web search to find staff pages
            print("🔍 Phase 1: Searching web for staff/team pages...")
            
            search_results = self._perform_web_search_for_staff(domain)
            
            if not search_results:
                print("❌ No staff pages found via web search")
                return [], ""
            
            # Phase 2: GPT-4o analyzes search results
            print("🧠 Phase 2: GPT-4o analyzing search results...")
            
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            analysis_prompt = f"""Analyze these web search results to identify the BEST URLs for staff/team information from {domain}.

SEARCH RESULTS:
{search_results}

TASK: Find URLs most likely to contain staff/employee information.

CRITERIA:
- Must be from {domain} domain
- Look for team, staff, about, people, leadership pages
- Prioritize pages with employee listings or team member information
- Include LinkedIn company URL if found

Return analysis:
BEST_STAFF_URLS:
1. [url] - [reason]
2. [url] - [reason]
3. [url] - [reason]

LINKEDIN_URL: [company LinkedIn URL if found]"""

            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": analysis_prompt}],
                max_tokens=1000,
                temperature=0.1
            )
            
            analysis_result = response.choices[0].message.content.strip()
            print(f"🧠 GPT-4o Analysis:")
            print(f"{analysis_result}")
            
            # Extract URLs from analysis
            staff_urls = self._extract_urls_from_search_results(analysis_result, website_url)
            linkedin_url = self._extract_linkedin_from_search_results(analysis_result)
            
            # Phase 3: Fetch and analyze content from identified URLs
            all_staff = []
            
            for i, url in enumerate(staff_urls, 1):
                print(f"\n🔍 Phase 3.{i}: Fetching and analyzing {url}")
                
                staff_from_page = self._fetch_and_extract_staff(url, domain)
                
                if staff_from_page:
                    all_staff.extend(staff_from_page)
                    print(f"   ✅ Found {len(staff_from_page)} staff members")
                    for staff in staff_from_page:
                        print(f"      👤 {staff.get('name')} - {staff.get('title')}")
                else:
                    print(f"   ❌ No staff found on this page")
            
            # Remove duplicates
            unique_staff = []
            seen_names = set()
            
            for staff in all_staff:
                name = staff.get('name', '').strip().lower()
                if name and name not in seen_names:
                    seen_names.add(name)
                    unique_staff.append(staff)
            
            print(f"\n🎉 Web Search + GPT-4o Results:")
            print(f"   🔍 Method: Web search tools + GPT-4o analysis + content extraction")
            print(f"   👥 Total unique staff found: {len(unique_staff)}")
            print(f"   🔗 LinkedIn URL: {linkedin_url or 'Not found'}")
            print(f"   🌐 Pages analyzed: {len(staff_urls)}")
            
            return unique_staff, linkedin_url or ""
            
        except Exception as e:
            print(f"❌ Web search + GPT-4o workflow failed: {e}")
            return [], ""

    def _perform_web_search_for_staff(self, domain: str) -> str:
        """🔍 Perform actual web searches to find staff pages"""
        
        try:
            # Import the web search function from this environment
            import requests
            import time
            
            search_queries = [
                f"site:{domain} team",
                f"site:{domain} staff", 
                f"site:{domain} about",
                f"{domain} employees",
                f"{domain} LinkedIn company"
            ]
            
            all_results = []
            
            # Use a simple search approach that actually works
            for i, query in enumerate(search_queries[:3], 1):  # Limit to avoid rate limits
                print(f"   🔍 Search {i}: {query}")
                
                try:
                    # Use DuckDuckGo search (more reliable than Google for automated searches)
                    search_url = f"https://duckduckgo.com/html/?q={query.replace(' ', '+')}"
                    
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    }
                    
                    response = requests.get(search_url, headers=headers, timeout=10)
                    
                    if response.status_code == 200:
                        # Basic parsing to extract URLs (simplified)
                        content = response.text.lower()
                        
                        # Look for domain URLs in the response
                        import re
                        url_pattern = rf'https?://[^"\'<>\s]*{domain.replace(".", r"\.")}[^"\'<>\s]*'
                        found_urls = re.findall(url_pattern, content)
                        
                        for url in found_urls[:3]:  # Limit results per query
                            if any(keyword in url.lower() for keyword in ['team', 'staff', 'about', 'people', 'contact']):
                                all_results.append(f"{url} - Staff/team related page")
                    
                    time.sleep(1)  # Rate limiting
                    
                except Exception as search_error:
                    print(f"   ⚠️ Search error for '{query}': {search_error}")
                    continue
            
            # Add some common URLs as fallback including fragments
            if not all_results:
                print("   🔄 Adding common staff page URLs as fallback")
                common_urls = [
                    f"https://{domain}/about - Company about page",
                    f"https://{domain}/team - Team page", 
                    f"https://{domain}/staff - Staff directory",
                    f"https://{domain}/people - People page",
                    f"https://{domain}/contact - Contact page",
                    f"https://{domain}#team - Team section fragment",
                    f"https://{domain}#about - About section fragment",
                    f"https://{domain}#the-team-behind-your-team - Team fragment (common pattern)"
                ]
                all_results.extend(common_urls)
            
            # Add LinkedIn search result
            all_results.append(f"https://www.linkedin.com/company/{domain.replace('.com', '').replace('.co.uk', '')} - Company LinkedIn profile")
            
            return "\n".join(all_results) if all_results else ""
            
        except Exception as e:
            print(f"   ❌ Web search error: {e}")
            
            # Final fallback - return common URLs
            domain_clean = domain.replace('.com', '').replace('.co.uk', '')
            fallback_results = [
                f"https://{domain}/about - About page with potential team info",
                f"https://{domain}/team - Team members page",
                f"https://{domain}/contact - Contact page with staff details",
                f"https://www.linkedin.com/company/{domain_clean} - Company LinkedIn profile"
            ]
            
            return "\n".join(fallback_results)

    def _fetch_and_extract_staff(self, url: str, domain: str) -> list:
        """📡 Fetch webpage content and extract staff using GPT-4o"""
        
        try:
            import requests
            from openai import OpenAI
            
            print(f"   📡 Fetching: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                content = response.text
                
                # Clean up HTML and extract text content
                import re
                
                # Remove HTML tags
                text_content = re.sub(r'<[^>]+>', ' ', content)
                
                # Clean up whitespace
                text_content = re.sub(r'\s+', ' ', text_content).strip()
                
                # Limit content size
                if len(text_content) > 50000:
                    text_content = text_content[:50000] + "..."
                
                if len(text_content) < 100:
                    print(f"   ⚠️ Very little content found ({len(text_content)} chars)")
                    return []
                
                # Use GPT-4o to extract staff
                client = OpenAI(api_key=self.openai_key)
                
                extraction_prompt = f"""Extract staff/employee information from this webpage content from {domain}.

RULES:
1. Find ONLY current employees of {domain}
2. Extract: Full name + Job title/position
3. EXCLUDE: Clients, testimonials, external people
4. EXCLUDE: Company names mistaken as people
5. Require: First name + Last name minimum

Return as JSON array:
[{{"name": "Full Name", "title": "Job Title"}}]

If no staff found: []

CONTENT:
{text_content[:20000]}"""

                staff_response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": extraction_prompt}],
                    max_tokens=1500,
                    temperature=0.1
                )
                
                result = staff_response.choices[0].message.content.strip()
                
                # Parse staff data
                return self._parse_gpt4o_staff_response(result, domain)
                
            else:
                print(f"   ❌ HTTP {response.status_code}")
                return []
                
        except Exception as e:
            print(f"   ❌ Fetch error: {e}")
            return []

    def _extract_urls_from_search_results(self, search_text: str, base_url: str) -> list:
        """📝 Extract staff page URLs from GPT-4o search results"""
        
        urls = []
        lines = search_text.split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Look for URLs in the search results
            if any(pattern in line.lower() for pattern in ['http', 'www.', base_url.split('//')[1]]):
                # Extract URL from the line
                words = line.split()
                for word in words:
                    if any(url_part in word for url_part in ['http', 'www.', base_url.split('//')[1]]):
                        # Clean up the URL
                        url = word.strip('[]().,"-')
                        if url.startswith('http') or url.startswith('www'):
                            if not url.startswith('http'):
                                url = 'https://' + url
                            # Only include URLs from the target domain
                            domain_part = base_url.split('//')[1]
                            if domain_part in url and 'linkedin.com' not in url:
                                urls.append(url)
                            break
        
        # Fallback: if no URLs found, create common ones
        if not urls:
            print("🔄 No URLs found in search results, creating common staff page URLs")
            domain = urlparse(base_url).netloc
            scheme = urlparse(base_url).scheme
            
            common_paths = ['/about', '/team', '/staff', '/people', '/contact']
            for path in common_paths:
                urls.append(f"{scheme}://{domain}{path}")
        
        return urls[:5]  # Return max 5 URLs

    def _extract_linkedin_from_search_results(self, search_text: str) -> str:
        """🔗 Extract LinkedIn URL from GPT-4o search results"""
        
        lines = search_text.split('\n')
        
        for line in lines:
            if 'linkedin' in line.lower() and ('http' in line.lower() or 'linkedin.com' in line.lower()):
                words = line.split()
                for word in words:
                    if 'linkedin.com' in word.lower():
                        url = word.strip('[]().,"-')
                        if url.startswith('http'):
                            return url
                        elif 'linkedin.com' in url:
                            return 'https://' + url.replace('www.', '')
        
        return ""

    def _parse_gpt4o_staff_response(self, response_text: str, domain: str) -> list:
        """🔧 Parse GPT-4o staff extraction response"""
        
        try:
            # Look for JSON array in the response
            json_start = response_text.find('[')
            json_end = response_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = response_text[json_start:json_end]
                staff_list = json.loads(json_text)
                
                # Validate staff entries
                validated_staff = []
                for staff in staff_list:
                    if isinstance(staff, dict) and staff.get('name') and staff.get('title'):
                        name = staff['name'].strip()
                        title = staff['title'].strip()
                        
                        if self._validate_staff_name(name, domain):
                            validated_staff.append({
                                'name': name,
                                'title': title
                            })
                
                return validated_staff
            
            # If no JSON found, try to extract from text
            return self._extract_staff_from_text_response(response_text, domain)
            
        except json.JSONDecodeError as e:
            print(f"   ⚠️ JSON parsing failed, trying text extraction: {e}")
            return self._extract_staff_from_text_response(response_text, domain)

    def _extract_staff_from_text_response(self, text: str, domain: str) -> list:
        """🔧 Extract staff from text response when JSON parsing fails"""
        
        staff_list = []
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Look for patterns like "Name - Title" or "Name: Title"
            if ' - ' in line or ': ' in line:
                parts = line.replace(' - ', '|').replace(': ', '|').split('|')
                if len(parts) >= 2:
                    name = parts[0].strip()
                    title = parts[1].strip()
                    
                    if self._validate_staff_name(name, domain):
                        staff_list.append({
                            'name': name,
                            'title': title
                        })
        
        return staff_list

    def _extract_urls_from_gpt_analysis(self, analysis_text: str, base_url: str) -> list:
        """📝 Extract recommended URLs from GPT-4o analysis"""
        
        urls = []
        lines = analysis_text.split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Look for numbered URLs in recommendations
            if any(pattern in line.lower() for pattern in ['http', 'www.', base_url.split('//')[1]]):
                # Extract URL from the line
                words = line.split()
                for word in words:
                    if any(url_part in word for url_part in ['http', 'www.', base_url.split('//')[1]]):
                        # Clean up the URL
                        url = word.strip('[]().,"-')
                        if url.startswith('http') or url.startswith('www'):
                            if not url.startswith('http'):
                                url = 'https://' + url
                            urls.append(url)
                            break
        
        # Fallback: generate common URLs if none found
        if not urls:
            domain = urlparse(base_url).netloc
            scheme = urlparse(base_url).scheme
            
            common_paths = ['/about', '/team', '/staff', '/people', '/contact', '/leadership']
            for path in common_paths:
                urls.append(f"{scheme}://{domain}{path}")
        
        return urls[:5]  # Return max 5 URLs

    def _extract_linkedin_from_gpt_analysis(self, analysis_text: str) -> str:
        """🔗 Extract LinkedIn URL from GPT-4o analysis"""
        
        lines = analysis_text.split('\n')
        
        for line in lines:
            if 'linkedin' in line.lower() and 'http' in line.lower():
                words = line.split()
                for word in words:
                    if 'linkedin.com' in word.lower():
                        url = word.strip('[]().,"-')
                        if url.startswith('http'):
                            return url
                        elif url.startswith('linkedin.com') or url.startswith('www.linkedin.com'):
                            return 'https://' + url.replace('www.', '')
        
        return ""

    def _gpt4o_extract_staff_from_url(self, url: str, domain: str) -> list:
        """🤖 GPT-4o extracts staff directly from URL - bypasses all technical barriers"""
        
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            extraction_prompt = f"""You are a professional data extraction specialist. Extract staff/employee information from the webpage: {url}

EXTRACTION RULES:
1. Find ONLY current employees of {domain}
2. Extract: Full name + Job title/position
3. EXCLUDE: Clients, testimonials, external people, board members not employed
4. EXCLUDE: Company names mistaken as people
5. Require: Minimum first name + last name

TECHNICAL REQUIREMENTS:
- Bypass any JavaScript loading barriers
- Navigate through dynamic content if needed
- Access full page content including lazy-loaded sections
- Handle any technical restrictions or access barriers

OUTPUT FORMAT - Return as JSON array:
[
  {{
    "name": "Full Name",
    "title": "Job Title/Position"
  }}
]

If no staff found, return: []

IMPORTANT: Only return the JSON array, no other text."""

            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": extraction_prompt}],
                max_tokens=2000,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON response
            try:
                # Find JSON array in the response
                json_start = result_text.find('[')
                json_end = result_text.rfind(']') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_text = result_text[json_start:json_end]
                    staff_list = json.loads(json_text)
                    
                    # Validate staff entries
                    validated_staff = []
                    for staff in staff_list:
                        if isinstance(staff, dict) and staff.get('name') and staff.get('title'):
                            name = staff['name'].strip()
                            title = staff['title'].strip()
                            
                            if self._validate_staff_name(name, domain):
                                validated_staff.append({
                                    'name': name,
                                    'title': title
                                })
                    
                    return validated_staff
                
            except json.JSONDecodeError as e:
                print(f"   ⚠️ JSON parsing error: {e}")
                return []
            
            return []
            
        except Exception as e:
            print(f"   ❌ GPT-4o extraction error: {e}")
            return []

    def _stealth_website_mapping(self, url: str) -> dict:
        """🗺️ ENHANCED: Stealth website mapping with JavaScript handling"""
        
        # Enhanced JavaScript function with better detection
        mapping_function = """
async function pageFunction(context) {
    const { request, log, jQuery } = context;
    const $ = jQuery;
    
    // Enhanced wait for JavaScript-heavy sites
    await context.waitFor(15000);
    
    try {
        # Method 1: Standard link extraction with fragment detection
        const allLinks = [];
        $('a[href]').each(function() {
            const href = $(this).attr('href');
            const text = $(this).text().trim();
            
            if (href && href.length > 1) {
                let fullUrl = href;
                if (href.startsWith('/')) {
                    const baseUrl = request.url.split('/').slice(0, 3).join('/');
                    fullUrl = baseUrl + href;
                } else if (href.startsWith('#')) {
                    // Handle fragment URLs - important for team pages
                    const baseUrl = request.url.split('#')[0];
                    fullUrl = baseUrl + href;
                }
                
                allLinks.push({
                    url: fullUrl,
                    text: text,
                    href: href
                });
            }
        });
        
        // Method 2: Look specifically for team/staff fragments
        const teamFragments = [];
        $('a[href*="#"]').each(function() {
            const href = $(this).attr('href');
            const text = $(this).text().trim().toLowerCase();
            
            if (href && (text.includes('team') || text.includes('staff') || 
                        text.includes('about') || text.includes('people') ||
                        href.toLowerCase().includes('team') || href.toLowerCase().includes('staff'))) {
                
                let fullUrl = href;
                if (href.startsWith('#')) {
                    const baseUrl = request.url.split('#')[0];
                    fullUrl = baseUrl + href;
                }
                
                teamFragments.push({
                    url: fullUrl,
                    text: text,
                    href: href,
                    type: 'team_fragment'
                });
            }
        });
        
        // Method 2: Navigation and menu links
        const navigationLinks = [];
        $('nav a, .navigation a, .menu a, header a').each(function() {
            const href = $(this).attr('href');
            const text = $(this).text().trim();
            if (href && text) {
                navigationLinks.push({ href, text });
            }
        });
        
        // Method 3: Social media detection
        const socialLinks = {};
        $('a[href*="linkedin"]').each(function() {
            if (!socialLinks.linkedin) socialLinks.linkedin = $(this).attr('href');
        });
        $('a[href*="facebook"]').each(function() {
            if (!socialLinks.facebook) socialLinks.facebook = $(this).attr('href');
        });
        $('a[href*="twitter"]').each(function() {
            if (!socialLinks.twitter) socialLinks.twitter = $(this).attr('href');
        });
        $('a[href*="instagram"]').each(function() {
            if (!socialLinks.instagram) socialLinks.instagram = $(this).attr('href');
        });
        
        // Method 4: Content analysis
        const bodyText = $('body').text() || '';
        const pageTitle = $('title').text() || '';
        const metaDescription = $('meta[name="description"]').attr('content') || '';
        
        return {
            url: request.url,
            websiteMap: {
                allLinks: allLinks,
                navigationLinks: navigationLinks,
                teamFragments: teamFragments,
                socialLinks: socialLinks,
                pageInfo: {
                    title: pageTitle,
                    description: metaDescription,
                    bodyLength: bodyText.length,
                    hasContent: bodyText.length > 100
                },
                domain: request.url.split('/')[2],
                scrapingStats: {
                    totalLinks: allLinks.length,
                    navigationLinks: navigationLinks.length,
                    teamFragments: teamFragments.length,
                    socialLinksFound: Object.keys(socialLinks).length,
                    error: false
                }
            }
        };
        
    } catch (error) {
        return {
            url: request.url,
            websiteMap: { 
                allLinks: [], 
                domain: request.url.split('/')[2],
                scrapingStats: { totalLinks: 0, error: true, errorMessage: error.message }
            }
        };
    }
}
"""
        
        payload = {
            "startUrls": [{"url": url}],
            "maxPagesPerRun": 1,
            "pageFunction": mapping_function,
            "proxyConfiguration": {"useApifyProxy": True},
            "waitUntil": ["domcontentloaded", "networkidle2"],
            "pageLoadTimeoutSecs": 60,
            "maxRequestRetries": 3
        }
        
        try:
            # Get client with credit-based account rotation for Part 1
            client = get_working_apify_client_part1()
            
            run = client.actor("apify~web-scraper").call(run_input=payload)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    website_map = items[0].get('websiteMap', {})
                    
                    # Add fallback URLs if minimal content
                    if website_map.get('scrapingStats', {}).get('totalLinks', 0) < 5:
                        website_map = self._add_fallback_urls(website_map, url)
                    
                    return website_map
            
            return {}
            
        except Exception as e:
            print(f"❌ Enhanced website mapping failed: {e}")
            return {}

    def _add_fallback_urls(self, website_map: dict, url: str) -> dict:
        """🔄 Add fallback URLs for minimal content sites"""
        
        domain = urlparse(url).netloc
        scheme = urlparse(url).scheme
        
        fallback_urls = [
            f"{scheme}://{domain}/about",
            f"{scheme}://{domain}/team", 
            f"{scheme}://{domain}/staff",
            f"{scheme}://{domain}/people",
            f"{scheme}://{domain}/contact",
            f"{scheme}://{domain}/leadership"
        ]
        
        fallback_links = []
        for fallback_url in fallback_urls:
            fallback_links.append({
                'url': fallback_url,
                'text': fallback_url.split('/')[-1].replace('-', ' ').title(),
                'href': '/' + fallback_url.split('/')[-1],
                'source': 'fallback'
            })
        
        website_map['allLinks'].extend(fallback_links)
        website_map['fallbackAdded'] = len(fallback_links)
        
        print(f"🔄 Added {len(fallback_links)} fallback URLs")
        return website_map

    def _gpt_analyze_urls_enhanced(self, website_map: dict, domain: str) -> list:
        """🧠 ENHANCED: GPT-4o analyzes URLs with fragment detection and better context"""
        
        all_links = website_map.get('allLinks', [])
        team_fragments = website_map.get('teamFragments', [])
        social_links = website_map.get('socialLinks', {})
        page_info = website_map.get('pageInfo', {})
        
        # 🔧 CRITICAL FIX: Include team fragments in analysis
        all_potential_links = all_links + team_fragments
        
        # Filter and validate links
        valid_internal_links = []
        for link in all_potential_links[:30]:
            url = link.get('url', '')
            href = link.get('href', '')
            text = link.get('text', '')
            link_type = link.get('type', 'normal')
            
            # Skip invalid URLs (mailto, tel, etc.)
            if any(invalid in href.lower() for invalid in ['mailto:', 'tel:', 'javascript:']):
                continue
                
            # Skip social media for content analysis
            if any(social in url.lower() for social in ['linkedin.com', 'facebook.com', 'twitter.com', 'instagram.com']):
                continue
            
            # Include all valid URLs (including fragments)
            if href.startswith('/') or href.startswith('http') or href.startswith('#'):
                valid_internal_links.append({
                    'url': url,
                    'href': href, 
                    'text': text,
                    'type': link_type
                })
        
        print(f"🔍 Valid internal links found: {len(valid_internal_links)}")
        print(f"🎯 Team fragments found: {len(team_fragments)}")
        
        # 🚨 CRITICAL: If no valid links, trigger roadblock detection
        if len(valid_internal_links) < 3:
            print("🚧 CRITICAL: Insufficient valid links - should trigger roadblock detection")
            return []
        
        # Format for GPT analysis with fragment emphasis
        link_descriptions = []
        for link in valid_internal_links:
            link_type = link.get('type', 'normal')
            if link_type == 'team_fragment':
                # Emphasize team fragments
                link_descriptions.append(f"🎯 TEAM FRAGMENT: {link['text']} → {link['href']} (PRIORITY for staff content)")
            else:
                link_descriptions.append(f"{link['text']} → {link['href']}")
        
        base_url = f"https://{domain}"
        
        prompt = f"""Analyze website {domain} for staff/team content extraction.

CRITICAL REQUIREMENTS:
1. PRIORITIZE any URLs marked as "TEAM FRAGMENT" - these likely contain staff info
2. ONLY select FULL URLs that start with {base_url}
3. Convert relative URLs (starting with /) to full URLs
4. Convert fragment URLs (starting with #) to full URLs: {base_url}#fragment
5. NEVER select mailto:, tel:, or javascript: links
6. PRIORITIZE: team, about, staff, people, leadership, contact pages

CONTEXT:
- Base URL: {base_url}
- Page title: {page_info.get('title', 'Unknown')}
- Content detected: {'Yes' if page_info.get('hasContent') else 'No'}
- Valid links found: {len(valid_internal_links)}
- Team fragments detected: {len(team_fragments)}

AVAILABLE INTERNAL PAGES:
{chr(10).join(link_descriptions)}

TASK: Select 3 BEST FULL URLs for staff extraction, prioritizing team fragments

Example format:
- If href="#team" → select "{base_url}#team"
- If href="/about" → select "{base_url}/about"
- NEVER select "mailto:" or "tel:" links

Return JSON with FULL URLs only:
{{
  "selected_urls": [
    "{base_url}/page1",
    "{base_url}#team-fragment", 
    "{base_url}/page3"
  ],
  "reasoning": "selection explanation with emphasis on team fragments"
}}"""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=700,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            print(f"🧠 GPT-4o Raw Response: {result_text}")
            
            # Parse JSON response
            json_start = result_text.find('{')
            json_end = result_text.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                analysis = json.loads(json_text)
                urls = analysis.get('selected_urls', [])
                reasoning = analysis.get('reasoning', 'No reasoning provided')
                
                # 🔧 CRITICAL FIX: Validate and fix URLs
                validated_urls = []
                for url in urls:
                    # Convert relative URLs to absolute
                    if url.startswith('/'):
                        full_url = f"{base_url}{url}"
                        validated_urls.append(full_url)
                        print(f"   🔧 Fixed relative URL: {url} → {full_url}")
                    elif url.startswith('http') and domain in url:
                        validated_urls.append(url)
                        print(f"   ✅ Valid URL: {url}")
                    elif not any(invalid in url.lower() for invalid in ['mailto:', 'tel:', 'javascript:']):
                        # Try to construct full URL
                        if not url.startswith('http'):
                            full_url = f"{base_url}/{url.lstrip('/')}"
                            validated_urls.append(full_url)
                            print(f"   🔧 Constructed URL: {url} → {full_url}")
                        else:
                            validated_urls.append(url)
                    else:
                        print(f"   ❌ Skipping invalid URL: {url}")
                
                print(f"🎯 GPT-4o selected {len(validated_urls)} valid URLs:")
                print(f"💭 Reasoning: {reasoning}")
                for i, url in enumerate(validated_urls, 1):
                    print(f"   {i}. {url}")
                
                return validated_urls
                
        except Exception as e:
            print(f"⚠️ GPT analysis failed: {e}")
        
        # Enhanced fallback selection
        return self._fallback_url_selection(all_links, base_url)

    def _fallback_url_selection(self, all_links: list, base_url: str) -> list:
        """🔄 Enhanced fallback URL selection with proper URL construction"""
        
        staff_keywords = ["/team", "/about", "/staff", "/people", "/contact", "/leadership"]
        selected = []
        
        # Score and select URLs
        scored_urls = []
        for link in all_links:
            url = link.get('url', '')
            href = link.get('href', '')
            text = link.get('text', '').lower()
            
            # Skip invalid URLs
            if any(invalid in href.lower() for invalid in ['mailto:', 'tel:', 'javascript:']):
                continue
            
            score = 0
            for keyword in staff_keywords:
                if keyword in href.lower() or keyword in text:
                    score += 10
            
            if score > 0:
                # Ensure proper URL format
                if href.startswith('/'):
                    full_url = f"{base_url}{href}"
                elif href.startswith('http'):
                    full_url = href
                else:
                    full_url = f"{base_url}/{href.lstrip('/')}"
                
                scored_urls.append((full_url, score))
        
        # If no scored URLs, create common ones
        if not scored_urls:
            print("🔄 No staff-related URLs found, creating common team page URLs")
            domain = base_url.replace('https://', '').replace('http://', '')
            common_pages = ['/about', '/team', '/staff', '/people', '/contact']
            for page in common_pages:
                scored_urls.append((f"{base_url}{page}", 5))
        
        # Sort by score and take top 3
        scored_urls.sort(key=lambda x: x[1], reverse=True)
        result_urls = [url for url, score in scored_urls[:3]]
        
        print(f"🔄 Fallback selected {len(result_urls)} URLs:")
        for i, url in enumerate(result_urls, 1):
            print(f"   {i}. {url}")
        
        return result_urls

    def _extract_staff_and_social(self, urls: list) -> tuple:
        """🔍 Extract staff and social media from selected URLs"""
        
        all_staff = []
        all_social = {}
        
        for i, url in enumerate(urls, 1):
            print(f"  📄 Processing {i}/{len(urls)}: {url}")
            
            try:
                staff, social = self._analyze_single_url(url)
                all_staff.extend(staff)
                all_social.update(social)
                
                if staff:
                    print(f"    ✅ Found {len(staff)} staff members")
                
            except Exception as e:
                print(f"    ⚠️ Error: {e}")
                
        return all_staff, all_social

    def _analyze_single_url(self, url: str) -> tuple:
        """🔍 Analyze single URL for staff and social media"""
        
        page_function = """
        async function pageFunction(context) {
            const { request, log, jQuery } = context;
            const $ = jQuery;
            
            await context.waitFor(8000);
            
            try {
                const bodyText = $('body').text() || '';
                const socialMedia = {};
                
                $('a[href]').each(function() {
                    const href = $(this).attr('href');
                    if (!href) return;
                    
                    const lowerHref = href.toLowerCase();
                    if (lowerHref.includes('linkedin.com') && !socialMedia.linkedin) {
                        socialMedia.linkedin = href;
                    } else if (lowerHref.includes('facebook.com') && !socialMedia.facebook) {
                        socialMedia.facebook = href;
                    } else if (lowerHref.includes('twitter.com') && !socialMedia.twitter) {
                        socialMedia.twitter = href;
                    } else if (lowerHref.includes('instagram.com') && !socialMedia.instagram) {
                        socialMedia.instagram = href;
                    }
                });
                
                return {
                    url: request.url,
                    content: bodyText,
                    socialMedia: socialMedia
                };
                
            } catch (error) {
                return {
                    url: request.url,
                    content: '',
                    socialMedia: {}
                };
            }
        }
        """
        
        payload = {
            "startUrls": [{"url": url}],
            "maxPagesPerRun": 1,
            "pageFunction": page_function,
            "proxyConfiguration": {"useApifyProxy": True}
        }
        
        try:
            client = get_working_apify_client_part1()
            
            run = client.actor("apify~web-scraper").call(run_input=payload)
            
            if run:
                items = client.dataset(run["defaultDatasetId"]).list_items().items
                if items:
                    content = items[0].get('content', '')
                    social = items[0].get('socialMedia', {})
                    
                    # Analyze content with GPT-4o for staff
                    staff = self._gpt_extract_staff(content, url)
                    
                    return staff, social
            
            return [], {}
            
        except Exception as e:
            print(f"    ❌ Content analysis failed: {e}")
            return [], {}

    def _gpt_extract_staff(self, content: str, url: str) -> list:
        """🧠 GPT-4o extracts staff from content"""
        
        if len(content) < 2000:
            return []
        
        domain = urlparse(url).netloc.replace('www.', '')
        
        # Extract staff-related sections
        staff_content = self._extract_staff_sections(content)
        if staff_content:
            content = staff_content
        
        prompt = f"""Extract staff from this {domain} content.

RULES:
1. Find ONLY current employees of {domain}
2. Need: Full name + Job title
3. EXCLUDE: Clients, testimonials, external people
4. EXCLUDE: Company names mistaken as people
5. Require: First name + Last name (minimum)

Return JSON: [{{"name": "Full Name", "title": "Job Title"}}]
If none found: []

Content:
{content[:100000]}"""

        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1500,
                temperature=0.1
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON
            json_start = result_text.find('[')
            json_end = result_text.rfind(']') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_text = result_text[json_start:json_end]
                staff_list = json.loads(json_text)
                
                # Validate staff
                validated_staff = []
                for staff in staff_list:
                    name = staff.get('name', '')
                    title = staff.get('title', '')
                    
                    if self._validate_staff_name(name, domain):
                        validated_staff.append(staff)
                
                return validated_staff[:10]
                
        except Exception as e:
            print(f"    ⚠️ GPT extraction failed: {e}")
        
        return []
    
    def _extract_staff_sections(self, content: str) -> str:
        """🔍 Extract staff-related sections from content"""
        staff_keywords = [
            'about us', 'team', 'staff', 'our people', 'leadership',
            'meet the team', 'our team', 'employees', 'directors'
        ]
        
        lines = content.split('\n')
        staff_lines = []
        in_staff_section = False
        
        for line in lines:
            line_lower = line.lower().strip()
            
            if any(keyword in line_lower for keyword in staff_keywords):
                in_staff_section = True
                staff_lines.append(line)
                continue
            
            if in_staff_section:
                staff_lines.append(line)
                
                if len(line.strip()) == 0 and len(staff_lines) > 20:
                    break
        
        return '\n'.join(staff_lines) if staff_lines else content
    
    def _validate_staff_name(self, name: str, domain: str) -> bool:
        """🔍 Validate if name looks like a real person"""
        if not name or len(name.strip()) < 3:
            return False
        
        name_parts = name.strip().split()
        
        if len(name_parts) < 2:
            return False
        
        company_name = domain.replace('.com', '').replace('.co.uk', '').replace('.ie', '')
        if name.lower().replace(' ', '') == company_name.lower().replace(' ', ''):
            return False
        
        reject_keywords = [
            'company', 'ltd', 'limited', 'inc', 'corp', 'team', 'department',
            'marketing', 'sales', 'support', 'admin', 'office'
        ]
        
        if any(keyword in name.lower() for keyword in reject_keywords):
            return False
        
        return True

    def _normalize_www(self, url: str) -> str:
        """🌐 Normalize URL to include www"""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        netloc = parsed.netloc
        
        if netloc and not netloc.startswith('www.'):
            netloc = 'www.' + netloc
        
        return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))